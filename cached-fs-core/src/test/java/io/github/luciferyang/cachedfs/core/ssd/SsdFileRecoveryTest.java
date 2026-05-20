/*
 * Copyright (c) 2026 The cached-fs Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.luciferyang.cachedfs.core.ssd;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.luciferyang.cachedfs.core.RawFileCacheKey;
import io.github.luciferyang.cachedfs.core.id.StringIdMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SsdFileRecoveryTest {

  private static SsdFile.Config cfg(Path dir, long ckptIntervalBytes, boolean checksum) {
    return new SsdFile.Config(
        dir.resolve("data"),
        dir.resolve("cpt"),
        dir.resolve("log"),
        dir.resolve("cpt.tmp"),
        /* maxRegions= */ 2,
        /* maxEntries= */ 0,
        /* checkpointIntervalBytes= */ ckptIntervalBytes,
        /* checksumEnabled= */ checksum,
        /* checksumReadVerificationEnabled= */ checksum);
  }

  private static ByteBuffer payload(int size, byte fill) {
    byte[] arr = new byte[size];
    Arrays.fill(arr, fill);
    return ByteBuffer.wrap(arr);
  }

  @Test
  @DisplayName("removeFileEntries: pinned entry is retained and reported")
  void removeFileEntriesPinnedRetained(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://pinned");
    try (SsdFile f = new SsdFile(cfg(dir, 0L, false), ids)) {
      f.open();
      f.write(new RawFileCacheKey(fn, 0L), payload(4096, (byte) 1));
      try (SsdPin pin = f.find(fn, 0L)) {
        assertThat(pin.isEmpty()).isFalse();

        Set<Long> retained = f.removeFileEntries(Set.of(fn));
        assertThat(retained).containsExactly(fn);
        // Entry must still be findable while pinned.
        assertThat(f.find(fn, 0L).isEmpty()).isFalse();
      }
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("removeFileEntries: opportunistic compaction clears regions over 50% erased")
  void opportunisticCompaction(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fnVictim = ids.makeId("file://victim");
    long fnSurvivor = ids.makeId("file://survivor");
    try (SsdFile f = new SsdFile(cfg(dir, 0L, false), ids)) {
      f.open();
      // Two entries in region 0; victim is larger so removing it crosses the 50% threshold and
      // triggers compaction (which drops the survivor entry too).
      f.write(new RawFileCacheKey(fnVictim, 0L), payload(8 * 1024 * 1024, (byte) 0xAA));
      f.write(new RawFileCacheKey(fnSurvivor, 0L), payload(1024, (byte) 0xBB));
      assertThat(f.size()).isEqualTo(2);

      Set<Long> retained = f.removeFileEntries(Set.of(fnVictim));
      assertThat(retained).isEmpty();
      // After compaction, the region is fully cleared so survivor's entry is also gone.
      assertThat(f.find(fnSurvivor, 0L).isEmpty()).isTrue();
      assertThat(f.size()).isZero();
    } finally {
      ids.release(fnVictim);
      ids.release(fnSurvivor);
    }
  }

  @Test
  @DisplayName("corrupt checkpoint on open: graceful cold start, leftover meta cleaned")
  void corruptCheckpointGracefulRecovery(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://corrupt");
    // First, create a valid checkpoint via normal write+close.
    try (SsdFile f = new SsdFile(cfg(dir, 1L << 20, false), ids)) {
      f.open();
      f.write(new RawFileCacheKey(fn, 0L), payload(4096, (byte) 0x77));
      f.checkpoint();
    }
    // Corrupt the checkpoint.
    try (FileChannel ch = FileChannel.open(dir.resolve("cpt"), StandardOpenOption.WRITE)) {
      ch.write(ByteBuffer.wrap(new byte[] {0, 0, 0, 0}), 0L);
    }
    StringIdMap ids2 = new StringIdMap();
    try (SsdFile f = new SsdFile(cfg(dir, 1L << 20, false), ids2)) {
      f.open();
      // Cold start — no entry recovered.
      assertThat(f.find(fn, 0L).isEmpty()).isTrue();
      // Cleanup must have removed the corrupt cpt + tmp + log so they don't trip the next open.
      assertThat(Files.exists(dir.resolve("cpt"))).isFalse();
      assertThat(Files.exists(dir.resolve("cpt.tmp"))).isFalse();
    }
  }

  @Test
  @DisplayName("checkpoint disabled: pre-existing meta files are removed at open")
  void checkpointDisabledCleansStaleMeta(@TempDir Path dir) throws IOException {
    // Pre-seed stale meta files.
    Files.write(dir.resolve("cpt"), new byte[] {0});
    Files.write(dir.resolve("log"), new byte[] {0});
    Files.write(dir.resolve("cpt.tmp"), new byte[] {0});

    StringIdMap ids = new StringIdMap();
    try (SsdFile f = new SsdFile(cfg(dir, 0L, false), ids)) {
      f.open();
      assertThat(Files.exists(dir.resolve("cpt"))).isFalse();
      assertThat(Files.exists(dir.resolve("log"))).isFalse();
      assertThat(Files.exists(dir.resolve("cpt.tmp"))).isFalse();
    }
  }

  @Test
  @DisplayName(
      "recovery: only regions in the eviction log are writable; partial regions are sealed")
  void recoveryWritablePolicy(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://watermark");
    var key0 = new RawFileCacheKey(fn, 0L);
    try (SsdFile f = new SsdFile(cfg(dir, 1L << 20, false), ids)) {
      f.open();
      // Write a small entry into region 0 and checkpoint+close — region 0 is partially filled.
      f.write(key0, payload(4096, (byte) 0x77));
    }
    // Reopen — region 0 must NOT be writable; the next write should grow region 1 (or evict).
    StringIdMap ids2 = new StringIdMap();
    try (SsdFile f = new SsdFile(cfg(dir, 1L << 20, false), ids2)) {
      f.open();
      // Original entry must still be findable.
      assertThat(f.find(fn, 0L).isEmpty()).isFalse();
      // A second entry must NOT land in region 0; it should land at offset >= REGION_SIZE.
      long fn2 = ids2.makeId("file://w2");
      var w = f.write(new RawFileCacheKey(fn2, 0L), payload(4096, (byte) 0x88));
      assertThat(w).isPresent();
      assertThat(w.get().offset()).isGreaterThanOrEqualTo((long) SsdFile.REGION_SIZE);
      ids2.release(fn2);
    } finally {
      ids2.release(fn);
    }
  }

  @Test
  @DisplayName("disabled-checkpoint open: pre-existing data file regions are reused as writable")
  void disabledCheckpointReusesExistingDataFile(@TempDir Path dir) throws IOException {
    // Pre-seed the data file with 2 regions worth of garbage bytes (not zeros).
    long twoRegions = 2L * SsdFile.REGION_SIZE;
    try (FileChannel ch =
        FileChannel.open(
            dir.resolve("data"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      // Sparse write at the watermark — file size becomes 2 × REGION_SIZE.
      ByteBuffer marker = ByteBuffer.wrap(new byte[] {(byte) 0xCC});
      ch.write(marker, twoRegions - 1);
    }
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://reuse");
    try (SsdFile f = new SsdFile(cfg(dir, /* checkpointIntervalBytes= */ 0L, false), ids)) {
      f.open();
      // First write must land at offset 0 (region 0 is reused as scratch from the head).
      var w = f.write(new RawFileCacheKey(fn, 0L), payload(4096, (byte) 0x77));
      assertThat(w).isPresent();
      assertThat(w.get().offset()).isZero();
    } finally {
      ids.release(fn);
    }
    // File size after open+write should still floor-align to 2 regions.
    assertThat(Files.size(dir.resolve("data"))).isEqualTo(twoRegions);
  }

  @Test
  @DisplayName(
      "opportunistic compaction: log append failure preserves per-entry erasures, defers region clear")
  void opportunisticCompactionLogFailure(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fnVictim = ids.makeId("file://logfail-v");
    long fnSurvivor = ids.makeId("file://logfail-s");
    SsdFile f = new SsdFile(cfg(dir, /* checkpointIntervalBytes= */ 1L << 20, false), ids);
    try {
      f.open();
      f.write(new RawFileCacheKey(fnVictim, 0L), payload(8 * 1024 * 1024, (byte) 0xAA));
      f.write(new RawFileCacheKey(fnSurvivor, 0L), payload(1024, (byte) 0xBB));

      // Make the eviction log unappendable: replace the file with a directory of the same name.
      Files.delete(dir.resolve("log"));
      Files.createDirectory(dir.resolve("log"));
      try {
        // removeFileEntries should NOT throw — log-and-continue path activates.
        Set<Long> retained = f.removeFileEntries(Set.of(fnVictim));
        assertThat(retained).isEmpty();
        // Per-entry erasure of victim still happened.
        assertThat(f.find(fnVictim, 0L).isEmpty()).isTrue();
        // Survivor stays — compaction was deferred (no log) so its region is intact.
        assertThat(f.find(fnSurvivor, 0L).isEmpty()).isFalse();
      } finally {
        // Restore log to a regular file so close-time checkpoint+truncate can succeed.
        Files.delete(dir.resolve("log"));
      }
    } finally {
      f.close();
      ids.release(fnVictim);
      ids.release(fnSurvivor);
    }
  }

  @Test
  @DisplayName("disabled-checkpoint open: misaligned data file is floor-truncated to REGION_SIZE")
  void disabledCheckpointFloorTruncatesMisalignedFile(@TempDir Path dir) throws IOException {
    // Pre-seed REGION_SIZE + 12345 bytes — open should floor-truncate down to REGION_SIZE.
    long misaligned = (long) SsdFile.REGION_SIZE + 12345L;
    try (FileChannel ch =
        FileChannel.open(
            dir.resolve("data"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      ByteBuffer marker = ByteBuffer.wrap(new byte[] {(byte) 0xCC});
      ch.write(marker, misaligned - 1);
    }
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://misaligned");
    SsdFile.Config cfg =
        new SsdFile.Config(
            dir.resolve("data"),
            dir.resolve("cpt"),
            dir.resolve("log"),
            dir.resolve("cpt.tmp"),
            /* maxRegions= */ 4,
            /* maxEntries= */ 0,
            /* checkpointIntervalBytes= */ 0L,
            false,
            false);
    try (SsdFile f = new SsdFile(cfg, ids)) {
      f.open();
      assertThat(Files.size(dir.resolve("data"))).isEqualTo(SsdFile.REGION_SIZE);
      // Region 0 is writable; first write lands at offset 0.
      var w = f.write(new RawFileCacheKey(fn, 0L), payload(4096, (byte) 0x77));
      assertThat(w).isPresent();
      assertThat(w.get().offset()).isZero();
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("disabled-checkpoint open: existing regions are clamped to maxRegions")
  void disabledCheckpointClampsToMaxRegions(@TempDir Path dir) throws IOException {
    // Pre-seed 5 × REGION_SIZE bytes but maxRegions=2.
    long fiveRegions = 5L * SsdFile.REGION_SIZE;
    try (FileChannel ch =
        FileChannel.open(
            dir.resolve("data"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      ch.write(ByteBuffer.wrap(new byte[] {(byte) 0xCC}), fiveRegions - 1);
    }
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://clamp");
    try (SsdFile f = new SsdFile(cfg(dir, /* checkpointIntervalBytes= */ 0L, false), ids)) {
      f.open();
      // File should be truncated down from 5 regions to 2 (the maxRegions in cfg()).
      assertThat(Files.size(dir.resolve("data"))).isEqualTo(2L * SsdFile.REGION_SIZE);
      var w = f.write(new RawFileCacheKey(fn, 0L), payload(1024, (byte) 0x88));
      assertThat(w).isPresent();
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName(
      "recovery skips entries whose region index >= numRegions (defends against corrupt cpt)")
  void recoverySkipsEntriesPastNumRegions(@TempDir Path dir) throws IOException {
    // Hand-craft a checkpoint that claims numRegions=1 but contains an entry at region 2.
    Path cpt = dir.resolve("cpt");
    Path tmp = dir.resolve("cpt.tmp");
    Path log = dir.resolve("log");
    int maxRegions = 2;
    double[] scores = new double[maxRegions];
    Map<Long, String> fileIds = new HashMap<>();
    fileIds.put(99L, "file://forged");
    var entries =
        List.of(
            new Checkpoint.Entry(
                99L,
                (long) SsdFile.REGION_SIZE * 2, // offset in region 2 — past numRegions=1
                SsdRun.of((long) SsdFile.REGION_SIZE * 2, 4096).fileBits(),
                0));
    var snap = new Checkpoint.Snapshot(maxRegions, /* numRegions= */ 1, scores, fileIds, entries);
    Checkpoint.write(cpt, tmp, snap, false);
    Checkpoint.truncateLog(log);

    StringIdMap ids = new StringIdMap();
    try (SsdFile f = new SsdFile(cfg(dir, 1L << 20, false), ids)) {
      f.open();
      // Forged entry must be silently skipped — open does not throw.
      assertThat(f.size()).isZero();
      assertThat(f.find(99L, (long) SsdFile.REGION_SIZE * 2).isEmpty()).isTrue();
    } finally {
      ids.release(99L);
    }
  }

  @Test
  @DisplayName("recovery: eviction-log entries past numRegions are silently ignored (no OOB)")
  void recoveryIgnoresOutOfRangeEvictionLogEntries(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://oor");
    var key = new RawFileCacheKey(fn, 0L);
    try (SsdFile f = new SsdFile(cfg(dir, /* checkpointIntervalBytes= */ 1L << 20, false), ids)) {
      f.open();
      f.write(key, payload(4096, (byte) 0x55));
    }
    // Append a region index that's past the recovered numRegions.
    Checkpoint.appendEvictions(dir.resolve("log"), new int[] {99});

    StringIdMap ids2 = new StringIdMap();
    try (SsdFile f = new SsdFile(cfg(dir, 1L << 20, false), ids2)) {
      f.open();
      // Open must not throw OOB; the original entry in region 0 should still be present
      // (region 99 is out of range, so it can't have evicted region 0's entry).
      assertThat(f.find(fn, 0L).isEmpty()).isFalse();
    } finally {
      ids2.release(fn);
    }
  }

  @Test
  @DisplayName(
      "recovery rebuilds regionErased: post-recovery compaction triggers from re-erased gaps")
  void recoveryRebuildsRegionErased(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fnA = ids.makeId("file://A");
    long fnB = ids.makeId("file://B");
    long fnC = ids.makeId("file://C");
    // Write A (8 MiB), B (8 MiB), C (8 MiB) into region 0 — total 24 MiB, watermark 24 MiB.
    try (SsdFile f = new SsdFile(cfg(dir, /* checkpointIntervalBytes= */ 1L << 20, false), ids)) {
      f.open();
      f.write(new RawFileCacheKey(fnA, 0L), payload(8 * 1024 * 1024, (byte) 0xAA));
      f.write(new RawFileCacheKey(fnB, 0L), payload(8 * 1024 * 1024, (byte) 0xBB));
      f.write(new RawFileCacheKey(fnC, 0L), payload(8 * 1024 * 1024, (byte) 0xCC));
      // checkpoint() runs on close — entries A,B,C survive at full watermark, regionErased=0.
    }
    // Reopen and erase A+B (16 MiB > 50% of 24 MiB watermark) — opportunistic compaction
    // must fire, dropping C as collateral. This works ONLY if regionErased was correctly
    // rebuilt from (watermark - liveBytes) on recovery; if it had been left at 0, the
    // 16 MiB > 0.5*24 MiB threshold would not be met after one removeFileEntries call alone.
    StringIdMap ids2 = new StringIdMap();
    try (SsdFile f = new SsdFile(cfg(dir, 1L << 20, false), ids2)) {
      f.open();
      assertThat(f.size()).isEqualTo(3);
      Set<Long> retained = f.removeFileEntries(Set.of(fnA, fnB));
      assertThat(retained).isEmpty();
      // C must be gone too (compaction dropped the whole region).
      assertThat(f.find(fnC, 0L).isEmpty()).isTrue();
      assertThat(f.size()).isZero();
    } finally {
      ids2.release(fnA);
      ids2.release(fnB);
      ids2.release(fnC);
    }
  }

  @Test
  @DisplayName("eviction-log replay: entries in evicted regions are not re-added on recovery")
  void evictionLogReplaySkipsEvictedRegions(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://evlog");
    var key = new RawFileCacheKey(fn, 0L);

    try (SsdFile f = new SsdFile(cfg(dir, 1L << 20, false), ids)) {
      f.open();
      f.write(key, payload(4096, (byte) 0x33));
      // close() will fsync data + write checkpoint + truncate log.
    }
    // Append AFTER close so close()'s truncateLog does not wipe our forged record.
    Checkpoint.appendEvictions(dir.resolve("log"), new int[] {0});

    StringIdMap ids2 = new StringIdMap();
    try (SsdFile f = new SsdFile(cfg(dir, 1L << 20, false), ids2)) {
      f.open();
      // Entry must NOT be replayed because its region is in the eviction log.
      assertThat(f.find(fn, 0L).isEmpty()).isTrue();
    } finally {
      ids2.release(fn);
    }
  }
}
