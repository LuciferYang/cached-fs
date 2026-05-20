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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.luciferyang.cachedfs.core.RawFileCacheKey;
import io.github.luciferyang.cachedfs.core.id.StringIdMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SsdFileTest {

  private static SsdFile.Config config(Path dir, int maxRegions, boolean checksum) {
    return new SsdFile.Config(
        dir.resolve("data"),
        dir.resolve("cpt"),
        dir.resolve("log"),
        dir.resolve("cpt.tmp"),
        maxRegions,
        /* maxEntries= */ 0,
        /* checkpointIntervalBytes= */ 1L << 20, // 1 MiB — checkpoint after every batch in tests
        checksum,
        checksum);
  }

  private static ByteBuffer payload(int size, byte fill) {
    byte[] arr = new byte[size];
    Arrays.fill(arr, fill);
    return ByteBuffer.wrap(arr);
  }

  @Test
  @DisplayName("write then find returns the same bytes")
  void writeAndFind(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://a");
    try (SsdFile f = new SsdFile(config(dir, 4, false), ids)) {
      f.open();
      var key = new RawFileCacheKey(fn, 0L);
      var run = f.write(key, payload(8192, (byte) 0x42));
      assertThat(run).isPresent();
      assertThat(run.get().size()).isEqualTo(8192);

      try (SsdPin pin = f.find(fn, 0L)) {
        assertThat(pin.isEmpty()).isFalse();
        ByteBuffer dst = ByteBuffer.allocate(8192);
        f.load(pin, dst);
        assertThat(dst.position()).isEqualTo(8192);
        for (int i = 0; i < 8192; i++) assertThat(dst.get(i)).isEqualTo((byte) 0x42);
      }
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("find returns empty pin on miss")
  void findMiss(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    try (SsdFile f = new SsdFile(config(dir, 2, false), ids)) {
      f.open();
      assertThat(f.find(99L, 0L).isEmpty()).isTrue();
    }
  }

  @Test
  @DisplayName("region pin prevents eviction")
  void pinnedRegionNotEvicted(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://b");
    try (SsdFile f = new SsdFile(config(dir, 2, false), ids)) {
      f.open();
      var key = new RawFileCacheKey(fn, 0L);
      f.write(key, payload(8192, (byte) 1));

      try (SsdPin pin = f.find(fn, 0L)) {
        assertThat(f.testingRegionPins(0)).isEqualTo(1);
        // Fill enough to grow to maxRegions then trigger eviction request — pinned region 0
        // should not be selected. Use entries near max payload (8 MiB) so we fill regions fast
        // without needing tens of thousands of writes.
        int big = SsdRun.MAX_SIZE; // 8 MiB
        for (int i = 0; i < 32; i++) {
          var w = f.write(new RawFileCacheKey(fn, 8192L * (i + 1)), payload(big, (byte) i));
          if (w.isEmpty()) {
            break; // accept early if no space (eviction blocked by pin)
          }
        }
        // pin still alive; entry still findable; pin counter still 1.
        assertThat(f.find(fn, 0L).isEmpty()).isFalse();
        assertThat(f.testingRegionPins(0)).isEqualTo(2); // original pin + the find() above
      }
      // After try-with-resources closes the pin, the counter drops by 1 (the inner find still
      // holds the other reference until garbage-collected — but we expect the pin closed in
      // the try block to decrement once).
      assertThat(f.testingRegionPins(0)).isLessThanOrEqualTo(1);
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("checksum mismatch on read throws IOException")
  void checksumMismatch(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://c");
    try (SsdFile f = new SsdFile(config(dir, 2, true), ids)) {
      f.open();
      var key = new RawFileCacheKey(fn, 0L);
      var run = f.write(key, payload(4096, (byte) 0xAA));
      assertThat(run).isPresent();
      // Corrupt the data file by writing different bytes at the run's offset.
      try (FileChannel ch = FileChannel.open(dir.resolve("data"), StandardOpenOption.WRITE)) {
        ch.position(run.get().offset());
        ch.write(ByteBuffer.wrap(new byte[] {(byte) 0xBB}));
      }
      try (SsdPin pin = f.find(fn, 0L)) {
        ByteBuffer dst = ByteBuffer.allocate(4096);
        assertThatThrownBy(() -> f.load(pin, dst))
            .isInstanceOf(IOException.class)
            .hasMessageContaining("checksum mismatch");
      }
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("checkpoint + recovery: entries survive close/reopen")
  void checkpointAndRecover(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://d");
    var key = new RawFileCacheKey(fn, 0L);
    {
      try (SsdFile f = new SsdFile(config(dir, 2, false), ids)) {
        f.open();
        f.write(key, payload(4096, (byte) 0x55));
        f.checkpoint(); // force checkpoint
      }
    }
    // Reopen — entry must still be findable, with same bytes.
    {
      StringIdMap ids2 = new StringIdMap();
      try (SsdFile f = new SsdFile(config(dir, 2, false), ids2)) {
        f.open();
        try (SsdPin pin = f.find(fn, 0L)) {
          assertThat(pin.isEmpty()).isFalse();
          ByteBuffer dst = ByteBuffer.allocate(4096);
          f.load(pin, dst);
          assertThat(dst.array()[0]).isEqualTo((byte) 0x55);
          assertThat(dst.array()[4095]).isEqualTo((byte) 0x55);
        }
      } finally {
        ids2.release(fn);
      }
    }
  }

  @Test
  @DisplayName("isOutOfSpace detects 'No space left on device' message")
  void isOutOfSpaceDetector() {
    assertThat(SsdFile.isOutOfSpace(new IOException("No space left on device"))).isTrue();
    assertThat(SsdFile.isOutOfSpace(new IOException("Permission denied"))).isFalse();
    assertThat(SsdFile.isOutOfSpace(new IOException((String) null))).isFalse();
  }

  @Test
  @DisplayName("write returns Optional.empty when maxEntries cap is hit")
  void maxEntriesCap(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://cap");
    SsdFile.Config cfg =
        new SsdFile.Config(
            dir.resolve("data"),
            dir.resolve("cpt"),
            dir.resolve("log"),
            dir.resolve("cpt.tmp"),
            /* maxRegions= */ 2,
            /* maxEntries= */ 1,
            /* checkpointIntervalBytes= */ 0L,
            /* checksumEnabled= */ false,
            /* checksumReadVerificationEnabled= */ false);
    try (SsdFile f = new SsdFile(cfg, ids)) {
      f.open();
      assertThat(f.write(new RawFileCacheKey(fn, 0L), payload(1024, (byte) 1))).isPresent();
      assertThat(f.write(new RawFileCacheKey(fn, 1024L), payload(1024, (byte) 2))).isEmpty();
      assertThat(f.size()).isEqualTo(1);
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName(
      "load: empty pin throws IllegalStateException; small dst throws IllegalArgumentException")
  void loadGuards(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://guards");
    try (SsdFile f = new SsdFile(config(dir, 2, false), ids)) {
      f.open();
      assertThatThrownBy(() -> f.load(SsdPin.empty(), ByteBuffer.allocate(1024)))
          .isInstanceOf(IllegalStateException.class);

      f.write(new RawFileCacheKey(fn, 0L), payload(4096, (byte) 0x11));
      try (SsdPin pin = f.find(fn, 0L)) {
        assertThatThrownBy(() -> f.load(pin, ByteBuffer.allocate(1024)))
            .isInstanceOf(IllegalArgumentException.class);
      }
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("full region triggers eviction; new write succeeds after region freed")
  void fullRegionEvictionPath(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://evict");
    SsdFile.Config cfg =
        new SsdFile.Config(
            dir.resolve("data"),
            dir.resolve("cpt"),
            dir.resolve("log"),
            dir.resolve("cpt.tmp"),
            /* maxRegions= */ 2,
            /* maxEntries= */ 0,
            /* checkpointIntervalBytes= */ 0L,
            false,
            false);
    try (SsdFile f = new SsdFile(cfg, ids)) {
      f.open();
      int big = SsdRun.MAX_SIZE; // 8 MiB
      // Fill both regions exactly: 8 entries × 8 MiB = 64 MiB per region; 16 entries fills both.
      for (int i = 0; i < 16; i++) {
        var w = f.write(new RawFileCacheKey(fn, (long) i * big), payload(big, (byte) i));
        assertThat(w).isPresent();
      }
      // 17th write must trigger eviction (no pins held) and succeed.
      var w17 = f.write(new RawFileCacheKey(fn, 17L * big), payload(big, (byte) 99));
      assertThat(w17).isPresent();
      // At least one of the original entries must be gone.
      int survivors = 0;
      for (int i = 0; i < 16; i++) {
        if (!f.find(fn, (long) i * big).isEmpty()) survivors++;
      }
      assertThat(survivors).isLessThan(16);
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("checksum=true + verify=false: corrupt bytes are NOT detected on read")
  void checksumWithoutVerifyToleratesCorruption(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://chknoverify");
    SsdFile.Config cfg =
        new SsdFile.Config(
            dir.resolve("data"),
            dir.resolve("cpt"),
            dir.resolve("log"),
            dir.resolve("cpt.tmp"),
            /* maxRegions= */ 2,
            /* maxEntries= */ 0,
            /* checkpointIntervalBytes= */ 0L,
            /* checksumEnabled= */ true,
            /* checksumReadVerificationEnabled= */ false);
    try (SsdFile f = new SsdFile(cfg, ids)) {
      f.open();
      var key = new RawFileCacheKey(fn, 0L);
      var run = f.write(key, payload(4096, (byte) 0xAA));
      assertThat(run).isPresent();
      // Corrupt the bytes on disk.
      try (FileChannel ch = FileChannel.open(dir.resolve("data"), StandardOpenOption.WRITE)) {
        ch.position(run.get().offset());
        ch.write(ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}));
      }
      // load() must NOT throw because read-verification is disabled — corruption is silent.
      try (SsdPin pin = f.find(fn, 0L)) {
        ByteBuffer dst = ByteBuffer.allocate(4096);
        f.load(pin, dst);
        // First 3 bytes should be the corrupted 0xFF values, not the original 0xAA.
        assertThat(dst.array()[0]).isEqualTo((byte) 0xFF);
      }
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("load() after shard close throws ClosedChannelException")
  void loadAfterCloseThrowsClosedChannel(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://closed");
    SsdFile f = new SsdFile(config(dir, 2, false), ids);
    SsdPin pin = SsdPin.empty();
    try {
      f.open();
      f.write(new RawFileCacheKey(fn, 0L), payload(4096, (byte) 0x42));
      pin = f.find(fn, 0L);
      assertThat(pin.isEmpty()).isFalse();
      f.close();
      ByteBuffer dst = ByteBuffer.allocate(4096);
      SsdPin captured = pin;
      assertThatThrownBy(() -> f.load(captured, dst)).isInstanceOf(ClosedChannelException.class);
    } finally {
      // pin.close() decrements regionPins; safe even after channel close.
      pin.close();
      try {
        f.close();
      } catch (IOException ignored) {
        // already-closed channel is fine
      }
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("data-write ENOSPC transitions the shard to NO_SPACE and returns Optional.empty")
  void dataWriteEnospcTransitionsToNoSpace(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://enospc-data");
    try (SsdFile f = new SsdFile(config(dir, 2, false), ids)) {
      f.open();
      // Swap in a channel that fails every write — simulates underlying disk hitting ENOSPC.
      FileChannel real =
          FileChannel.open(dir.resolve("data"), StandardOpenOption.READ, StandardOpenOption.WRITE);
      f.testingReplaceChannel(FaultyFileChannel.failAllWrites(real));

      var w = f.write(new RawFileCacheKey(fn, 0L), payload(4096, (byte) 0x42));
      assertThat(w).isEmpty();
      assertThat(f.state()).isEqualTo(SsdFile.State.NO_SPACE);
      // Subsequent writes short-circuit on the NO_SPACE check.
      assertThat(f.write(new RawFileCacheKey(fn, 4096L), payload(1024, (byte) 0x43))).isEmpty();
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("grow ENOSPC falls through to eviction when candidates exist")
  void growEnospcFallsThroughToEviction(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://enospc-grow");
    SsdFile.Config cfg =
        new SsdFile.Config(
            dir.resolve("data"),
            dir.resolve("cpt"),
            dir.resolve("log"),
            dir.resolve("cpt.tmp"),
            /* maxRegions= */ 2,
            /* maxEntries= */ 0,
            /* checkpointIntervalBytes= */ 0L,
            false,
            false);
    try (SsdFile f = new SsdFile(cfg, ids)) {
      f.open();
      // Fill region 0 with 8 × 8 MiB entries — region 0 becomes full but not yet rolled.
      int big = SsdRun.MAX_SIZE;
      for (int i = 0; i < 8; i++) {
        var w = f.write(new RawFileCacheKey(fn, (long) i * big), payload(big, (byte) i));
        assertThat(w).isPresent();
      }
      // Now numRegions=1, region 0 is at watermark REGION_SIZE. Next write will roll, find
      // writableRegions empty, and try to grow region 1. Inject failure on the grow write.
      FileChannel real =
          FileChannel.open(dir.resolve("data"), StandardOpenOption.READ, StandardOpenOption.WRITE);
      // Grow targets the sparse byte at offset 2*REGION_SIZE - 1. Any write at or above that
      // position should fail; lower writes (the next entry's actual payload) should succeed.
      f.testingReplaceChannel(
          FaultyFileChannel.failGrowAt(real, (long) SsdFile.REGION_SIZE * 2 - 1));

      // 9th write — grow throws ENOSPC; growOrEvictLocked falls through to eviction; write
      // lands at offset 0 of evicted region 0.
      var w9 = f.write(new RawFileCacheKey(fn, 9L * big), payload(big, (byte) 99));
      assertThat(w9).isPresent();
      assertThat(w9.get().offset()).isZero();
      // State stays ACTIVE — only data-write ENOSPC sets NO_SPACE.
      assertThat(f.state()).isEqualTo(SsdFile.State.ACTIVE);
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("write into NO_SPACE shard returns Optional.empty without throwing")
  void writeOnNoSpace(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://nospace");
    try (SsdFile f = new SsdFile(config(dir, 2, false), ids)) {
      f.open();
      f.testingForceState(SsdFile.State.NO_SPACE);
      assertThat(f.write(new RawFileCacheKey(fn, 0L), payload(4096, (byte) 0x55))).isEmpty();
      assertThat(f.state()).isEqualTo(SsdFile.State.NO_SPACE);
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("removeFileEntries drops entries and reports retained pinned files")
  void removeFileEntries(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn1 = ids.makeId("file://e");
    long fn2 = ids.makeId("file://f");
    try (SsdFile f = new SsdFile(config(dir, 2, false), ids)) {
      f.open();
      f.write(new RawFileCacheKey(fn1, 0L), payload(4096, (byte) 1));
      f.write(new RawFileCacheKey(fn2, 0L), payload(4096, (byte) 2));
      assertThat(f.size()).isEqualTo(2);

      Set<Long> retained = f.removeFileEntries(Set.of(fn1));
      assertThat(retained).isEmpty();
      assertThat(f.find(fn1, 0L).isEmpty()).isTrue();
      assertThat(f.find(fn2, 0L).isEmpty()).isFalse();
    } finally {
      ids.release(fn1);
      ids.release(fn2);
    }
  }
}
