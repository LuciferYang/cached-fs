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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SsdCacheTest {

  @Test
  @DisplayName("shardFor routes by fileNum % numShards")
  void shardForRouting(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    var cfg = SsdCache.Config.single(dir, "ssd", 4, 2, 0, 0L, false, false);
    try (SsdCache c = new SsdCache(cfg, ids)) {
      assertThat(c.shardFor(0L)).isSameAs(c.shard(0));
      assertThat(c.shardFor(1L)).isSameAs(c.shard(1));
      assertThat(c.shardFor(5L)).isSameAs(c.shard(1));
      assertThat(c.shardFor(7L)).isSameAs(c.shard(3));
      // negative fileNum (Math.floorMod handles correctly)
      assertThat(c.shardFor(-1L)).isSameAs(c.shard(3));
    }
  }

  @Test
  @DisplayName("checksumReadVerificationEnabled requires checksumEnabled")
  void verifyRequiresChecksum(@TempDir Path dir) {
    assertThatThrownBy(() -> SsdCache.Config.single(dir, "ssd", 1, 1, 0, 0L, false, true))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("write to one shard does not affect others")
  void shardIsolation(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn0 = ids.makeId("file://0");
    var cfg = SsdCache.Config.single(dir, "ssd", 2, 2, 0, 0L, false, false);
    try (SsdCache c = new SsdCache(cfg, ids)) {
      c.shardFor(fn0).write(new RawFileCacheKey(fn0, 0L), ByteBuffer.wrap(new byte[4096]));
      // Each shard is independent
      assertThat(c.shard(0).size() + c.shard(1).size()).isEqualTo(1);
    } finally {
      ids.release(fn0);
    }
  }

  @Test
  @DisplayName("removeFileEntries aggregates retained file ids across shards")
  void removeFileEntriesAggregatesAcrossShards(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fnA = ids.makeId("file://A"); // shard for fnA % 2
    long fnB = ids.makeId("file://B");
    long fnC = 0L; // optional third id only minted if A,B collide on the same shard
    boolean collides = Math.floorMod(fnA, 2) == Math.floorMod(fnB, 2);
    if (collides) {
      fnC = ids.makeId("file://C");
    }
    var cfg = SsdCache.Config.single(dir, "ssd", 2, 2, 0, 0L, false, false);
    try (SsdCache c = new SsdCache(cfg, ids)) {
      long pair0 = fnA;
      long pair1 = collides ? fnC : fnB;

      c.shardFor(pair0).write(new RawFileCacheKey(pair0, 0L), ByteBuffer.wrap(new byte[1024]));
      c.shardFor(pair1).write(new RawFileCacheKey(pair1, 0L), ByteBuffer.wrap(new byte[1024]));

      try (SsdPin pinA = c.shardFor(pair0).find(pair0, 0L);
          SsdPin pinB = c.shardFor(pair1).find(pair1, 0L)) {
        Set<Long> retained = c.removeFileEntries(Set.of(pair0, pair1));
        assertThat(retained).contains(pair0, pair1);
      }
    } finally {
      ids.release(fnA);
      ids.release(fnB);
      if (collides) {
        ids.release(fnC);
      }
    }
  }

  @Test
  @DisplayName("shard(int) bounds-checks the index")
  void shardIndexBoundsCheck(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    var cfg = SsdCache.Config.single(dir, "ssd", 2, 2, 0, 0L, false, false);
    try (SsdCache c = new SsdCache(cfg, ids)) {
      assertThatThrownBy(() -> c.shard(2)).isInstanceOf(IndexOutOfBoundsException.class);
      assertThatThrownBy(() -> c.shard(-1)).isInstanceOf(IndexOutOfBoundsException.class);
    }
  }

  @Test
  @DisplayName("removeFileEntries rejects null input")
  void removeFileEntriesNullThrows(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    var cfg = SsdCache.Config.single(dir, "ssd", 1, 1, 0, 0L, false, false);
    try (SsdCache c = new SsdCache(cfg, ids)) {
      assertThatThrownBy(() -> c.removeFileEntries(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("filesToRemove");
    }
  }

  @Test
  @DisplayName("close aggregates per-shard checkpoint IOException as primary + suppressed")
  void closeAggregatesSuppressed(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://closesup");
    var cfg = SsdCache.Config.single(dir, "ssd", 2, 2, 0, 1L << 20, false, false);
    SsdCache c = new SsdCache(cfg, ids);
    try {
      c.shard(0).write(new RawFileCacheKey(0L, 0L), ByteBuffer.wrap(new byte[1024]));
      c.shard(1).write(new RawFileCacheKey(1L, 0L), ByteBuffer.wrap(new byte[1024]));
      // Force the close-time checkpoint to fail on BOTH shards by replacing each .cpt.tmp
      // path with a directory of the same name. SsdCache.close() must surface the first
      // IOException as primary and add the second as suppressed.
      Files.createDirectories(dir.resolve("ssd-0.cpt.tmp"));
      Files.createDirectories(dir.resolve("ssd-1.cpt.tmp"));
      assertThatThrownBy(c::close)
          .isInstanceOf(IOException.class)
          .satisfies(t -> assertThat(t.getSuppressed()).hasSize(1));
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("checkpointIntervalBytes is divided across shards (per-shard threshold)")
  void perShardCheckpointIntervalDivision(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://psi");
    // cache-wide interval = 4 MiB / 2 shards = 2 MiB per shard. One write of 3 MiB to a single
    // shard should cross the per-shard threshold and create the .cpt file; the other shard
    // stays untouched and produces no checkpoint.
    long total = 4L << 20;
    var cfg = SsdCache.Config.single(dir, "ssd", 2, 2, 0, total, false, false);
    try (SsdCache c = new SsdCache(cfg, ids)) {
      SsdFile target = c.shardFor(fn);
      int targetIdx = (target == c.shard(0)) ? 0 : 1;
      target.write(new RawFileCacheKey(fn, 0L), ByteBuffer.wrap(new byte[3 << 20]));
      // Auto-checkpoint should have fired on the target shard.
      assertThat(Files.exists(dir.resolve("ssd-" + targetIdx + ".cpt"))).isTrue();
      int otherIdx = 1 - targetIdx;
      assertThat(Files.exists(dir.resolve("ssd-" + otherIdx + ".cpt"))).isFalse();
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("checkpoint aggregates per-shard IOException as primary + suppressed")
  void checkpointAggregatesSuppressed(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://chk");
    var cfg = SsdCache.Config.single(dir, "ssd", 2, 2, 0, 1L << 20, false, false);
    try (SsdCache c = new SsdCache(cfg, ids)) {
      // Stage a write into each shard so checkpointLocked has work to flush.
      c.shard(0).write(new RawFileCacheKey(0L, 0L), ByteBuffer.wrap(new byte[1024]));
      c.shard(1).write(new RawFileCacheKey(1L, 0L), ByteBuffer.wrap(new byte[1024]));
      // Replace each shard's checkpoint-tmp path with a directory so the staging-file open
      // throws IOException for BOTH shards. SsdCache.checkpoint must surface one IOException
      // as primary and the other as suppressed.
      Files.createDirectories(dir.resolve("ssd-0.cpt.tmp"));
      Files.createDirectories(dir.resolve("ssd-1.cpt.tmp"));
      assertThatThrownBy(c::checkpoint)
          .isInstanceOf(IOException.class)
          .satisfies(t -> assertThat(t.getSuppressed()).hasSize(1));
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("constructor rolls back partially-opened shards if a later shard fails")
  void constructorRollbackOnPartialOpen(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    // Pre-create shard-1's data path AS A DIRECTORY so its FileChannel.open will fail with
    // FileSystemException, after shard-0 has been successfully opened. Verifies the rollback
    // path closes shard-0 instead of leaking the channel.
    Files.createDirectories(dir.resolve("ssd-1.data"));

    var cfg = SsdCache.Config.single(dir, "ssd", 3, 2, 0, 0L, false, false);
    assertThatThrownBy(() -> new SsdCache(cfg, ids)).isInstanceOf(IOException.class);

    // shard-0's data file should exist but be closed — verify by reopening with a fresh cache
    // after the directory blocker is removed.
    Files.delete(dir.resolve("ssd-1.data"));
    try (SsdCache c2 = new SsdCache(cfg, ids)) {
      assertThat(c2.numShards()).isEqualTo(3);
    }
  }

  // --- multi-directory tests -------------------------------------------------

  @Test
  @DisplayName("multi-directory: shards land round-robin across the configured paths")
  void multiDirRoundRobinPlacement(@TempDir Path dirA, @TempDir Path dirB) throws IOException {
    StringIdMap ids = new StringIdMap();
    // 4 shards across 2 directories — expect shards 0,2 in dirA and shards 1,3 in dirB.
    var cfg =
        new SsdCache.Config(
            List.of(dirA, dirB),
            "ssd",
            /* numShards= */ 4,
            /* regionsPerShard= */ 2,
            /* maxEntriesPerShard= */ 0,
            /* checkpointIntervalBytes= */ 0L,
            /* checksumEnabled= */ false,
            /* checksumReadVerificationEnabled= */ false);
    try (SsdCache c = new SsdCache(cfg, ids)) {
      assertThat(Files.exists(dirA.resolve("ssd-0.data"))).isTrue();
      assertThat(Files.exists(dirB.resolve("ssd-1.data"))).isTrue();
      assertThat(Files.exists(dirA.resolve("ssd-2.data"))).isTrue();
      assertThat(Files.exists(dirB.resolve("ssd-3.data"))).isTrue();
      // Cross-mount false positives — a shard's files must NOT appear in the wrong directory.
      assertThat(Files.exists(dirB.resolve("ssd-0.data"))).isFalse();
      assertThat(Files.exists(dirA.resolve("ssd-1.data"))).isFalse();
    }
  }

  @Test
  @DisplayName("multi-directory: numShards independent of directory count (8 shards / 2 dirs)")
  void multiDirShardsExceedDirs(@TempDir Path dirA, @TempDir Path dirB) throws IOException {
    StringIdMap ids = new StringIdMap();
    var cfg = new SsdCache.Config(List.of(dirA, dirB), "ssd", 8, 2, 0, 0L, false, false);
    try (SsdCache c = new SsdCache(cfg, ids)) {
      assertThat(c.numShards()).isEqualTo(8);
      // Even shards in dirA, odd shards in dirB.
      for (int i = 0; i < 8; i++) {
        Path expected = (i % 2 == 0) ? dirA : dirB;
        assertThat(Files.exists(expected.resolve("ssd-" + i + ".data"))).isTrue();
      }
    }
  }

  @Test
  @DisplayName("multi-directory: per-disk failure isolation — one disk ENOSPC, others keep working")
  void multiDirPerDiskFailureIsolation(@TempDir Path dirA, @TempDir Path dirB) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fnA = ids.makeId("file://A");
    long fnB = ids.makeId("file://B");
    var cfg = new SsdCache.Config(List.of(dirA, dirB), "ssd", 2, 2, 0, 0L, false, false);
    try (SsdCache c = new SsdCache(cfg, ids)) {
      // Force shard 0 (on dirA) into NO_SPACE; shard 1 (on dirB) stays ACTIVE and serves writes.
      c.shard(0).testingForceState(SsdFile.State.NO_SPACE);
      assertThat(c.shard(0).state()).isEqualTo(SsdFile.State.NO_SPACE);
      assertThat(c.shard(1).state()).isEqualTo(SsdFile.State.ACTIVE);

      // A write to shard 0 returns empty (NO_SPACE), shard 1 succeeds.
      assertThat(c.shard(0).write(new RawFileCacheKey(0L, 0L), ByteBuffer.wrap(new byte[1024])))
          .isEmpty();
      assertThat(c.shard(1).write(new RawFileCacheKey(1L, 0L), ByteBuffer.wrap(new byte[1024])))
          .isPresent();
    } finally {
      ids.release(fnA);
      ids.release(fnB);
    }
  }

  @Test
  @DisplayName("multi-directory: fail-fast when a configured directory is missing")
  void multiDirFailFastOnMissing(@TempDir Path dir) {
    StringIdMap ids = new StringIdMap();
    Path missing = dir.resolve("not-mounted");
    var cfg = new SsdCache.Config(List.of(dir, missing), "ssd", 2, 2, 0, 0L, false, false);
    assertThatThrownBy(() -> new SsdCache(cfg, ids))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("not-mounted");
  }

  @Test
  @DisplayName("multi-directory: fail-fast when a configured directory points to a regular file")
  void multiDirFailFastOnNonDirectory(@TempDir Path dirA, @TempDir Path dirB) throws IOException {
    StringIdMap ids = new StringIdMap();
    Path file = dirB.resolve("not-a-directory");
    Files.writeString(file, "blocker");
    var cfg = new SsdCache.Config(List.of(dirA, file), "ssd", 2, 2, 0, 0L, false, false);
    assertThatThrownBy(() -> new SsdCache(cfg, ids))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("not-a-directory");
  }

  @Test
  @DisplayName("multi-directory: empty directories list rejected at construction")
  void multiDirEmptyListRejected() {
    assertThatThrownBy(() -> new SsdCache.Config(List.of(), "ssd", 1, 1, 0, 0L, false, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("directories");
  }

  @Test
  @DisplayName("multi-directory: duplicate directory paths rejected at construction")
  void multiDirDuplicateRejected(@TempDir Path dirA) {
    StringIdMap ids = new StringIdMap();
    assertThatThrownBy(
            () -> new SsdCache.Config(List.of(dirA, dirA), "ssd", 2, 2, 0, 0L, false, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("duplicate directory");
  }

  @Test
  @DisplayName("multi-directory: null directories list rejected at construction")
  void multiDirNullListRejected() {
    assertThatThrownBy(() -> new SsdCache.Config(null, "ssd", 1, 1, 0, 0L, false, false))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  @DisplayName(
      "close() with auto-checkpointing enabled flushes final state — entries recover on reopen")
  void closeWithAutoCheckpointEnabledRecovers(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://durable");
    RawFileCacheKey key = new RawFileCacheKey(fn, 0L);
    byte[] payload = new byte[1024];
    for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i & 0xff);

    // checkpointIntervalBytes = 1 MiB enables checkpointing; SsdFile.close() runs a final
    // checkpoint via checkpointLocked() (velox parity). Without that internal checkpoint, the
    // entry written below would not survive close → reopen → recover.
    var cfg = SsdCache.Config.single(dir, "ssd", 1, 2, 0, 1L << 20, false, false);
    SsdCache c1 = new SsdCache(cfg, ids);
    try {
      c1.shardFor(fn).write(key, ByteBuffer.wrap(payload));
    } finally {
      c1.close();
    }

    StringIdMap ids2 = new StringIdMap();
    long fn2 = ids2.makeId("file://durable");
    SsdCache c2 = new SsdCache(cfg, ids2);
    try {
      try (SsdPin probe = c2.shardFor(fn2).find(fn2, 0L)) {
        assertThat(probe.isEmpty())
            .as("entry recovered from the checkpoint written by SsdFile.close()")
            .isFalse();
      }
    } finally {
      c2.close();
      ids2.release(fn2);
      ids.release(fn);
    }
  }

  @Test
  @DisplayName(
      "removeFileEntries: one SSD shard throws → other shards proceed, targets surface as retained")
  void removeFileEntriesPerShardFailSoft(@TempDir Path dir) throws Exception {
    StringIdMap ids = new StringIdMap();
    var cfg = SsdCache.Config.single(dir, "ssd", 4, 2, 64, 1L << 20, false, false);
    SsdCache cache = new SsdCache(cfg, ids);
    try {
      // Swap shard 0 for a Mockito mock that throws on removeFileEntries — mirrors the
      // AsyncDataCacheTest pattern. Locks in the SSD-side per-shard fail-soft contract added in
      // SsdCache.removeFileEntries.
      java.lang.reflect.Field shardsField = SsdCache.class.getDeclaredField("shards");
      shardsField.setAccessible(true);
      SsdFile[] shards = (SsdFile[]) shardsField.get(cache);
      SsdFile original = shards[0];
      SsdFile mocked = org.mockito.Mockito.mock(SsdFile.class);
      org.mockito.Mockito.when(mocked.removeFileEntries(org.mockito.ArgumentMatchers.anySet()))
          .thenThrow(new RuntimeException("simulated SSD shard 0 failure"));
      try {
        shards[0] = mocked;

        Set<Long> targets = Set.of(0L, 4L, 8L);
        Set<Long> retained = cache.removeFileEntries(targets);

        assertThat(retained)
            .as("failed SSD shard reports all targets as retained so the TTL controller retries")
            .containsAll(targets);
        assertThatThrownBy(() -> retained.add(123L))
            .as("retained set must be immutable")
            .isInstanceOf(UnsupportedOperationException.class);
        // The mock was actually invoked (not short-circuited by the catch).
        org.mockito.Mockito.verify(mocked).removeFileEntries(org.mockito.ArgumentMatchers.anySet());
      } finally {
        shards[0] = original;
      }
    } finally {
      cache.close();
    }
  }
}
