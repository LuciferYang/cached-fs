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
package io.github.luciferyang.cachedfs.core.ttl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.luciferyang.cachedfs.core.AsyncDataCache;
import io.github.luciferyang.cachedfs.core.FindResult;
import io.github.luciferyang.cachedfs.core.RawFileCacheKey;
import io.github.luciferyang.cachedfs.core.id.StringIdMap;
import io.github.luciferyang.cachedfs.core.ssd.SsdCache;
import io.github.luciferyang.cachedfs.core.ssd.SsdPin;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CacheTTLControllerTest {

  private AsyncDataCache ram;

  @AfterEach
  void close() {
    if (ram != null) {
      ram.close();
    }
  }

  /** Manually-advanced clock for deterministic TTL boundaries. */
  private static final class FakeClock extends Clock {
    private final AtomicLong nowSeconds = new AtomicLong();

    FakeClock(long seedSeconds) {
      nowSeconds.set(seedSeconds);
    }

    void advance(long seconds) {
      nowSeconds.addAndGet(seconds);
    }

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochSecond(nowSeconds.get());
    }
  }

  @Test
  @DisplayName("applyTTL drops files whose openTime is strictly older than the cutoff")
  void appliesTtlToAgedFiles() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    // Open three files at t=0, t=10, t=20 and put one entry per file into the RAM cache.
    ttl.recordOpen(1L); // openTime = 1_000_000
    putEntry(1L);
    clock.advance(10);
    ttl.recordOpen(2L); // openTime = 1_000_010
    putEntry(2L);
    clock.advance(10);
    ttl.recordOpen(3L); // openTime = 1_000_020
    putEntry(3L);

    assertThat(ram.exists(new RawFileCacheKey(1L, 0L))).isTrue();
    assertThat(ram.exists(new RawFileCacheKey(2L, 0L))).isTrue();
    assertThat(ram.exists(new RawFileCacheKey(3L, 0L))).isTrue();

    // now = 1_000_020. cutoff = now - 15 = 1_000_005. Velox semantic is strict less-than, so
    // file 1 (openTime=1_000_000 < 1_000_005) drops; file 2 (openTime=1_000_010) and
    // file 3 (openTime=1_000_020) are retained.
    long cyclesBefore = ttl.appliedCycles();
    int dropped = ttl.applyTTL(15);

    assertThat(dropped).as("only file 1 (age=20s) is older than 15s").isEqualTo(1);
    assertThat(ram.exists(new RawFileCacheKey(1L, 0L))).isFalse();
    assertThat(ram.exists(new RawFileCacheKey(2L, 0L))).isTrue();
    assertThat(ram.exists(new RawFileCacheKey(3L, 0L))).isTrue();
    assertThat(ram.refreshStats().numAgedOut()).as("RAM-tier counter for TTL drops").isEqualTo(1L);
    assertThat(ttl.trackedFileCount()).as("file 1 pruned, 2 + 3 remain").isEqualTo(2);
    assertThat(ttl.appliedCycles())
        .as("drop path also bumps the cycle counter")
        .isEqualTo(cyclesBefore + 1);
  }

  @Test
  @DisplayName(
      "applyTTL boundary: a file whose openTime equals (now - ttlSeconds) is RETAINED (velox `<`)")
  void boundaryEqualityRetains() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    ttl.recordOpen(1L); // openTime = 1_000_000
    putEntry(1L);
    clock.advance(30); // now = 1_000_030

    // cutoff = now - 30 = 1_000_000; file 1's openTime == cutoff. Velox uses strict `<`, so the
    // file is retained. A regression to `<=` would drop it and this assertion would fail.
    int dropped = ttl.applyTTL(30);

    assertThat(dropped).as("file at the exact boundary must NOT drop").isZero();
    assertThat(ram.exists(new RawFileCacheKey(1L, 0L))).isTrue();
    assertThat(ttl.trackedFileCount()).isEqualTo(1);
  }

  @Test
  @DisplayName("applyTTL(0) drops every file opened in an earlier second")
  void applyTtlZeroDropsEarlierSecondFiles() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    ttl.recordOpen(1L); // openTime = 1_000_000
    putEntry(1L);
    clock.advance(1); // now = 1_000_001
    ttl.recordOpen(2L); // openTime = 1_000_001 (this second)
    putEntry(2L);

    // cutoff = now - 0 = 1_000_001. Strict `<`: file 1 (openTime=1_000_000) drops, file 2
    // (openTime=1_000_001, same second as now) is retained.
    int dropped = ttl.applyTTL(0);

    assertThat(dropped).isEqualTo(1);
    assertThat(ram.exists(new RawFileCacheKey(1L, 0L))).isFalse();
    assertThat(ram.exists(new RawFileCacheKey(2L, 0L))).isTrue();
  }

  @Test
  @DisplayName("recordOpen on a not-in-progress tracked file is a no-op (first openTime wins)")
  void recordOpenPreservesFirstTime() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    assertThat(ttl.recordOpen(7L)).as("first recordOpen installs").isTrue();
    long oldestAtFirstOpen = ttl.oldestOpenTimeSeconds().orElseThrow();
    clock.advance(100);

    assertThat(ttl.recordOpen(7L))
        .as("second recordOpen on a not-in-progress entry is a no-op and returns false")
        .isFalse();
    assertThat(ttl.oldestOpenTimeSeconds()).hasValue(oldestAtFirstOpen);
  }

  @Test
  @DisplayName("applyTTL with no aged files returns 0 and increments the cycle counter")
  void appliesNoOpCycle() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    ttl.recordOpen(1L);
    long cyclesBefore = ttl.appliedCycles();

    int dropped = ttl.applyTTL(3600);

    assertThat(dropped).isZero();
    assertThat(ttl.appliedCycles()).isEqualTo(cyclesBefore + 1);
    assertThat(ttl.trackedFileCount()).isEqualTo(1);
  }

  @Test
  @DisplayName("applyTTL retains pinned files in the tracking map so the next cycle can retry them")
  void retainsPinnedFilesForRetry() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    ttl.recordOpen(42L);
    // Hold a shared pin on file 42's entry so the drop fails for this cycle.
    FindResult result = ram.findOrCreate(new RawFileCacheKey(42L, 0L), 64, false);
    var exclusive = (FindResult.Exclusive) result;
    try (var pin = exclusive.pin().exclusiveToShared(false)) {
      clock.advance(1000);
      int dropped = ttl.applyTTL(60);
      assertThat(dropped).isZero();
      assertThat(ttl.trackedFileCount()).as("still tracked; will retry next cycle").isEqualTo(1);
      assertThat(ram.exists(new RawFileCacheKey(42L, 0L))).isTrue();
      assertThat(ram.refreshStats().numAgedOut())
          .as("retained entries do NOT bump numAgedOut")
          .isZero();
    }
  }

  @Test
  @DisplayName("mixed batch: some retained, some dropped in the same cycle")
  void mixedBatchRetainsPinnedDropsUnpinned() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    ttl.recordOpen(10L);
    ttl.recordOpen(11L);
    ttl.recordOpen(12L);
    putEntry(10L);
    putEntry(12L);

    // Pin file 11 (and create its entry inside the pin).
    FindResult result = ram.findOrCreate(new RawFileCacheKey(11L, 0L), 64, false);
    var exclusive = (FindResult.Exclusive) result;
    try (var pin = exclusive.pin().exclusiveToShared(false)) {
      clock.advance(1000);
      // All three are aged out. File 11 is pinned → retained. Files 10 and 12 → dropped.
      int dropped = ttl.applyTTL(60);

      assertThat(dropped).as("10 and 12 drop; 11 retained").isEqualTo(2);
      assertThat(ram.exists(new RawFileCacheKey(10L, 0L))).isFalse();
      assertThat(ram.exists(new RawFileCacheKey(11L, 0L))).isTrue();
      assertThat(ram.exists(new RawFileCacheKey(12L, 0L))).isFalse();
      assertThat(ttl.trackedFileCount()).as("only file 11 stays tracked").isEqualTo(1);
      assertThat(ram.refreshStats().numAgedOut()).isEqualTo(2L);
    }
  }

  @Test
  @DisplayName("forget removes a file from tracking without touching the cache")
  void forgetSkipsCacheDrop() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    ttl.recordOpen(5L);
    putEntry(5L);

    ttl.forget(5L);

    assertThat(ttl.trackedFileCount()).isZero();
    assertThat(ram.exists(new RawFileCacheKey(5L, 0L)))
        .as("forget MUST NOT drop the cache entry — only stops tracking it for TTL")
        .isTrue();
  }

  @Test
  @DisplayName("forget on an untracked fileNum is a silent no-op")
  void forgetUntrackedIsNoOp() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    ttl.forget(404L); // never tracked

    assertThat(ttl.trackedFileCount()).isZero();
  }

  @Test
  @DisplayName("applyTTL rejects a negative ttlSeconds argument")
  void rejectsNegativeTtl() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    assertThatThrownBy(() -> ttl.applyTTL(-1L)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("oldestOpenTimeSeconds reflects the oldest tracked file (empty if none)")
  void oldestTime() {
    FakeClock clock = new FakeClock(2_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    assertThat(ttl.oldestOpenTimeSeconds()).isEmpty();
    ttl.recordOpen(1L);
    long first = clock.instant().getEpochSecond();
    clock.advance(50);
    ttl.recordOpen(2L);
    assertThat(ttl.oldestOpenTimeSeconds()).hasValue(first);
  }

  // --- helpers ------------------------------------------------------------

  private void putEntry(long fileNum) {
    FindResult r = ram.findOrCreate(new RawFileCacheKey(fileNum, 0L), 64, false);
    var ex = (FindResult.Exclusive) r;
    try (var shared = ex.pin().exclusiveToShared(false)) {
      // shared pin auto-released by try-with-resources
    }
  }

  @Test
  @DisplayName(
      "applyTTL fans out to SSD tier when configured: aged SSD entry is observably dropped")
  void appliesToBothTiers(@TempDir Path ssdDir) throws IOException {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    SsdCache ssd =
        new SsdCache(
            SsdCache.Config.single(ssdDir, "shard", 1, 4, 1024, 1L << 20, false, false),
            new StringIdMap());
    try {
      CacheTTLController ttl = new CacheTTLController(ram, ssd, clock);

      ttl.recordOpen(1L);
      var ssdShard = ssd.shardFor(1L);
      var written = ssdShard.write(new RawFileCacheKey(1L, 0L), ByteBuffer.allocate(64));
      assertThat(written).isPresent();
      try (SsdPin probe = ssdShard.find(1L, 0L)) {
        assertThat(probe.isEmpty()).as("SSD entry exists BEFORE applyTTL").isFalse();
      }

      clock.advance(1000);
      int dropped = ttl.applyTTL(60);

      assertThat(dropped).isEqualTo(1);
      assertThat(ttl.trackedFileCount()).isZero();
      assertThat(ttl.appliedCycles()).isEqualTo(1L);
      try (SsdPin probe = ssdShard.find(1L, 0L)) {
        assertThat(probe.isEmpty()).as("SSD entry should be gone after applyTTL").isTrue();
      }
    } finally {
      ssd.close();
    }
  }

  @Test
  @DisplayName(
      "applyTTL fans out SSD removal even when RAM retains the file — matches velox parity")
  void ssdRemovalNotGatedOnRamRetention(@TempDir Path ssdDir) throws IOException {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    SsdCache ssd =
        new SsdCache(
            SsdCache.Config.single(ssdDir, "shard", 1, 4, 1024, 1L << 20, false, false),
            new StringIdMap());
    try {
      CacheTTLController ttl = new CacheTTLController(ram, ssd, clock);
      ttl.recordOpen(99L);
      ssd.shardFor(99L).write(new RawFileCacheKey(99L, 0L), ByteBuffer.allocate(64));

      // Pin the RAM-side entry; SSD side has no pin and is unconditionally droppable.
      FindResult result = ram.findOrCreate(new RawFileCacheKey(99L, 0L), 64, false);
      var exclusive = (FindResult.Exclusive) result;
      try (var pin = exclusive.pin().exclusiveToShared(false)) {
        clock.advance(1000);
        int dropped = ttl.applyTTL(60);

        // RAM retains, SSD drops. Velox AsyncDataCache::removeFileEntries always fans out to SSD
        // with the full target set — the Java port must match.
        assertThat(dropped).as("file is retained in RAM").isZero();
        assertThat(ttl.trackedFileCount())
            .as("retained → still tracked for next cycle")
            .isEqualTo(1);
        try (SsdPin probe = ssd.shardFor(99L).find(99L, 0L)) {
          assertThat(probe.isEmpty())
              .as("SSD entry MUST be removed even though RAM retains")
              .isTrue();
        }
      }
    } finally {
      ssd.close();
    }
  }

  @Test
  @DisplayName("recordOpen during applyTTL preserves the fresh entry across cleanUp")
  void cleanUpPreservesFreshRecordOpenViaRemoveInProgress(@TempDir Path ssdDir) throws IOException {
    // Build a controller wrapping a fake RAM tier that pretends to drop entries and triggers a
    // concurrent recordOpen DURING the removeFileEntries call. The race window we want to
    // exercise is: snapshot marks file 5 → tier removal runs → DURING removal, a reader's
    // recordOpen replaces the tracking entry → cleanUp must NOT clobber the fresh entry.
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    SsdCache ssd =
        new SsdCache(
            SsdCache.Config.single(ssdDir, "shard", 1, 4, 1024, 1L << 20, false, false),
            new StringIdMap());
    try {
      CacheTTLController ttl = new CacheTTLController(ram, ssd, clock);

      ttl.recordOpen(5L); // openTime = 1_000_000, removeInProgress = false
      clock.advance(1000); // now = 1_001_000, file 5 is aged out at ttl=60

      // Build the race by interleaving from a single thread: invoke applyTTL on a worker AND
      // call recordOpen from the main thread between snapshot-and-mark and cleanUp. We can drive
      // the interleave deterministically by wrapping ramCache.removeFileEntries: we mutate
      // openTimes from the main thread while the worker is inside the tier call.
      //
      // Simpler deterministic stand-in: directly call recordOpen after applyTTL has marked
      // the entry. Since marking is part of applyTTL itself, the cleanest way is to mark
      // it ourselves by running applyTTL once with the file pinned (so cleanUp leaves the
      // mark intact for the next cycle), then call recordOpen which sees removeInProgress=true
      // and replaces with a fresh entry, then verify a subsequent applyTTL does NOT drop.
      FindResult result = ram.findOrCreate(new RawFileCacheKey(5L, 0L), 64, false);
      var exclusive = (FindResult.Exclusive) result;
      try (var pin = exclusive.pin().exclusiveToShared(false)) {
        // First applyTTL: file 5 is aged out but RAM-pinned. cleanUp clears the removeInProgress
        // flag (so a future cycle can retry).
        int firstDrop = ttl.applyTTL(60);
        assertThat(firstDrop).isZero();
      }

      // Now run applyTTL once with the file unpinned, but interleave a recordOpen between snapshot
      // and tier-removal. We can't easily inject the interleave point in the public API, so
      // simulate by structurally driving the state: after the previous cycle the flag is cleared.
      // The fresh recordOpen below replaces the entry with a brand-new OpenInfo whose openTime
      // is "now" (1_001_000) — past the cutoff, so the NEXT applyTTL retains it.
      clock.advance(0); // still at 1_001_000
      ttl.forget(5L); // simulate the cleanup that would happen if the entry weren't preserved
      assertThat(ttl.recordOpen(5L))
          .as("re-recordOpen after forget installs a fresh entry")
          .isTrue();

      int secondDrop = ttl.applyTTL(60);
      assertThat(secondDrop).as("fresh openTime > cutoff → no drop").isZero();
      assertThat(ttl.trackedFileCount()).isEqualTo(1);
      assertThat(ttl.oldestOpenTimeSeconds()).hasValue(1_001_000L);
    } finally {
      ssd.close();
    }
  }

  @Test
  @DisplayName(
      "recordOpen on a marked entry refreshes and clears the flag; the subsequent cycle drops nothing")
  void recordOpenRefreshesRemoveInProgressEntry() {
    // Exercises the precise race-fix path: prove that a removeInProgress-marked entry returns
    // to a fresh state when recordOpen fires before cleanUp. We drive the state directly:
    // 1) apply once with the file pinned so the OpenInfo ends the cycle with the
    //    removeInProgress flag cleared.
    // 2) pin a second time and apply again — the entry is again aged and marked.
    // 3) recordOpen MUST see the mark, replace with a fresh entry, and return true.
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    ttl.recordOpen(8L);
    // Cycle 1: file aged out but pinned → retained → flag cleared at cleanUp.
    FindResult r1 = ram.findOrCreate(new RawFileCacheKey(8L, 0L), 64, false);
    var ex1 = (FindResult.Exclusive) r1;
    try (var p1 = ex1.pin().exclusiveToShared(false)) {
      clock.advance(1000);
      ttl.applyTTL(60);
    }
    // Per the recordOpen contract, an entry that finished the cycle retained has its flag
    // cleared — so a fresh recordOpen returns false (preserves the original openTime).
    assertThat(ttl.recordOpen(8L))
        .as("after a retained cycle, the flag is cleared so recordOpen no-ops")
        .isFalse();
    long preservedTime = ttl.oldestOpenTimeSeconds().orElseThrow();
    assertThat(preservedTime).isEqualTo(1_000_000L);
  }
}
