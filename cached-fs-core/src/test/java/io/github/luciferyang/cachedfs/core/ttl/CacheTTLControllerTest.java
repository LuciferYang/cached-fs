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
  @DisplayName("recordOpen on a marked entry CAS-replaces with a fresh openTime and returns true")
  void recordOpenOnMarkedEntryReplaces() {
    // Exercises the race-fix branch directly. markForTesting drops the entry into the same
    // remove-in-progress state that applyTTL's snapshot loop produces, without thread interleaving.
    // recordOpen contract: marked → CAS-replace with fresh OpenInfo, return true; subsequent
    // applyTTL sees the fresh openTime and skips.
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    ttl.recordOpen(5L);
    ttl.markForTesting(5L);
    assertThat(ttl.isMarkedForTesting(5L)).as("seam installed the mark").isTrue();
    clock.advance(1000);

    boolean refreshed = ttl.recordOpen(5L);

    assertThat(refreshed).as("marked entry → CAS-replace path returns true").isTrue();
    assertThat(ttl.isMarkedForTesting(5L))
        .as("recordOpen MUST clear the mark on replace")
        .isFalse();
    assertThat(ttl.oldestOpenTimeSeconds())
        .as("openTime must be refreshed to the current clock")
        .hasValue(1_001_000L);

    // Sanity: a stricter applyTTL still does not drop because the openTime is now fresh. A
    // regression that left the marked OpenInfo in place with the stale openTime would drop.
    int dropped = ttl.applyTTL(60);
    assertThat(dropped).isZero();
    assertThat(ttl.trackedFileCount()).isEqualTo(1);
  }

  @Test
  @DisplayName("OpenInfo identity-equality invariant: structurally-equal values are NOT equals()")
  void openInfoIdentityEqualityInvariant() {
    // The CAS-protection in cleanUp relies on ConcurrentMap.remove(K, V) and replace(K, V, V)
    // comparing values via Object.equals — and OpenInfo intentionally does NOT override equals,
    // so two OpenInfo with the same field values are NOT equal. Convert OpenInfo to a record and
    // this test fails: recordOpen below would either be a no-op (record's structural equals lets
    // the snapshot value match the live value, but the new OpenInfo is structurally equal too,
    // so semantics shift) or the mark stays set. Pin via the public surface.
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    ttl.recordOpen(13L);
    ttl.markForTesting(13L);
    // Clock is at 1_000_000 still; the about-to-be-replaced OpenInfo has openTime=1_000_000,
    // removeInProgress=true. recordOpen CAS-replaces with OpenInfo(1_000_000, false). Field values
    // identical to the previous unmarked record, only the flag differs. A record-typed OpenInfo
    // with structural equals would not change semantics in this exact call (the new value
    // differs in the flag) — but the test below proves the FLAG was cleared after recordOpen.
    boolean refreshed = ttl.recordOpen(13L);

    assertThat(refreshed).as("CAS-replace path fires on marked entries").isTrue();
    assertThat(ttl.isMarkedForTesting(13L))
        .as("flag cleared by replace — proves a NEW OpenInfo instance was installed")
        .isFalse();
    assertThat(ttl.oldestOpenTimeSeconds())
        .as("openTime preserved because clock did not advance")
        .hasValue(1_000_000L);
  }

  @Test
  @DisplayName(
      "applyTTL on tier exception: appliedCycles still bumps, reset clears the mark, openTime preserved")
  void appliedCyclesIncrementsOnTierException() {
    FakeClock clock = new FakeClock(1_000_000L);
    // AsyncDataCache is final; use Mockito (mockito-inline default in 5.x) to mock the one method
    // applyTTL calls. The mock returns defaults for everything else, including close().
    AsyncDataCache throwingRam = org.mockito.Mockito.mock(AsyncDataCache.class);
    org.mockito.Mockito.when(throwingRam.removeFileEntries(org.mockito.ArgumentMatchers.anySet()))
        .thenThrow(new RuntimeException("simulated tier failure"));
    CacheTTLController ttl = new CacheTTLController(throwingRam, null, clock);
    ttl.recordOpen(7L); // openTime = 1_000_000
    clock.advance(1000); // now = 1_001_000

    long cyclesBefore = ttl.appliedCycles();
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> ttl.applyTTL(60))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("simulated tier failure");

    assertThat(ttl.appliedCycles())
        .as("finally block must run; appliedCycles is an accurate health signal even on failure")
        .isEqualTo(cyclesBefore + 1);
    // Velox-parity reset: marked flag cleared, openTime preserved so the next cycle re-evaluates
    // against the ORIGINAL openTime (not refreshed). A regression that dropped reset() would leave
    // the flag set, and a subsequent recordOpen would refresh the openTime and silently extend the
    // file's lifetime past the TTL window.
    assertThat(ttl.isMarkedForTesting(7L))
        .as("reset() must clear the removeInProgress flag on tier-exception failure")
        .isFalse();
    assertThat(ttl.oldestOpenTimeSeconds())
        .as("original openTime preserved across the failed cycle")
        .hasValue(1_000_000L);
    // ram is null so the @AfterEach close() is skipped.
  }

  @Test
  @DisplayName("OpenInfo MUST NOT be a record — structural equals would break cleanUp CAS")
  void openInfoIsNotARecord() {
    // Direct regression guard: if a future contributor converts the OpenInfo inner class to a
    // record (an attractive "modernize value type" refactor flagged by the project's Java
    // coding-style rule preferring records), structural equals would silently break the
    // cleanUp() compare-and-remove protocol. Lock it in via reflection so the invariant fails
    // visibly at test time.
    Class<?>[] innerClasses = CacheTTLController.class.getDeclaredClasses();
    Class<?> openInfo = null;
    for (Class<?> inner : innerClasses) {
      if (inner.getSimpleName().equals("OpenInfo")) {
        openInfo = inner;
        break;
      }
    }
    assertThat(openInfo).as("OpenInfo inner class must exist").isNotNull();
    assertThat(openInfo.isRecord())
        .as("OpenInfo MUST NOT be a record; identity equals is load-bearing for cleanUp CAS")
        .isFalse();
  }

  @Test
  @DisplayName("applyTTL rejects ttlSeconds larger than current epoch — would underflow the cutoff")
  void rejectsTtlLargerThanEpoch() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    assertThatThrownBy(() -> ttl.applyTTL(Long.MAX_VALUE))
        .as("Long.MAX_VALUE would underflow cutoff; must throw not silently drop everything")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("underflows");
  }
}
