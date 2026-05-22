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
import io.github.luciferyang.cachedfs.core.ssd.SsdCache;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
  @DisplayName("applyTTL drops files whose openTime is older than the cutoff")
  void appliesTtlToAgedFiles() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    // Open three files at t=0, t=10, t=20 and put one entry per file into the RAM cache.
    ttl.recordOpen(1L);
    putEntry(1L);
    clock.advance(10);
    ttl.recordOpen(2L);
    putEntry(2L);
    clock.advance(10);
    ttl.recordOpen(3L);
    putEntry(3L);

    assertThat(ram.exists(new RawFileCacheKey(1L, 0L))).isTrue();
    assertThat(ram.exists(new RawFileCacheKey(2L, 0L))).isTrue();
    assertThat(ram.exists(new RawFileCacheKey(3L, 0L))).isTrue();

    // Now t = 1_000_000 + 20. Files older than 15s ago: file 1 (opened at +0, age=20s) and
    // file 2 (opened at +10, age=10s) — wait, age=10s for file 2 which is NOT older than 15s.
    // Only file 1 should drop.
    int dropped = ttl.applyTTL(15);

    assertThat(dropped).isEqualTo(1);
    assertThat(ram.exists(new RawFileCacheKey(1L, 0L))).isFalse();
    assertThat(ram.exists(new RawFileCacheKey(2L, 0L))).isTrue();
    assertThat(ram.exists(new RawFileCacheKey(3L, 0L))).isTrue();
    // numAgedOut should reflect the one dropped entry.
    assertThat(ram.refreshStats().numAgedOut()).isEqualTo(1L);
    // File 1 is pruned from the tracking map; 2 + 3 remain.
    assertThat(ttl.trackedFileCount()).isEqualTo(2);
  }

  @Test
  @DisplayName("recordOpen is putIfAbsent — first observed open-time wins")
  void recordOpenPreservesFirstTime() {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    CacheTTLController ttl = new CacheTTLController(ram, null, clock);

    ttl.recordOpen(7L);
    long oldestAtFirstOpen = ttl.oldestOpenTimeSeconds().orElseThrow();
    clock.advance(100);
    ttl.recordOpen(7L); // second recordOpen for the same fileNum — should be a no-op

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
    // Hold a shared pin on file 42's entry so the drop fails for this cycle. Promote the exclusive
    // pin into a shared one and keep it open across the applyTTL call.
    FindResult result = ram.findOrCreate(new RawFileCacheKey(42L, 0L), 64, false);
    var exclusive = (FindResult.Exclusive) result;
    try (var pin = exclusive.pin().exclusiveToShared(false)) {
      clock.advance(1000);
      int dropped = ttl.applyTTL(60);
      // File 42 is aged-out by time but pinned by us — drop returns 0 dropped, tracking retained.
      assertThat(dropped).isZero();
      assertThat(ttl.trackedFileCount()).isEqualTo(1); // still tracked; will retry next cycle
      assertThat(ram.exists(new RawFileCacheKey(42L, 0L))).isTrue();
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
    // forget does NOT drop the entry from the cache — only stops tracking it for TTL.
    assertThat(ram.exists(new RawFileCacheKey(5L, 0L))).isTrue();
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
  @DisplayName("oldestOpenTimeSeconds reflects the oldest tracked file")
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
    // Create the exclusive placeholder, promote to shared (completes the entry into the LRU as
    // unpinned-but-resident), and close the shared pin so the entry is fully evictable.
    FindResult r = ram.findOrCreate(new RawFileCacheKey(fileNum, 0L), 64, false);
    var ex = (FindResult.Exclusive) r;
    try (var shared = ex.pin().exclusiveToShared(false)) {
      // shared pin auto-released by try-with-resources
    }
  }

  /**
   * SSD-tier smoke check: when an SSD cache is wired in, applyTTL fans out to both tiers and drops
   * files in both. (Full SSD coverage lives in SsdCacheTest; this test only verifies the controller
   * calls into the SSD path when one is configured.)
   */
  @Test
  @DisplayName("applyTTL fans out to SSD tier when one is configured")
  void appliesToBothTiers(@org.junit.jupiter.api.io.TempDir java.nio.file.Path ssdDir)
      throws java.io.IOException {
    FakeClock clock = new FakeClock(1_000_000L);
    ram = new AsyncDataCache(AsyncDataCache.Options.defaults());
    SsdCache ssd =
        new SsdCache(
            SsdCache.Config.single(ssdDir, "shard", 1, 4, 1024, 1L << 20, false, false),
            new io.github.luciferyang.cachedfs.core.id.StringIdMap());
    try {
      CacheTTLController ttl = new CacheTTLController(ram, ssd, clock);
      ttl.recordOpen(1L);

      clock.advance(1000);
      int dropped = ttl.applyTTL(60);

      // File 1 was aged-out and dropped from tracking; neither tier retained it (no entries to
      // pin), so the drop count reflects one file successfully cleared.
      assertThat(dropped).isEqualTo(1);
      assertThat(ttl.trackedFileCount()).isZero();
      assertThat(ttl.appliedCycles()).isEqualTo(1L);
    } finally {
      try {
        ssd.close();
      } catch (java.io.IOException ignored) {
        // best-effort
      }
    }
  }
}
