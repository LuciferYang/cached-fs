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
package io.github.luciferyang.cachedfs.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.luciferyang.cachedfs.core.stats.CacheStats;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AsyncDataCacheTest {

  @Test
  @DisplayName("miss then hit yields shared pin")
  void missThenHit() {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey key = new RawFileCacheKey(1L, 0L);

      FindResult miss = cache.findOrCreate(key, 4096, false);
      assertThat(miss).isInstanceOf(FindResult.Exclusive.class);

      try (CachePin shared = miss.pin().exclusiveToShared(false)) {
        assertThat(shared.entry().numPins()).isEqualTo(1);
      }

      Optional<FindResult> hit = cache.find(key);
      assertThat(hit).isPresent();
      assertThat(hit.get()).isInstanceOf(FindResult.Hit.class);
      try (CachePin pin = hit.get().pin()) {
        assertThat(pin.entry().fileNum()).isEqualTo(1L);
        assertThat(pin.entry().numPins()).isEqualTo(1);
      }

      CacheStats stats = cache.refreshStats();
      assertThat(stats.numNew()).isEqualTo(1L);
      assertThat(stats.numHit()).isEqualTo(1L);
    }
  }

  @Test
  @DisplayName("exclusive pin waiters are woken on promotion")
  void exclusiveWaitersWokenOnPromotion() throws Exception {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey key = new RawFileCacheKey(2L, 0L);
      FindResult first = cache.findOrCreate(key, 4096, false);
      Optional<FindResult> waiter = cache.find(key);
      assertThat(waiter.get()).isInstanceOf(FindResult.Waiting.class);
      CompletableFuture<Void> future = waiter.get().waitFuture();
      assertThat(future.isDone()).isFalse();
      first.pin().exclusiveToShared(false).close();
      future.get(2, TimeUnit.SECONDS);
      assertThat(cache.refreshStats().numWaitExclusive()).isEqualTo(1L);
    }
  }

  @Test
  @DisplayName("failed exclusive pin releases entry and wakes waiters")
  void failedExclusiveUnwinds() throws Exception {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey key = new RawFileCacheKey(3L, 0L);
      FindResult first = cache.findOrCreate(key, 4096, false);
      Optional<FindResult> waiter = cache.find(key);
      assertThat(waiter.get()).isInstanceOf(FindResult.Waiting.class);
      first.pin().close(); // simulate load failure
      waiter.get().waitFuture().get(2, TimeUnit.SECONDS);
      assertThat(cache.find(key)).isEmpty();
    }
  }

  @Test
  @DisplayName("stale entry self-heals when larger size requested")
  void staleEntrySelfHeals() {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey key = new RawFileCacheKey(4L, 0L);
      cache.findOrCreate(key, 1024, false).pin().exclusiveToShared(false).close();
      FindResult large = cache.findOrCreate(key, 4096, false);
      assertThat(large).isInstanceOf(FindResult.Exclusive.class);
      assertThat(large.pin().entry().size()).isEqualTo(4096);
      large.pin().exclusiveToShared(false).close();

      CacheStats stats = cache.refreshStats();
      assertThat(stats.numStales()).isEqualTo(1L);
      assertThat(stats.numNew()).isEqualTo(2L);
    }
  }

  @Test
  @DisplayName("smaller request reuses larger cached entry without rebuilding")
  void smallerRequestReusesLargerEntry() {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey key = new RawFileCacheKey(40L, 0L);
      cache.findOrCreate(key, 8192, false).pin().exclusiveToShared(false).close();
      FindResult small = cache.findOrCreate(key, 1024, false);
      assertThat(small).isInstanceOf(FindResult.Hit.class);
      assertThat(small.pin().entry().size()).isEqualTo(8192);
      small.pin().close();
      assertThat(cache.refreshStats().numNew()).isEqualTo(1L);
      assertThat(cache.refreshStats().numStales()).isZero();
    }
  }

  @Test
  @DisplayName("clear() drops all unpinned entries")
  void clearDropsUnpinned() {
    AsyncDataCache.Options opts = new AsyncDataCache.Options(2, 0.7, 0.125, 1L << 20, 0L);
    try (AsyncDataCache cache = new AsyncDataCache(opts)) {
      for (long off = 0; off < 16; off++) {
        cache
            .findOrCreate(new RawFileCacheKey(99L, off * 4096L), 4096, false)
            .pin()
            .exclusiveToShared(false)
            .close();
      }
      assertThat(cache.refreshStats().numEntries()).isEqualTo(16);
      cache.clear();
      assertThat(cache.refreshStats().numEntries()).isZero();
    }
  }

  @Test
  @DisplayName("evict skips pinned entries even with evictAllUnpinned=true")
  void evictPreservesPinnedEntries() {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey pinnedKey = new RawFileCacheKey(50L, 0L);
      try (CachePin keep =
          cache.findOrCreate(pinnedKey, 4096, false).pin().exclusiveToShared(false)) {
        // populate other entries on different shards
        for (int i = 1; i <= 8; i++) {
          cache
              .findOrCreate(new RawFileCacheKey(50L + i, 0L), 4096, false)
              .pin()
              .exclusiveToShared(false)
              .close();
        }
        for (CacheShard s : new CacheShard[] {cache.shardFor(pinnedKey)}) {
          s.evict(Long.MAX_VALUE, true);
        }
        // pinnedKey still in its shard
        assertThat(cache.exists(pinnedKey)).isTrue();
      }
    }
  }

  @Test
  @DisplayName("prefetched entry first reuse does not count as hit")
  void prefetchPromotion() {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey key = new RawFileCacheKey(5L, 0L);
      FindResult fresh = cache.findOrCreate(key, 4096, false);
      fresh.pin().entry().setPrefetch(true);
      fresh.pin().exclusiveToShared(false).close();

      try (CachePin p = cache.find(key).orElseThrow().pin()) {
        assertThat(p.entry().getAndClearFirstUseFlag()).isTrue();
      }
      assertThat(cache.refreshStats().numHit()).isZero();

      cache.find(key).orElseThrow().pin().close();
      assertThat(cache.refreshStats().numHit()).isEqualTo(1L);
    }
  }

  @Test
  @DisplayName("cold-load fresh entry has firstUse flag set")
  void coldLoadSetsFirstUse() {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey key = new RawFileCacheKey(60L, 0L);
      FindResult fresh = cache.findOrCreate(key, 4096, false);
      assertThat(fresh.pin().entry().getAndClearFirstUseFlag()).isTrue();
      // second read returns false
      assertThat(fresh.pin().entry().getAndClearFirstUseFlag()).isFalse();
      fresh.pin().exclusiveToShared(false).close();
    }
  }

  @Test
  @DisplayName("findOrCreate concurrent — only one initialization happens")
  void findOrCreateSingleFlight() throws Exception {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults());
        ExecutorService pool = Executors.newFixedThreadPool(8)) {
      RawFileCacheKey key = new RawFileCacheKey(7L, 0L);
      CountDownLatch ready = new CountDownLatch(1);
      AtomicInteger exclusiveCount = new AtomicInteger();
      AtomicInteger waitingCount = new AtomicInteger();

      Future<?>[] futures = new Future[8];
      for (int i = 0; i < 8; i++) {
        futures[i] =
            pool.submit(
                () -> {
                  try {
                    ready.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  FindResult r = cache.findOrCreate(key, 4096, false);
                  if (r instanceof FindResult.Exclusive) exclusiveCount.incrementAndGet();
                  else if (r instanceof FindResult.Waiting) waitingCount.incrementAndGet();
                  if (r instanceof FindResult.Exclusive ex) {
                    ex.pin().exclusiveToShared(false).close();
                  }
                  return null;
                });
      }
      ready.countDown();
      for (Future<?> f : futures) f.get(2, TimeUnit.SECONDS);

      assertThat(exclusiveCount.get()).isEqualTo(1);
      assertThat(waitingCount.get()).isLessThanOrEqualTo(7);
      assertThat(cache.refreshStats().numNew()).isEqualTo(1L);
    }
  }

  @Test
  @DisplayName("makeEvictable + score-based evict reaps the marked entry first")
  void makeEvictablePreferred() {
    AsyncDataCache.Options opts = new AsyncDataCache.Options(1, 0.7, 0.125, 1L << 20, 0L);
    try (AsyncDataCache cache = new AsyncDataCache(opts)) {
      // Populate a couple of "warm" entries.
      RawFileCacheKey hot = new RawFileCacheKey(70L, 0L);
      cache.findOrCreate(hot, 4096, false).pin().exclusiveToShared(false).close();
      cache.exists(hot); // touch

      RawFileCacheKey doomed = new RawFileCacheKey(70L, 4096L);
      cache.findOrCreate(doomed, 4096, false).pin().exclusiveToShared(false).close();
      cache.makeEvictable(doomed);

      // Score-based evict (evictAllUnpinned=false) should drop the doomed entry first.
      cache.shardFor(doomed).evict(8192, false);
      assertThat(cache.exists(doomed)).isFalse();
      assertThat(cache.exists(hot)).isTrue();
    }
  }

  @Test
  @DisplayName("Options validates numShards, ratios, and byte thresholds")
  void optionsValidation() {
    // numShards: power-of-2 and positive
    assertThatThrownBy(() -> new AsyncDataCache.Options(3, 0.7, 0.125, 0L, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("numShards");
    assertThatThrownBy(() -> new AsyncDataCache.Options(0, 0.7, 0.125, 0L, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("numShards");
    // maxWriteRatio: [0,1]
    assertThatThrownBy(() -> new AsyncDataCache.Options(2, 1.5, 0.125, 0L, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxWriteRatio");
    assertThatThrownBy(() -> new AsyncDataCache.Options(2, -0.1, 0.125, 0L, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxWriteRatio");
    // ssdSavableRatio: [0,1]
    assertThatThrownBy(() -> new AsyncDataCache.Options(2, 0.7, 1.5, 0L, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ssdSavableRatio");
    assertThatThrownBy(() -> new AsyncDataCache.Options(2, 0.7, -0.1, 0L, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ssdSavableRatio");
    // minSsdSavableBytes: >= 0
    assertThatThrownBy(() -> new AsyncDataCache.Options(2, 0.7, 0.125, -1L, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("minSsdSavableBytes");
    // ssdFlushThresholdBytes: >= 0
    assertThatThrownBy(() -> new AsyncDataCache.Options(2, 0.7, 0.125, 0L, -1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ssdFlushThresholdBytes");
  }

  @Test
  @DisplayName("constructor rejects null Options with a named NPE")
  void constructorRejectsNullOptions() {
    assertThatThrownBy(() -> new AsyncDataCache(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("options");
  }

  @Test
  @DisplayName("getInstance returns Optional; close clears the singleton")
  void singletonLifecycle() {
    AsyncDataCache c = new AsyncDataCache(AsyncDataCache.Options.defaults());
    try {
      AsyncDataCache.setInstance(c);
      assertThat(AsyncDataCache.getInstance()).containsSame(c);
    } finally {
      c.close(); // ensures the static INSTANCE is cleared even on assertion failure
      AsyncDataCache.clearInstance();
    }
    assertThat(AsyncDataCache.getInstance()).isEmpty();
  }

  @Test
  @DisplayName("setInstance(null) is rejected — must use clearInstance()")
  void setInstanceRejectsNull() {
    assertThatThrownBy(() -> AsyncDataCache.setInstance(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  @DisplayName("findOrCreate that throws inside initialize() unwinds the exclusive placeholder")
  void findOrCreateInitializeThrowingUnwinds() throws Exception {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey key = new RawFileCacheKey(80L, 0L);
      // size <= 0 throws IllegalArgumentException inside CacheEntry.initialize.
      assertThatThrownBy(() -> cache.findOrCreate(key, 0, false))
          .isInstanceOf(IllegalArgumentException.class);
      // The entry must NOT be left in EXCLUSIVE state — a subsequent valid request must succeed
      // (proving the placeholder was unwound and waiters won't deadlock).
      FindResult retry = cache.findOrCreate(key, 4096, false);
      assertThat(retry).isInstanceOf(FindResult.Exclusive.class);
      retry.pin().exclusiveToShared(false).close();
    }
  }

  @Test
  @DisplayName("clearInstance() unregisters without close()")
  void clearInstanceUnregisters() {
    AsyncDataCache c = new AsyncDataCache(AsyncDataCache.Options.defaults());
    try {
      AsyncDataCache.setInstance(c);
      assertThat(AsyncDataCache.getInstance()).containsSame(c);
      AsyncDataCache.clearInstance();
      assertThat(AsyncDataCache.getInstance()).isEmpty();
    } finally {
      c.close();
    }
  }

  @Test
  @DisplayName(
      "removeFileEntries: one shard throws → other shards proceed, retained surfaces failed targets")
  void removeFileEntriesPerShardFailSoft() throws Exception {
    AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults());
    try {
      // Place a pre-populated entry on a NON-zero shard so a regression that early-aborts the
      // loop on shard 0 (instead of fail-soft + continue) leaves this entry alive and trips the
      // exists() assertion. Pick the first fileNum > 0 whose shardFor() is not shard 0.
      java.lang.reflect.Field shardsField = AsyncDataCache.class.getDeclaredField("shards");
      shardsField.setAccessible(true);
      CacheShard[] shards = (CacheShard[]) shardsField.get(cache);
      long victimFileNum = -1L;
      for (long fn = 1; fn < 1024 && victimFileNum < 0; fn++) {
        if (cache.shardFor(new RawFileCacheKey(fn, 0L)) != shards[0]) {
          victimFileNum = fn;
        }
      }
      assertThat(victimFileNum).as("found a fileNum that hashes off shard 0").isPositive();
      RawFileCacheKey victim = new RawFileCacheKey(victimFileNum, 0L);
      FindResult r = cache.findOrCreate(victim, 64, false);
      ((FindResult.Exclusive) r).pin().exclusiveToShared(false).close();
      assertThat(cache.exists(victim)).isTrue();

      CacheShard original = shards[0];
      CacheShard mocked = org.mockito.Mockito.mock(CacheShard.class);
      org.mockito.Mockito.when(mocked.removeFileEntries(org.mockito.ArgumentMatchers.anySet()))
          .thenThrow(new RuntimeException("simulated shard 0 failure"));
      try {
        shards[0] = mocked;

        java.util.Set<Long> targets = java.util.Set.of(victimFileNum, 9999L);
        java.util.Set<Long> retained = cache.removeFileEntries(targets);

        assertThat(retained)
            .as("failed shard reports all targets as retained")
            .containsAll(targets);
        // Returned set is the documented immutable contract — both tiers must agree.
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> retained.add(123L))
            .as("retained set must be immutable for symmetry with SsdCache")
            .isInstanceOf(UnsupportedOperationException.class);
        // Surviving shards actually executed: the non-zero shard's real removeFileEntries
        // dropped victim's entry. Regression: if catch was changed to early-abort, victim would
        // still exist.
        assertThat(cache.exists(victim))
            .as("non-failing shard must have processed its targets and removed the entry")
            .isFalse();
        // Confirm the mock was invoked (catch path actually triggered, not bypassed).
        org.mockito.Mockito.verify(mocked).removeFileEntries(org.mockito.ArgumentMatchers.anySet());
      } finally {
        shards[0] = original;
      }
    } finally {
      cache.close();
    }
  }
}
