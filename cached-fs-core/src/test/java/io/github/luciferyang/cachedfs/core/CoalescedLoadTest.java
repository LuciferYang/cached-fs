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

import io.github.luciferyang.cachedfs.core.CoalescedLoad.LoadResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CoalescedLoadTest {

  /** Test load that creates exclusive entries via the cache and returns them. */
  static final class TestLoad extends CoalescedLoad {
    final AsyncDataCache cache;
    final AtomicInteger loadDataCount = new AtomicInteger();
    final CountDownLatch hold;

    TestLoad(
        AsyncDataCache cache,
        List<RawFileCacheKey> keys,
        List<Integer> sizes,
        CountDownLatch hold) {
      super(keys, sizes);
      this.cache = cache;
      this.hold = hold;
    }

    @Override
    public long size() {
      return sizes().stream().mapToLong(Integer::longValue).sum();
    }

    @Override
    public boolean isSsdLoad() {
      return false;
    }

    @Override
    protected List<CachePin> loadData(boolean prefetch) {
      loadDataCount.incrementAndGet();
      if (hold != null) {
        try {
          hold.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      List<CachePin> pins = new ArrayList<>();
      for (int i = 0; i < keys().size(); i++) {
        FindResult r = cache.findOrCreate(keys().get(i), sizes().get(i), false);
        if (r instanceof FindResult.Exclusive ex) {
          pins.add(ex.pin());
        }
      }
      return pins;
    }
  }

  @Test
  @DisplayName("PLANNED → loadOrFuture runs loadData, transitions to LOADED, holds shared pins")
  void plannedLoadsAndTransitions() {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey k = new RawFileCacheKey(10L, 0L);
      try (TestLoad load = new TestLoad(cache, List.of(k), List.of(4096), null)) {
        assertThat(load.state()).isEqualTo(CoalescedLoad.State.PLANNED);
        assertThat(load.loadOrFuture(false, false)).isInstanceOf(LoadResult.Done.class);
        assertThat(load.state()).isEqualTo(CoalescedLoad.State.LOADED);
        assertThat(load.loadDataCount.get()).isEqualTo(1);
        assertThat(load.pins()).hasSize(1);
        assertThat(load.pins().get(0).entry().numPins()).isEqualTo(1);
      }
      // After close(), pin is released and entry becomes evictable but still findable
      assertThat(cache.find(k)).isPresent();
    }
  }

  @Test
  @DisplayName("LOADING with wait=true returns Pending; future completes on load finish")
  void loadingWaitTrueReturnsFuture() throws Exception {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults());
        ExecutorService pool = Executors.newCachedThreadPool()) {
      RawFileCacheKey k = new RawFileCacheKey(11L, 0L);
      CountDownLatch hold = new CountDownLatch(1);
      try (TestLoad load = new TestLoad(cache, List.of(k), List.of(4096), hold)) {
        var first = pool.submit(() -> load.loadOrFuture(false, false));
        long deadline = System.nanoTime() + 2_000_000_000L;
        while (load.state() != CoalescedLoad.State.LOADING && System.nanoTime() < deadline) {
          Thread.yield();
        }
        assertThat(load.state()).isEqualTo(CoalescedLoad.State.LOADING);

        LoadResult r = load.loadOrFuture(true, false);
        assertThat(r).isInstanceOf(LoadResult.Pending.class);
        var future = ((LoadResult.Pending) r).future();
        assertThat(future.isDone()).isFalse();

        hold.countDown();
        first.get(2, TimeUnit.SECONDS);
        future.get(2, TimeUnit.SECONDS);
        assertThat(load.state()).isEqualTo(CoalescedLoad.State.LOADED);
        assertThat(load.loadDataCount.get()).isEqualTo(1);
      }
    }
  }

  @Test
  @DisplayName("close() / cancel() transitions to CANCELLED and wakes waiters")
  void closeWakesWaiters() throws Exception {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults());
        ExecutorService pool = Executors.newCachedThreadPool()) {
      RawFileCacheKey k = new RawFileCacheKey(12L, 0L);
      CountDownLatch hold = new CountDownLatch(1);
      TestLoad load = new TestLoad(cache, List.of(k), List.of(4096), hold);
      var producer = pool.submit(() -> load.loadOrFuture(false, false));
      long deadline = System.nanoTime() + 2_000_000_000L;
      while (load.state() != CoalescedLoad.State.LOADING && System.nanoTime() < deadline) {
        Thread.yield();
      }
      LoadResult r = load.loadOrFuture(true, false);
      assertThat(r).isInstanceOf(LoadResult.Pending.class);
      load.close();
      ((LoadResult.Pending) r).future().get(2, TimeUnit.SECONDS);
      hold.countDown();
      // Wait for the producer to finish so cache.close() doesn't race with findOrCreate.
      producer.get(2, TimeUnit.SECONDS);
      assertThat(load.loadOrFuture(false, false)).isInstanceOf(LoadResult.Done.class);
    }
  }

  @Test
  @DisplayName("loadData throwing transitions to CANCELLED and rethrows")
  void loadDataThrowsCancels() {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      var failingLoad =
          new CoalescedLoad(List.of(new RawFileCacheKey(13L, 0L)), List.of(4096)) {
            @Override
            public long size() {
              return 4096;
            }

            @Override
            public boolean isSsdLoad() {
              return false;
            }

            @Override
            protected List<CachePin> loadData(boolean prefetch) {
              throw new RuntimeException("simulated load failure");
            }
          };
      assertThatThrownBy(() -> failingLoad.loadOrFuture(false, false))
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("simulated load failure");
      assertThat(failingLoad.state()).isEqualTo(CoalescedLoad.State.CANCELLED);
    }
  }

  @Test
  @DisplayName("constructor validates keys.size() == sizes.size()")
  void constructorValidates() {
    assertThatThrownBy(
            () ->
                new CoalescedLoad(List.of(new RawFileCacheKey(1, 0)), List.of()) {
                  @Override
                  public long size() {
                    return 0;
                  }

                  @Override
                  public boolean isSsdLoad() {
                    return false;
                  }

                  @Override
                  protected List<CachePin> loadData(boolean prefetch) {
                    return List.of();
                  }
                })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("cancel() on PLANNED transitions to CANCELLED without running loadData")
  void cancelPlanned() {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      TestLoad load = new TestLoad(cache, List.of(new RawFileCacheKey(14, 0)), List.of(4096), null);
      load.cancel();
      assertThat(load.state()).isEqualTo(CoalescedLoad.State.CANCELLED);
      assertThat(load.loadDataCount.get()).isZero();
    }
  }
}
