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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * precondition: prefetch byte-budget counter (no callers yet, wired by ). Tests the WARN-not-throw
 * integrity guard semantics: legitimate concurrent inc/dec pairs never trip the guard; a bug path
 * (dec without paired inc) drives the counter negative but does NOT throw.
 */
class PendingPrefetchBytesTest {

  @Test
  @DisplayName("increment / decrement: sum reflects the running total")
  void incrementDecrement() {
    AsyncDataCache cache = newCache();
    try {
      assertThat(cache.pendingPrefetchBytes()).isZero();
      cache.incrementPendingPrefetch(4 * 1024L);
      assertThat(cache.pendingPrefetchBytes()).isEqualTo(4096L);
      cache.decrementPendingPrefetch(1024L);
      assertThat(cache.pendingPrefetchBytes()).isEqualTo(3072L);
    } finally {
      cache.close();
    }
  }

  @Test
  @DisplayName("16 threads × 10k paired increment/decrement: no exception, final sum is exactly 0")
  void contendedPairsConverge() throws InterruptedException {
    AsyncDataCache cache = newCache();
    try {
      int threads = 16;
      int iterations = 10_000;
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      try {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
          pool.submit(
              () -> {
                try {
                  start.await();
                  for (int i = 0; i < iterations; i++) {
                    cache.incrementPendingPrefetch(1024L);
                    cache.decrementPendingPrefetch(1024L);
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                } finally {
                  done.countDown();
                }
              });
        }
        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
      } finally {
        pool.shutdown();
      }
      assertThat(cache.pendingPrefetchBytes()).isZero();
    } finally {
      cache.close();
    }
  }

  @Test
  @DisplayName("guard does NOT throw on dec-without-inc; counter goes negative as designed")
  void guardWarnNotThrow() {
    AsyncDataCache cache = newCache();
    try {
      // Single-threaded, deliberately violate inc/dec pairing — exercises the WARN path.
      cache.decrementPendingPrefetch(100L);
      // Counter is intentionally negative — that's the live signal a bug path produced.
      assertThat(cache.pendingPrefetchBytes()).isEqualTo(-100L);
    } finally {
      cache.close();
    }
  }

  private static AsyncDataCache newCache() {
    AsyncDataCache.Options opts =
        new AsyncDataCache.Options(
            AsyncDataCache.DEFAULT_NUM_SHARDS,
            /* maxWriteRatio= */ 0.7,
            /* ssdSavableRatio= */ 0.125,
            /* minSsdSavableBytes= */ 16L << 20,
            /* ssdFlushThresholdBytes= */ 0L);
    return new AsyncDataCache(opts);
  }
}
