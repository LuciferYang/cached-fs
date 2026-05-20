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
package io.github.luciferyang.cachedfs.core.handle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CachedFactoryTest {

  @Test
  void singleFlightOnlyOneGenerationPerKey() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    CountDownLatch generatorEntry = new CountDownLatch(1);
    CountDownLatch generatorRelease = new CountDownLatch(1);

    CachedFactory<String, String> factory =
        new CachedFactory<>(
            16,
            key -> {
              calls.incrementAndGet();
              generatorEntry.countDown();
              try {
                generatorRelease.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return key + "!";
            });

    try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
      // First thread starts and parks inside the generator.
      var first = pool.submit(() -> factory.generate("a"));
      generatorEntry.await(2, TimeUnit.SECONDS);

      // 7 more threads pile up; all must wait for the same generator call.
      var followers =
          new java.util.ArrayList<
              java.util.concurrent.Future<CachedFactory.CachedPtr<String, String>>>();
      for (int i = 0; i < 7; i++) {
        followers.add(pool.submit(() -> factory.generate("a")));
      }
      Thread.sleep(50);
      generatorRelease.countDown();
      try (var p = first.get(2, TimeUnit.SECONDS)) {
        assertThat(p.value()).isEqualTo("a!");
      }
      // Drain followers and close their pins so the entry can be evicted normally.
      for (var f : followers) {
        try (var p = f.get(2, TimeUnit.SECONDS)) {
          assertThat(p.value()).isEqualTo("a!");
        }
      }
      pool.shutdown();
      assertThat(pool.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void evictsUnpinnedEntriesWhenOverCapacity() {
    CachedFactory<Integer, Integer> f = new CachedFactory<>(2, k -> k * 10);
    // Fill and immediately release so entries are evictable.
    for (int i = 0; i < 5; i++) {
      f.generate(i).close();
    }
    assertThat(f.size()).isLessThanOrEqualTo(2);
  }

  @Test
  void generatorThrowingClearsPendingAndAllowsRetry() {
    AtomicInteger calls = new AtomicInteger();
    CachedFactory<String, String> f =
        new CachedFactory<>(
            8,
            key -> {
              if (calls.incrementAndGet() == 1) {
                throw new RuntimeException("simulated open failure");
              }
              return key + "!";
            });
    try {
      f.generate("k");
      throw new AssertionError("expected exception");
    } catch (RuntimeException expected) {
      assertThat(expected).hasMessageContaining("simulated");
    }
    // pending must be cleared — second call succeeds
    try (var p = f.generate("k")) {
      assertThat(p.value()).isEqualTo("k!");
    }
    assertThat(calls.get()).isEqualTo(2);
  }
}
