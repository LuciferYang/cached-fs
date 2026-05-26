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
package io.github.luciferyang.cachedfs.hadoop;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.luciferyang.cachedfs.core.AsyncDataCache;
import io.github.luciferyang.cachedfs.core.RawFileCacheKey;
import io.github.luciferyang.cachedfs.core.stats.IoStatistics;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Targeted tests for {@link PrefetchTask} and {@link DiscardAndCountHandler} surface behavior. The
 * actual run-path is exercised end-to-end once Phase 5c wires the admission gate; these tests cover
 * the rejection path + future-completion accessors so the sibling-class invariants are locked in
 * independently.
 */
class PrefetchTaskTest {

  @Test
  @DisplayName(
      "DiscardAndCountHandler: rejects non-PrefetchTask runnables with RejectedExecutionException")
  void handlerRejectsNonPrefetchTask() {
    DiscardAndCountHandler handler = new DiscardAndCountHandler();
    Runnable plain = () -> {};
    // executor argument is unused by our handler — we pass null to keep the test hermetic.
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> handler.rejectedExecution(plain, (ThreadPoolExecutor) null))
        .isInstanceOf(RejectedExecutionException.class)
        .hasMessageContaining("non-PrefetchTask");
  }

  @Test
  @DisplayName("PrefetchExecutorFactory: validates threads + queueSize")
  void executorFactoryValidates() {
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> PrefetchExecutorFactory.create(0, 10))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> PrefetchExecutorFactory.create(2, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("PrefetchExecutorFactory: produced threads are daemon + named cached-fs-prefetch-N")
  void executorThreadsAreDaemon() throws InterruptedException {
    ThreadPoolExecutor exec = PrefetchExecutorFactory.create(1, 1);
    try {
      java.util.concurrent.atomic.AtomicReference<Thread> seen =
          new java.util.concurrent.atomic.AtomicReference<>();
      java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
      exec.execute(
          () -> {
            seen.set(Thread.currentThread());
            done.countDown();
          });
      assertThat(done.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      Thread t = seen.get();
      assertThat(t.isDaemon()).isTrue();
      assertThat(t.getName()).startsWith("cached-fs-prefetch-");
    } finally {
      exec.shutdown();
    }
  }

  @Test
  @DisplayName("PrefetchTask accessor surface returns the constructor-captured fields")
  void prefetchTaskAccessors() {
    // We construct a PrefetchTask but do NOT call run() — that path exercises live cache
    // entries and is covered end-to-end once the admission gate ships. Accessor-only test
    // locks down the public surface used by DiscardAndCountHandler.
    IoStatistics stats = new IoStatistics();
    CompletableFuture<RawFileCacheKey> fut = new CompletableFuture<>();
    RawFileCacheKey key = new RawFileCacheKey(42L, 1024L);
    AsyncDataCache cache = newCache();
    try {
      PrefetchTask task =
          new PrefetchTask(
              /* owner= */ null, // run() not invoked; null is acceptable for accessor verification
              stats,
              cache,
              /* readFile= */ null,
              /* chunkSize= */ 4096,
              key,
              /* nextOffset= */ 1024L,
              fut);
      assertThat(task.ioStats()).isSameAs(stats);
      assertThat(task.chunkSize()).isEqualTo(4096);
      assertThat(task.future()).isSameAs(fut);
    } finally {
      cache.close();
    }
  }

  private static AsyncDataCache newCache() {
    return new AsyncDataCache(AsyncDataCache.Options.defaults());
  }

  // Suppress unused-import-style warnings via a tiny use.
  @SuppressWarnings("unused")
  private static IOException unused() {
    return null;
  }
}
