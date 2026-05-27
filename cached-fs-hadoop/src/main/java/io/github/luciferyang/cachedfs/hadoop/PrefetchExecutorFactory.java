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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the shared prefetch executor owned by {@link CacheBootstrap}. Single fixed-size {@link
 * ThreadPoolExecutor} backed by an {@link ArrayBlockingQueue} of bounded size, daemon threads,
 * named {@code cached-fs-prefetch-N}, and {@link DiscardAndCountHandler} as the rejection policy
 * (discard-and-count semantics, NOT CallerRunsPolicy).
 *
 * <p>Threads are daemon so a JVM that omits {@code CacheBootstrap.uninstallForTesting()} on exit
 * does not block shutdown — at the cost that an OOMError on a prefetch worker silently kills the
 * thread (logged at WARN via the per-thread uncaught-exception handler) and the pool keeps running
 * with fewer workers until close.
 */
final class PrefetchExecutorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(PrefetchExecutorFactory.class);

  private PrefetchExecutorFactory() {}

  static ThreadPoolExecutor create(int threads, int queueSize) {
    if (threads <= 0) {
      throw new IllegalArgumentException("prefetch threads must be > 0: " + threads);
    }
    if (queueSize <= 0) {
      throw new IllegalArgumentException("prefetch queue size must be > 0: " + queueSize);
    }
    return new ThreadPoolExecutor(
        /* core= */ threads,
        /* max= */ threads,
        /* keepAlive= */ 0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(queueSize),
        new DaemonThreadFactory(),
        new DiscardAndCountHandler());
  }

  /** Daemon thread factory naming threads {@code cached-fs-prefetch-N}; WARN-logs uncaughts. */
  private static final class DaemonThreadFactory implements ThreadFactory {
    private final AtomicInteger id = new AtomicInteger();

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r, "cached-fs-prefetch-" + id.incrementAndGet());
      t.setDaemon(true);
      t.setUncaughtExceptionHandler(
          (thread, ex) ->
              LOG.warn("uncaught exception on prefetch thread {}: {}", thread.getName(), ex, ex));
      return t;
    }
  }
}
