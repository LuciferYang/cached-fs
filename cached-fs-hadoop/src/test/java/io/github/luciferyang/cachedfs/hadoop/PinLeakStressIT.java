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

import io.github.luciferyang.cachedfs.core.stats.CacheStats;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 5b pin-leak stress test (Failsafe IT, ~30 s wall-clock budget). Many threads issue random
 * positional reads against a shared file with coalesce + prefetch both enabled. Forces:
 *
 * <ul>
 *   <li>Multi-pin acquisition + abort-and-restart (Phase 5b) when a Waiting result fires
 *       mid-classification.
 *   <li>PrefetchTask + consumer races on the same chunk (Hit / Waiting / Exclusive triangle).
 *   <li>Stream close while prefetch is in-flight.
 * </ul>
 *
 * <p>Final assertions:
 *
 * <ul>
 *   <li>{@code numShared == 0} AND {@code numExclusive == 0} after all streams close — no pin
 *       reached past stream close.
 *   <li>{@code pendingPrefetchBytes() == 0} — every prefetch's byte-budget decrement landed.
 * </ul>
 */
class PinLeakStressIT {

  @AfterEach
  void teardown() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  @Test
  @DisplayName("100 threads × 200 random reads: no pin leak, no byte-budget leak")
  void noPinLeakUnderRandomLoad(@TempDir java.nio.file.Path dir) throws Exception {
    int chunkSize = 4096;
    int chunks = 32; // 128 KiB file — small enough that random reads collide on chunks often
    int totalBytes = chunks * chunkSize;
    int threads = 100;
    int readsPerThread = 200;
    int maxReadLen = 3 * chunkSize; // spans 1-4 chunks per read → forces coalesce path

    java.nio.file.Path file = dir.resolve("stress.bin");
    Files.write(file, bytes(totalBytes));

    Configuration conf = stressConf(chunkSize);
    try (CachedFileSystem cfs = newCfs(file.toUri(), conf)) {
      Path p = new Path(file.toUri());
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      AtomicLong errors = new AtomicLong();
      try {
        for (int t = 0; t < threads; t++) {
          final long seed = 1234567L + t;
          pool.submit(
              () -> {
                Random rnd = new Random(seed);
                byte[] buf = new byte[maxReadLen];
                try {
                  start.await();
                  // Each thread opens its OWN FSDataInputStream so close paths exercise per-stream
                  // cleanup. Sharing one stream across threads would violate the Hadoop
                  // non-thread-safety contract on read/seek.
                  try (FSDataInputStream in = cfs.open(p, 4096)) {
                    for (int i = 0; i < readsPerThread; i++) {
                      int len = 1 + rnd.nextInt(maxReadLen);
                      long pos = rnd.nextInt(Math.max(1, totalBytes - len));
                      in.readFully(pos, buf, 0, len);
                    }
                  }
                } catch (Throwable ex) {
                  errors.incrementAndGet();
                  ex.printStackTrace();
                } finally {
                  done.countDown();
                }
              });
        }
        start.countDown();
        assertThat(done.await(60, TimeUnit.SECONDS))
            .as("stress run completed within 60 s")
            .isTrue();
      } finally {
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      }

      assertThat(errors.get()).as("no thread observed an IO error").isZero();

      // Drain the prefetch executor before asserting pin/byte-budget state — there may be
      // in-flight tasks finishing their run-finally (decrement + clearPendingPrefetchIf).
      java.util.concurrent.ThreadPoolExecutor exec =
          CacheBootstrap.get().orElseThrow().prefetchExecutor();
      exec.shutdown();
      assertThat(exec.awaitTermination(10, TimeUnit.SECONDS))
          .as("prefetch executor drained")
          .isTrue();

      CacheStats stats = CacheBootstrap.get().orElseThrow().ramCache().refreshStats();
      assertThat(stats.numExclusive())
          .as("no exclusive pin left dangling after all streams closed")
          .isZero();
      assertThat(stats.numShared())
          .as("no shared pin left dangling after all streams closed")
          .isZero();
      assertThat(CacheBootstrap.get().orElseThrow().ramCache().pendingPrefetchBytes())
          .as("byte-budget counter settles to 0 after every task ran its decrement")
          .isZero();
    }
  }

  // --- helpers ---

  private static CachedFileSystem newCfs(URI fileUri, Configuration conf) throws IOException {
    CachedFileSystem cfs = new CachedFileSystem();
    cfs.initialize(URI.create(fileUri.getScheme() + ":///"), conf);
    return cfs;
  }

  private static Configuration stressConf(int loadQuantumBytes) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, LocalFileSystem.class.getName());
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, loadQuantumBytes);
    // Force-on coalesce to exercise the Phase 5b path (small cache would otherwise auto-disable).
    conf.setBoolean(CachedFsConfig.COALESCE_ALWAYS_ON, true);
    // Phase 5c prefetch on — explicit so a future default flip doesn't silently change coverage.
    conf.setBoolean(CachedFsConfig.PREFETCH_ENABLED, true);
    return conf;
  }

  private static byte[] bytes(int n) {
    byte[] out = new byte[n];
    for (int i = 0; i < n; i++) {
      out[i] = (byte) ((i * 31 + 7) & 0xff);
    }
    return out;
  }
}
