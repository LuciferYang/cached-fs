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
package io.github.luciferyang.cachedfs.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.luciferyang.cachedfs.hadoop.CacheBootstrap;
import io.github.luciferyang.cachedfs.hadoop.CachedFileSystem;
import io.github.luciferyang.cachedfs.hadoop.CachedFsConfig;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Guards that the benchmark harness still wires up and serves correct bytes. JMH benchmarks never
 * run in CI (too slow), so without this test a refactor that broke {@link
 * InMemoryLatencyFileSystem} or the bootstrap wiring in {@link PrefetchMultiplierBenchmark} would
 * go unnoticed until someone ran the jar by hand.
 */
class BenchHarnessSmokeTest {

  @AfterEach
  void teardown() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  @Test
  @DisplayName("synthetic FS serves the position-derived byte pattern and exact length")
  void syntheticStoreServesBytes() throws IOException {
    InMemoryLatencyFileSystem.fileSize = 4096;
    InMemoryLatencyFileSystem.readLatencyNanos = 0L;
    InMemoryLatencyFileSystem fs = new InMemoryLatencyFileSystem();
    fs.initialize(URI.create("file:///"), new Configuration(false));

    Path p = new Path("file:///bench/x.bin");
    assertThat(fs.getFileStatus(p).getLen()).isEqualTo(4096);
    try (FSDataInputStream in = fs.open(p, 4096)) {
      byte[] buf = new byte[1000];
      in.readFully(500, buf, 0, 1000);
      // Byte at absolute offset o is (o % 251). Spot-check the formula across the slice.
      assertThat(buf[0]).isEqualTo((byte) (500 % 251));
      assertThat(buf[999]).isEqualTo((byte) (1499 % 251));
    }
  }

  @Test
  @DisplayName("a cold scan through CachedFileSystem + prefetch returns the full file length")
  void coldScanThroughCache() throws IOException {
    int quantum = 1 << 16; // 64 KiB
    long fileSize = 1L << 20; // 1 MiB → 16 chunks
    InMemoryLatencyFileSystem.fileSize = fileSize;
    InMemoryLatencyFileSystem.readLatencyNanos = 0L;

    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, InMemoryLatencyFileSystem.class.getName());
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, quantum);
    conf.setBoolean(CachedFsConfig.PREFETCH_ENABLED, true);
    conf.setInt(CachedFsConfig.PREFETCH_THREADS, 4);
    conf.setInt(CachedFsConfig.PREFETCH_MAX_PENDING_MULTIPLIER, 4);
    conf.setInt(CachedFsConfig.PREFETCH_DENSITY_THRESHOLD_PCT, 0);
    conf.setBoolean(CachedFsConfig.JMX_ENABLED, false);

    try (CachedFileSystem cfs = new CachedFileSystem()) {
      cfs.initialize(URI.create("file:///"), conf);
      Path p = new Path("file:///bench/scan.bin");
      long total = 0;
      byte[] buf = new byte[quantum];
      try (FSDataInputStream in = cfs.open(p, quantum)) {
        int chunks = (int) (fileSize / quantum);
        for (int i = 0; i < chunks; i++) {
          in.readFully((long) i * quantum, buf, 0, quantum);
          total += quantum;
        }
      }
      assertThat(total).isEqualTo(fileSize);
    }
  }
}
