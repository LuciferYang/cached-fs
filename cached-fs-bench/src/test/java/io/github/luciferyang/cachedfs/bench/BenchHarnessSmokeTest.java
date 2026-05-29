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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Guards that the benchmark harness still wires up and serves correct bytes. JMH benchmarks never
 * run in CI (too slow), so without this test a refactor that broke {@link
 * InMemoryLatencyFileSystem} or the bootstrap wiring in {@link PrefetchMultiplierBenchmark} would
 * go unnoticed until someone ran the jar by hand.
 */
class BenchHarnessSmokeTest {

  @BeforeEach
  void resetStatics() {
    // The synthetic FS config lives in static fields (Hadoop builds the FS reflectively). Reset all
    // of it before each test so one test's latency/stride/park-count can't bleed into the next.
    InMemoryLatencyFileSystem.fileSize = 0L;
    InMemoryLatencyFileSystem.readLatencyNanos = 0L;
    InMemoryLatencyFileSystem.latencyStrideBytes = 0L;
    InMemoryLatencyFileSystem.parkCount.set(0);
  }

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
  @DisplayName("a cold scan through CachedFileSystem + prefetch serves byte-correct data")
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
      byte[] buf = new byte[quantum];
      try (FSDataInputStream in = cfs.open(p, quantum)) {
        int chunks = (int) (fileSize / quantum);
        for (int i = 0; i < chunks; i++) {
          long offset = (long) i * quantum;
          in.readFully(offset, buf, 0, quantum);
          // Verify the cache served the position-derived pattern (byte at absolute offset o is
          // o % 251), not zeros or a wrong chunk. Spot-check both ends of every chunk so an
          // off-by-one in the chunk math or a corrupt/duplicated fill is caught.
          assertThat(buf[0]).as("chunk %d first byte", i).isEqualTo((byte) (offset % 251));
          assertThat(buf[quantum - 1])
              .as("chunk %d last byte", i)
              .isEqualTo((byte) ((offset + quantum - 1) % 251));
        }
      }
    }
  }

  @Test
  @DisplayName("stride-aware latency charges once per stride boundary, not per page read")
  void strideAwareLatencyChargesPerRegion() throws IOException {
    // The benchmark's validity rests on charging first-byte latency once per chunk, not once per
    // 4 KiB page. Assert the EXACT park count (deterministic) rather than wall-clock, which would
    // flake under GC/scheduling: a read AT a stride boundary parks once; a read off-boundary parks
    // zero times. Latency kept tiny (1 µs) — the assertion is on the count, not the duration.
    InMemoryLatencyFileSystem.fileSize = 1L << 20;
    InMemoryLatencyFileSystem.readLatencyNanos = 1_000L; // 1 µs — keeps the test fast
    InMemoryLatencyFileSystem.latencyStrideBytes = 1 << 16; // 64 KiB
    InMemoryLatencyFileSystem fs = new InMemoryLatencyFileSystem();
    fs.initialize(URI.create("file:///"), new Configuration(false));

    Path p = new Path("file:///bench/stride.bin");
    byte[] buf = new byte[16];
    try (FSDataInputStream in = fs.open(p, 4096)) {
      // Read AT a 64 KiB boundary (offsets 0 and 65536) → one park each.
      in.readFully(0L, buf, 0, 16);
      in.readFully(65536L, buf, 0, 16);
      assertThat(InMemoryLatencyFileSystem.parkCount.get())
          .as("stride-aligned reads each charge one park")
          .isEqualTo(2);

      // Read OFF a boundary (offset 16, 4096, 70000 — none a multiple of 64 KiB) → no extra parks.
      in.readFully(16L, buf, 0, 16);
      in.readFully(4096L, buf, 0, 16);
      in.readFully(70000L, buf, 0, 16);
      assertThat(InMemoryLatencyFileSystem.parkCount.get())
          .as("non-aligned reads charge no parks")
          .isEqualTo(2);
    }
  }

  @Test
  @DisplayName("latencyStride <= 0 falls back to charging every read")
  void zeroStrideChargesEveryRead() throws IOException {
    InMemoryLatencyFileSystem.fileSize = 1L << 20;
    InMemoryLatencyFileSystem.readLatencyNanos = 1_000L; // 1 µs
    InMemoryLatencyFileSystem.latencyStrideBytes =
        0L; // worst-case: every read is a cold round-trip
    InMemoryLatencyFileSystem fs = new InMemoryLatencyFileSystem();
    fs.initialize(URI.create("file:///"), new Configuration(false));

    Path p = new Path("file:///bench/everyread.bin");
    byte[] buf = new byte[16];
    try (FSDataInputStream in = fs.open(p, 4096)) {
      // With stride <= 0 EVERY read parks, including off-boundary ones.
      in.readFully(16L, buf, 0, 16); // off-boundary
      in.readFully(4096L, buf, 0, 16); // off-boundary
      assertThat(InMemoryLatencyFileSystem.parkCount.get())
          .as("stride <= 0 charges every read")
          .isEqualTo(2);
    }
  }
}
