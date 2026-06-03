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

import io.github.luciferyang.cachedfs.hadoop.CacheBootstrap;
import io.github.luciferyang.cachedfs.hadoop.CachedFileSystem;
import io.github.luciferyang.cachedfs.hadoop.CachedFsConfig;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures how the prefetch admission multiplier ({@code
 * fs.cached.prefetch.max-pending-multiplier}) affects cold-scan wall-clock time across access
 * patterns and storage latencies. This is the benchmark behind reader-glue follow-up R1.1: "measure
 * the right multiplier on a representative workload."
 *
 * <p>The multiplier sets the in-flight prefetch byte budget to {@code loadQuantum × threads ×
 * multiplier}. A bigger budget lets more chunk loads stay outstanding, hiding more of the backing
 * store's per-read latency — until the prefetch thread pool, not the budget, caps concurrency. This
 * benchmark sweeps the multiplier against that ceiling so the default (4) can be confirmed or
 * revised against data instead of a guess.
 *
 * <p>Each invocation scans a UNIQUE synthetic path, so every read is a cold cache miss that drives
 * the prefetch path — re-scanning one path would serve RAM hits with no prefetch after the first
 * pass. The backing store ({@link InMemoryLatencyFileSystem}) injects a fixed per-read latency; see
 * its javadoc for why a zero-latency store would flatten every multiplier to the same number.
 *
 * <p>Run via the shaded jar:
 *
 * <pre>{@code
 * mvn -q -pl cached-fs-bench -am package
 * java -jar cached-fs-bench/target/benchmarks.jar PrefetchMultiplierBenchmark
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Measurement(iterations = 5, time = 3)
public class PrefetchMultiplierBenchmark {

  /** Access pattern the consumer walks the file with. */
  public enum Pattern {
    /** Read every chunk front to back — the ideal prefetch case. */
    SEQUENTIAL,
    /**
     * Read every other chunk (0, 2, 4, …). NOTE: the prefetcher only reads ONE chunk ahead ({@code
     * CachingInputStream} prefetches {@code nextChunkStart = thisChunkEnd}), so under this pattern
     * it loads the ODD chunks the consumer skips — every prefetch is wasted and every consumed
     * chunk is a cold, consumer-paid miss. This is therefore a "wasted-prefetch" stressor (does the
     * budget churn hurt when prefetch never helps?), NOT a look-ahead test.
     */
    STRIDED
  }

  @Param({"1", "2", "4", "8", "16"})
  public int multiplier;

  // 0 µs = cache/CPU-bound baseline; 200 µs ≈ warm NVMe; 2000 µs ≈ cloud object-store first byte.
  @Param({"0", "200", "2000"})
  public int latencyMicros;

  @Param({"SEQUENTIAL", "STRIDED"})
  public Pattern pattern;

  // Prefetch pool size. The multiplier's optimal value is thread-dependent (the budget caps
  // in-flight chunks, but real concurrency is capped by this pool), so sweeping it is the point of
  // running on representative hardware — set it to your deployment's prefetch threads. Single
  // default here so existing single-combo smoke runs are unaffected; override with
  // `-p prefetchThreads=8,16,32` for the real sweep.
  @Param({"8"})
  public int prefetchThreads;

  // Fixed knobs — chosen so a single scan is long enough for prefetch to matter yet short enough
  // for many JMH iterations. 64 MiB / 1 MiB quantum = 64 chunks.
  private static final int LOAD_QUANTUM = 1 << 20; // 1 MiB
  private static final long FILE_SIZE = 64L << 20; // 64 MiB
  private static final int READ_BUFFER = 1 << 20; // consumer reads one quantum at a time

  private CachedFileSystem cfs;
  private final AtomicLong pathCounter = new AtomicLong();
  private byte[] readBuf;

  @Setup(Level.Trial)
  public void setUp() throws IOException {
    // One bootstrap per fork; JMH gives each @Param combination its own fork, so the multiplier is
    // fixed for the bootstrap's lifetime (maxPendingPrefetchBytes is captured at install time).
    CacheBootstrap.uninstallForTesting();
    InMemoryLatencyFileSystem.fileSize = FILE_SIZE;
    InMemoryLatencyFileSystem.readLatencyNanos = TimeUnit.MICROSECONDS.toNanos(latencyMicros);
    // Charge first-byte latency once per chunk load, not once per 4 KiB page: the cache fills a
    // quantum as many page reads, but real storage pays the round-trip only on the leading read.
    InMemoryLatencyFileSystem.latencyStrideBytes = LOAD_QUANTUM;

    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, InMemoryLatencyFileSystem.class.getName());
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, LOAD_QUANTUM);
    conf.setBoolean(CachedFsConfig.PREFETCH_ENABLED, true);
    conf.setInt(CachedFsConfig.PREFETCH_THREADS, prefetchThreads);
    conf.setInt(CachedFsConfig.PREFETCH_MAX_PENDING_MULTIPLIER, multiplier);
    // Density gate off (threshold 0) so the multiplier — not the density heuristic — is what
    // governs admission in this benchmark.
    conf.setInt(CachedFsConfig.PREFETCH_DENSITY_THRESHOLD_PCT, 0);
    // JMX off: the bench JVM doesn't need an MBean and registration would warn on re-install.
    conf.setBoolean(CachedFsConfig.JMX_ENABLED, false);

    CachedFileSystem fs = new CachedFileSystem();
    fs.initialize(URI.create("file:///"), conf);
    this.cfs = fs;
    this.readBuf = new byte[READ_BUFFER];
  }

  @Setup(Level.Invocation)
  public void clearCacheBeforeEachScan() {
    // The RAM cache never auto-evicts (aging is TTL-controller-driven, never invoked here), every
    // scan() uses a UNIQUE path, and the chunk payload is OFF-HEAP direct memory (allocateDirect).
    // Without intervention each scan leaks ~64 MiB of direct memory for the fork's life; at low
    // latency a 2 s iteration is tens of scans (a couple GiB), enough to exhaust the default
    // MaxDirectMemorySize. clear() drops the refs; the JDK Cleaner reclaims the native memory on
    // direct-memory pressure, so clearing before EVERY scan (untimed; scans are tens-to-hundreds of
    // ms, fine for Level.Invocation) keeps the live footprint to roughly one scan and every scan a
    // genuine cold miss (a unique path is never resident).
    CacheBootstrap.get().ifPresent(b -> b.ramCache().clear());
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (cfs != null) {
      cfs.close();
    }
    CacheBootstrap.uninstallForTesting();
  }

  @Benchmark
  public long scan(Blackhole bh) throws IOException {
    // Unique path per invocation → cold cache miss → real prefetch work every time.
    Path p = new Path("file:///bench/scan-" + pathCounter.getAndIncrement() + ".bin");
    long total = 0;
    int chunks = (int) (FILE_SIZE / LOAD_QUANTUM);
    try (FSDataInputStream in = cfs.open(p, READ_BUFFER)) {
      if (pattern == Pattern.SEQUENTIAL) {
        for (int i = 0; i < chunks; i++) {
          in.readFully((long) i * LOAD_QUANTUM, readBuf, 0, LOAD_QUANTUM);
          total += readBuf[0];
        }
      } else {
        // Strided: even chunks only — half the bytes. The +1-chunk prefetcher loads the odd chunks
        // we skip, so prefetch is pure waste here (see Pattern.STRIDED); this stresses budget churn
        // when prefetch never helps, it is not a look-ahead test.
        for (int i = 0; i < chunks; i += 2) {
          in.readFully((long) i * LOAD_QUANTUM, readBuf, 0, LOAD_QUANTUM);
          total += readBuf[0];
        }
      }
    }
    bh.consume(total);
    return total;
  }
}
