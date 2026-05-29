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

import io.github.luciferyang.cachedfs.core.AsyncDataCache;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmark for the prefetch admission gate's accounting cost — the price every prefetch
 * decision pays regardless of the multiplier. The gate (in {@code
 * CachingInputStream.admissionGate}) reads {@code AsyncDataCache.pendingPrefetchBytes()} (a {@link
 * java.util.concurrent.atomic.LongAdder} {@code sum()}) and compares it to the budget; admitted
 * prefetches {@code incrementPendingPrefetch} and later {@code decrementPendingPrefetch}.
 *
 * <p>This benchmark drives that inc / sum / dec triple from many threads to confirm the gate stays
 * cheap under concurrency — i.e. that raising the multiplier (allowing more in-flight prefetches,
 * hence more concurrent gate traffic) does not turn the counter into a contention bottleneck. The
 * {@code LongAdder} {@code sum()} cost grows with the number of contending cells, so this is the
 * relevant scaling dimension; sweep it with JMH's {@code -t} thread count.
 *
 * <p>Run:
 *
 * <pre>{@code
 * java -jar cached-fs-bench/target/benchmarks.jar AdmissionGateBenchmark -t 1
 * java -jar cached-fs-bench/target/benchmarks.jar AdmissionGateBenchmark -t 8
 * java -jar cached-fs-bench/target/benchmarks.jar AdmissionGateBenchmark -t 32
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Threads(8)
@Measurement(iterations = 5, time = 3)
public class AdmissionGateBenchmark {

  private static final long CHUNK = 1L << 20; // 1 MiB, matches the default load quantum
  private static final long BUDGET = 64L << 20; // generous budget so the gate mostly admits

  private AsyncDataCache cache;

  @Setup(Level.Trial)
  public void setUp() {
    this.cache = new AsyncDataCache(AsyncDataCache.Options.defaults());
  }

  /**
   * One admission decision plus the matching accounting: read the pending sum (the gate check), and
   * if it would fit the budget, increment then decrement — the full lifecycle a real prefetch
   * drives through the counter. Returns the gate's boolean so JMH can't fold the read away.
   */
  @Benchmark
  public boolean admitAndAccount(Blackhole bh) {
    long pending = cache.pendingPrefetchBytes();
    boolean admit = pending + CHUNK <= BUDGET;
    if (admit) {
      cache.incrementPendingPrefetch(CHUNK);
      bh.consume(cache.pendingPrefetchBytes());
      cache.decrementPendingPrefetch(CHUNK);
    }
    return admit;
  }
}
