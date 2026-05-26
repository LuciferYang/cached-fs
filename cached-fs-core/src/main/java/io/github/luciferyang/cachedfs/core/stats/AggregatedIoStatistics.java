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
package io.github.luciferyang.cachedfs.core.stats;

import java.util.concurrent.atomic.LongAdder;

/**
 * Bootstrap-level aggregate of per-stream {@link IoStatistics}. Two semantically-distinct counter
 * groups live here:
 *
 * <ol>
 *   <li><b>Merged-from-stream counters</b> — sum of per-stream values, populated by {@link
 *       #add(IoStatistics)}: {@code readBytes}, {@code ramHitBytes}, {@code prefetchBytes}, {@code
 *       ssdReadBytes}, {@code rawOverreadBytes}, and the corresponding count counters. Naming
 *       matches the per-stream field.
 *   <li><b>Bootstrap-only counters</b> — JVM-wide signals bumped directly by {@code CacheBootstrap}
 *       and never merged from per-stream sources. Currently: {@link #staleScanIdRecoveries()}.
 *       Future bootstrap-only counters MUST use a {@code bootstrap*} prefix per convention; {@code
 *       staleScanIdRecoveries} is grandfathered un-prefixed because the external metric name {@code
 *       cachedfs_stale_scan_id_recoveries} is already documented.
 * </ol>
 *
 * <p><b>{@link #add(IoStatistics)} contract:</b> the source MUST be quiescent during the call —
 * each public counter is read via a separate {@code AtomicLong.get()}, so a concurrent writer can
 * tear the snapshot across counters. The Phase 5a use-site (per-stream close) satisfies this:
 * Hadoop's {@code FSDataInputStream.close} contract is non-thread-safe vs. reads.
 *
 * <p>Per-counter storage is {@link LongAdder} so concurrent {@code add(...)} calls from different
 * streams scale; the bootstrap-only {@code incStaleScanIdRecoveries} bump is also on the same
 * {@link LongAdder}.
 */
public final class AggregatedIoStatistics {

  // Merged-from-stream counters
  private final LongAdder ramHit = new LongAdder();
  private final LongAdder ramHitBytes = new LongAdder();
  private final LongAdder read = new LongAdder();
  private final LongAdder readBytes = new LongAdder();
  private final LongAdder prefetch = new LongAdder();
  private final LongAdder prefetchBytes = new LongAdder();
  private final LongAdder ssdRead = new LongAdder();
  private final LongAdder ssdReadBytes = new LongAdder();
  private final LongAdder rawOverreadBytes = new LongAdder();
  // Phase 5c merged-from-stream byte/event counters.
  private final LongAdder prefetchSkippedQueueFull = new LongAdder();
  private final LongAdder prefetchSkippedBudget = new LongAdder();
  private final LongAdder prefetchSkippedHeapPressure = new LongAdder();
  private final LongAdder prefetchSkippedOther = new LongAdder();
  private final LongAdder prefetchEvictedBeforeUse = new LongAdder();
  private final LongAdder prefetchEligibleSuppressedBytes = new LongAdder();
  private final LongAdder seqHwmRegimeResets = new LongAdder();

  // Bootstrap-only counters (NOT touched by add(IoStatistics))
  private final LongAdder staleScanIdRecoveries = new LongAdder();

  /**
   * Merges a per-stream {@link IoStatistics} snapshot into this aggregate. The source must be
   * quiescent (no concurrent writers) — see class javadoc. The {@link IoStatistics#NO_OP} sentinel
   * snapshots as all zeros and is safe to {@code add}.
   */
  public void add(IoStatistics source) {
    ramHit.add(source.ramHit());
    ramHitBytes.add(source.ramHitBytes());
    read.add(source.read());
    readBytes.add(source.readBytes());
    prefetch.add(source.prefetch());
    prefetchBytes.add(source.prefetchBytes());
    ssdRead.add(source.ssdRead());
    ssdReadBytes.add(source.ssdReadBytes());
    rawOverreadBytes.add(source.rawOverreadBytes());
    // Phase 5c: merge the new byte/event counters per the class-javadoc partition. The
    // staleScanIdRecoveries bootstrap-only counter is intentionally NOT merged here.
    prefetchSkippedQueueFull.add(source.prefetchSkipped("queue_full"));
    prefetchSkippedBudget.add(source.prefetchSkipped("budget"));
    prefetchSkippedHeapPressure.add(source.prefetchSkipped("heap_pressure"));
    prefetchSkippedOther.add(source.prefetchSkipped("other"));
    prefetchEvictedBeforeUse.add(source.prefetchEvictedBeforeUse());
    prefetchEligibleSuppressedBytes.add(source.prefetchEligibleSuppressedBytes());
    seqHwmRegimeResets.add(source.seqHwmRegimeResets());
  }

  /**
   * Bootstrap-only bump. Called when {@code CacheBootstrap.withScanId} enters with a stale
   * ThreadLocal slot and auto-recovers — see Phase 5a wiring §2.
   */
  public void incStaleScanIdRecoveries() {
    staleScanIdRecoveries.increment();
  }

  public long ramHit() {
    return ramHit.sum();
  }

  public long ramHitBytes() {
    return ramHitBytes.sum();
  }

  public long read() {
    return read.sum();
  }

  public long readBytes() {
    return readBytes.sum();
  }

  public long prefetch() {
    return prefetch.sum();
  }

  public long prefetchBytes() {
    return prefetchBytes.sum();
  }

  public long ssdRead() {
    return ssdRead.sum();
  }

  public long ssdReadBytes() {
    return ssdReadBytes.sum();
  }

  public long rawOverreadBytes() {
    return rawOverreadBytes.sum();
  }

  public long staleScanIdRecoveries() {
    return staleScanIdRecoveries.sum();
  }

  public long prefetchSkipped(String reason) {
    return switch (reason) {
      case "queue_full" -> prefetchSkippedQueueFull.sum();
      case "budget" -> prefetchSkippedBudget.sum();
      case "heap_pressure" -> prefetchSkippedHeapPressure.sum();
      case "other" -> prefetchSkippedOther.sum();
      default -> 0L;
    };
  }

  public long prefetchEvictedBeforeUse() {
    return prefetchEvictedBeforeUse.sum();
  }

  public long prefetchEligibleSuppressedBytes() {
    return prefetchEligibleSuppressedBytes.sum();
  }

  public long seqHwmRegimeResets() {
    return seqHwmRegimeResets.sum();
  }
}
