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

import io.github.luciferyang.cachedfs.core.stats.IoStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;

/**
 * Bridges cached-fs's {@link IoStatistics} into Hadoop's {@code org.apache.hadoop.fs.statistics}
 * surface. Two factory methods:
 *
 * <ul>
 *   <li>{@link #forStream(IoStatistics)} — per-stream counters, returned by {@link
 *       CachingInputStream#getIOStatistics()} so {@code FSDataInputStream.getIOStatistics()}
 *       cascades into per-stream metrics automatically.
 *   <li>{@link #forBootstrap(CacheBootstrap)} — bootstrap-level counters + gauges aggregated across
 *       all streams.
 * </ul>
 *
 * <p>Each function is a live read (no snapshot, no staleness) — Hadoop's {@code
 * dynamicIOStatistics} resolves the {@code ToLongFunction} on every consumer access.
 *
 * <p>Counter-name table (verified against {@link StreamStatisticNames}; cached-fs-specific keys use
 * the {@code cachedfs_} prefix when no Hadoop standard exists):
 *
 * <p><b>Per-stream</b>:
 *
 * <ul>
 *   <li>{@code stream_read_operations} (Hadoop) ← {@code read()} count
 *   <li>{@code stream_read_bytes} (Hadoop) ← {@code readBytes()}
 *   <li>{@code cachedfs_stream_cache_hit} ← {@code ramHit()} count
 *   <li>{@code cachedfs_stream_cache_hit_bytes} ← {@code ramHitBytes()}
 *   <li>{@code stream_read_prefetch_operations} (Hadoop) ← {@code prefetch()} count
 *   <li>{@code cachedfs_stream_prefetched_bytes} ← {@code prefetchBytes()}
 *   <li>{@code cachedfs_stream_ssd_read_operations} ← {@code ssdRead()} count
 *   <li>{@code cachedfs_stream_ssd_read_bytes} ← {@code ssdReadBytes()}
 *   <li>{@code cachedfs_stream_raw_overread_bytes} ← {@code rawOverreadBytes()}
 *   <li>{@code cachedfs_stream_prefetch_skipped_queue_full_bytes} ← Phase 5c
 *   <li>{@code cachedfs_stream_prefetch_skipped_budget_bytes} ← Phase 5c
 *   <li>{@code cachedfs_stream_prefetch_skipped_heap_pressure_bytes} ← Phase 5c
 *   <li>{@code cachedfs_stream_prefetch_skipped_other_bytes} ← Phase 5c
 *   <li>{@code cachedfs_stream_prefetch_evicted_before_use_bytes} ← Phase 5c
 *   <li>{@code cachedfs_stream_prefetch_eligible_suppressed_bytes} ← Phase 5c
 *   <li>{@code cachedfs_stream_seq_hwm_regime_resets} ← Phase 5c
 * </ul>
 *
 * <p><b>Bootstrap-level</b>:
 *
 * <ul>
 *   <li>{@code cachedfs_aggregate_read_bytes}, {@code cachedfs_aggregate_cache_hit_bytes}, {@code
 *       cachedfs_aggregate_prefetched_bytes}, etc. — sum of per-stream counters from {@code
 *       bootstrap.aggregateIoStats()} (populated on stream close).
 *   <li>{@code cachedfs_stale_scan_id_recoveries} (Phase 5a) ← bootstrap-only counter.
 *   <li>{@code cachedfs_scan_tracker_count} ← gauge, current map size.
 *   <li>{@code cachedfs_scan_tracker_entries} ← gauge, sum of size() across all trackers.
 *   <li>{@code cachedfs_scan_tracker_max_entries} ← gauge, max of size() across all trackers.
 *   <li>{@code cachedfs_pending_prefetch_bytes} ← gauge, live in-flight prefetch byte budget.
 * </ul>
 */
final class IoStatisticsAdapter {

  private IoStatisticsAdapter() {}

  static IOStatistics forStream(IoStatistics src) {
    // Only two Hadoop StreamStatisticNames constants in 3.4.1 match cached-fs counters exactly
    // (STREAM_READ_OPERATIONS, STREAM_READ_BYTES, STREAM_READ_PREFETCH_OPERATIONS). Everything
    // else uses the cachedfs_ prefix because there is no Hadoop-wide standard for the
    // corresponding metric. We DO NOT invent a constant that looks like a Hadoop standard (e.g.
    // "stream_read_cache_hit") — that would confuse downstream telemetry pipelines that lookup
    // constants from StreamStatisticNames.
    return IOStatisticsBinding.dynamicIOStatistics()
        .withLongFunctionCounter(StreamStatisticNames.STREAM_READ_OPERATIONS, k -> src.read())
        .withLongFunctionCounter(StreamStatisticNames.STREAM_READ_BYTES, k -> src.readBytes())
        .withLongFunctionCounter("cachedfs_stream_cache_hit", k -> src.ramHit())
        .withLongFunctionCounter("cachedfs_stream_cache_hit_bytes", k -> src.ramHitBytes())
        .withLongFunctionCounter(
            StreamStatisticNames.STREAM_READ_PREFETCH_OPERATIONS, k -> src.prefetch())
        .withLongFunctionCounter("cachedfs_stream_prefetched_bytes", k -> src.prefetchBytes())
        .withLongFunctionCounter("cachedfs_stream_ssd_read_operations", k -> src.ssdRead())
        .withLongFunctionCounter("cachedfs_stream_ssd_read_bytes", k -> src.ssdReadBytes())
        .withLongFunctionCounter("cachedfs_stream_raw_overread_bytes", k -> src.rawOverreadBytes())
        // Phase 5c per-stream counters.
        .withLongFunctionCounter(
            "cachedfs_stream_prefetch_skipped_queue_full_bytes",
            k -> src.prefetchSkipped("queue_full"))
        .withLongFunctionCounter(
            "cachedfs_stream_prefetch_skipped_budget_bytes", k -> src.prefetchSkipped("budget"))
        .withLongFunctionCounter(
            "cachedfs_stream_prefetch_skipped_heap_pressure_bytes",
            k -> src.prefetchSkipped("heap_pressure"))
        .withLongFunctionCounter(
            "cachedfs_stream_prefetch_skipped_other_bytes", k -> src.prefetchSkipped("other"))
        .withLongFunctionCounter(
            "cachedfs_stream_prefetch_evicted_before_use_bytes",
            k -> src.prefetchEvictedBeforeUse())
        .withLongFunctionCounter(
            "cachedfs_stream_prefetch_eligible_suppressed_bytes",
            k -> src.prefetchEligibleSuppressedBytes())
        .withLongFunctionCounter(
            "cachedfs_stream_seq_hwm_regime_resets", k -> src.seqHwmRegimeResets())
        .build();
  }

  /**
   * Bootstrap-level live view aggregating the merged-from-stream {@link
   * io.github.luciferyang.cachedfs.core.stats.AggregatedIoStatistics} totals AND a handful of
   * registry-snapshot gauges ({@code scanTrackerEntries}, {@code scanTrackerMaxEntries}, {@code
   * pendingPrefetchBytes}).
   */
  static IOStatistics forBootstrap(CacheBootstrap bootstrap) {
    io.github.luciferyang.cachedfs.core.stats.AggregatedIoStatistics agg =
        bootstrap.aggregateIoStats();
    return IOStatisticsBinding.dynamicIOStatistics()
        // Aggregated per-stream byte/event totals.
        .withLongFunctionCounter("cachedfs_aggregate_read", k -> agg.read())
        .withLongFunctionCounter("cachedfs_aggregate_read_bytes", k -> agg.readBytes())
        .withLongFunctionCounter("cachedfs_aggregate_cache_hit", k -> agg.ramHit())
        .withLongFunctionCounter("cachedfs_aggregate_cache_hit_bytes", k -> agg.ramHitBytes())
        .withLongFunctionCounter("cachedfs_aggregate_prefetch", k -> agg.prefetch())
        .withLongFunctionCounter("cachedfs_aggregate_prefetched_bytes", k -> agg.prefetchBytes())
        .withLongFunctionCounter("cachedfs_aggregate_ssd_read", k -> agg.ssdRead())
        .withLongFunctionCounter("cachedfs_aggregate_ssd_read_bytes", k -> agg.ssdReadBytes())
        .withLongFunctionCounter(
            "cachedfs_aggregate_raw_overread_bytes", k -> agg.rawOverreadBytes())
        .withLongFunctionCounter(
            "cachedfs_aggregate_prefetch_skipped_queue_full_bytes",
            k -> agg.prefetchSkipped("queue_full"))
        .withLongFunctionCounter(
            "cachedfs_aggregate_prefetch_skipped_budget_bytes", k -> agg.prefetchSkipped("budget"))
        .withLongFunctionCounter(
            "cachedfs_aggregate_prefetch_skipped_heap_pressure_bytes",
            k -> agg.prefetchSkipped("heap_pressure"))
        .withLongFunctionCounter(
            "cachedfs_aggregate_prefetch_skipped_other_bytes", k -> agg.prefetchSkipped("other"))
        .withLongFunctionCounter(
            "cachedfs_aggregate_prefetch_evicted_before_use_bytes",
            k -> agg.prefetchEvictedBeforeUse())
        .withLongFunctionCounter(
            "cachedfs_aggregate_prefetch_eligible_suppressed_bytes",
            k -> agg.prefetchEligibleSuppressedBytes())
        .withLongFunctionCounter(
            "cachedfs_aggregate_seq_hwm_regime_resets", k -> agg.seqHwmRegimeResets())
        // Bootstrap-only counter.
        .withLongFunctionCounter(
            "cachedfs_stale_scan_id_recoveries", k -> agg.staleScanIdRecoveries())
        // Registry-snapshot gauges (live reads from the registry; per-call iteration cost is
        // O(scanTrackers.size) — bounded by the open-follow-up LRU cap, typically <10k).
        .withLongFunctionGauge("cachedfs_scan_tracker_count", k -> bootstrap.scanTrackerCount())
        .withLongFunctionGauge("cachedfs_scan_tracker_entries", k -> bootstrap.scanTrackerEntries())
        .withLongFunctionGauge(
            "cachedfs_scan_tracker_max_entries", k -> bootstrap.scanTrackerMaxEntries())
        .withLongFunctionGauge(
            "cachedfs_pending_prefetch_bytes", k -> bootstrap.ramCache().pendingPrefetchBytes())
        .build();
  }
}
