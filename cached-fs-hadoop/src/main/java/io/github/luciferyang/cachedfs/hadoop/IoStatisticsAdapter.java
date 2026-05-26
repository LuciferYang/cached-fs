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
 * surface. Returned by {@link CachingInputStream#getIOStatistics()} so {@code
 * FSDataInputStream.getIOStatistics()} cascades into per-stream counters automatically.
 *
 * <p>Each gauge is a live function that reads the underlying {@link IoStatistics} on every {@code
 * gauges().get(name)} call — no snapshot, no staleness. {@code counters()} mirrors the same data
 * under the well-known {@link StreamStatisticNames} keys so existing Hadoop telemetry pipelines
 * (Prometheus exporters, etc.) pick them up without special-casing.
 *
 * <p>Counter-name table (verified against {@link StreamStatisticNames}; cached-fs-specific keys use
 * the {@code cachedfs_} prefix when no Hadoop standard exists):
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
 * </ul>
 */
final class IoStatisticsAdapter {

  private IoStatisticsAdapter() {}

  static IOStatistics forStream(IoStatistics src) {
    // Only two Hadoop StreamStatisticNames constants in 3.4.1 match cached-fs counters exactly
    // (STREAM_READ_OPERATIONS, STREAM_READ_BYTES). Everything else uses the cachedfs_ prefix
    // because there is no Hadoop-wide standard for the corresponding metric. We DO NOT invent
    // a constant that looks like a Hadoop standard (e.g. "stream_read_cache_hit") — that would
    // confuse downstream telemetry pipelines that lookup constants from StreamStatisticNames.
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
        .build();
  }
}
