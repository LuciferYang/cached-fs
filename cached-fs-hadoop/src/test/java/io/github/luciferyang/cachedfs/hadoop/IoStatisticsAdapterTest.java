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

import io.github.luciferyang.cachedfs.core.stats.IoStatistics;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the public counter-name contract of {@link IoStatisticsAdapter}. Downstream telemetry
 * pipelines (Prometheus exporters, dashboards) key off these names — a rename is a breaking change.
 * Treat each assertion as a "rename safety net": if you intentionally rename, update the test in
 * the SAME commit so the docs/dashboards drift is visible to reviewers.
 */
class IoStatisticsAdapterTest {

  @AfterEach
  void teardown() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  @Test
  @DisplayName("per-stream adapter exposes the documented counter names with live values")
  void streamCounterNames() {
    IoStatistics src = new IoStatistics();
    src.incRead(100);
    src.incRamHit(80);
    src.incPrefetch(50);
    src.incSsdRead(30);
    src.incRawOverreadBytes(10);
    src.incPrefetchSkipped("budget", 200);
    src.incPrefetchEvictedBeforeUse(7);
    src.incPrefetchEligibleSuppressed(15);
    src.incSeqHwmRegimeResets();

    IOStatistics out = IoStatisticsAdapter.forStream(src);
    java.util.Map<String, Long> c = out.counters();

    // Hadoop standard names.
    assertThat(c).containsEntry("stream_read_operations", 1L);
    assertThat(c).containsEntry("stream_read_bytes", 100L);
    assertThat(c).containsEntry("stream_read_prefetch_operations", 1L);
    // cached-fs-namespaced names.
    assertThat(c).containsEntry("cachedfs_stream_cache_hit", 1L);
    assertThat(c).containsEntry("cachedfs_stream_cache_hit_bytes", 80L);
    assertThat(c).containsEntry("cachedfs_stream_prefetched_bytes", 50L);
    assertThat(c).containsEntry("cachedfs_stream_ssd_read_operations", 1L);
    assertThat(c).containsEntry("cachedfs_stream_ssd_read_bytes", 30L);
    assertThat(c).containsEntry("cachedfs_stream_raw_overread_bytes", 10L);
    // Phase 5c.
    assertThat(c).containsEntry("cachedfs_stream_prefetch_skipped_queue_full_bytes", 0L);
    assertThat(c).containsEntry("cachedfs_stream_prefetch_skipped_budget_bytes", 200L);
    assertThat(c).containsEntry("cachedfs_stream_prefetch_skipped_heap_pressure_bytes", 0L);
    assertThat(c).containsEntry("cachedfs_stream_prefetch_skipped_other_bytes", 0L);
    assertThat(c).containsEntry("cachedfs_stream_prefetch_evicted_before_use_bytes", 7L);
    assertThat(c).containsEntry("cachedfs_stream_prefetch_eligible_suppressed_bytes", 15L);
    assertThat(c).containsEntry("cachedfs_stream_seq_hwm_regime_resets", 1L);
  }

  @Test
  @DisplayName(
      "per-stream counters are live: subsequent inc reflects in the same IOStatistics view")
  void streamCountersAreLive() {
    IoStatistics src = new IoStatistics();
    IOStatistics out = IoStatisticsAdapter.forStream(src);
    assertThat(out.counters().get("stream_read_bytes")).isZero();
    src.incRead(1024);
    // Same IOStatistics handle; expect 1024 on a fresh counters() call.
    assertThat(out.counters().get("stream_read_bytes")).isEqualTo(1024L);
  }

  @Test
  @DisplayName("bootstrap adapter exposes the aggregate counters + registry gauges")
  void bootstrapNamesAndGauges() throws IOException {
    Configuration conf = minimalConf();
    CacheBootstrap.installIfNeeded(conf);
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();

    // Drive a couple of bootstrap-level signals.
    b.aggregateIoStats().incStaleScanIdRecoveries();
    b.trackerFor("scan-A")
        .recordReference(io.github.luciferyang.cachedfs.core.tracker.TrackingId.of(1, 0), 4096);

    IOStatistics out = b.getIOStatistics();
    java.util.Map<String, Long> c = out.counters();
    java.util.Map<String, Long> g = out.gauges();

    // Bootstrap counter.
    assertThat(c).containsEntry("cachedfs_stale_scan_id_recoveries", 1L);
    // Aggregate counters seeded by tracker recording (referencedBytes only — read/prefetch
    // counters are still zero since no stream merged yet).
    assertThat(c).containsEntry("cachedfs_aggregate_read_bytes", 0L);
    // Registry-snapshot gauges.
    assertThat(g).containsEntry("cachedfs_scan_tracker_count", 1L);
    assertThat(g).containsEntry("cachedfs_scan_tracker_entries", 1L);
    assertThat(g).containsEntry("cachedfs_scan_tracker_max_entries", 1L);
    assertThat(g).containsEntry("cachedfs_pending_prefetch_bytes", 0L);
  }

  private static Configuration minimalConf() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, LocalFileSystem.class.getName());
    return conf;
  }
}
