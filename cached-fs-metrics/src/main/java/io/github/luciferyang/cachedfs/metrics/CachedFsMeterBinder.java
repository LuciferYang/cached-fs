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
package io.github.luciferyang.cachedfs.metrics;

import io.github.luciferyang.cachedfs.core.stats.AggregatedIoStatistics;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Micrometer {@link MeterBinder} that exposes cached-fs counters and gauges to any {@link
 * MeterRegistry} (Prometheus, JMX, statsd, etc.). Wire once after {@code
 * CacheBootstrap.installIfNeeded} and the registry will pick up every cumulative counter as a
 * {@link FunctionCounter} (correct semantics for derived rates) and every point-in-time value as a
 * {@link Gauge}.
 *
 * <p>cached-fs-metrics intentionally does NOT depend on cached-fs-hadoop — the binder is wired with
 * {@link LongSupplier}s for the bootstrap-level gauges so the same artifact composes against any
 * decorator (Hadoop today; a hypothetical native-S3 decorator tomorrow). Build the binder via the
 * {@link Builder} so unused inputs default to a no-op supplier without compile-time noise.
 *
 * <p>Counter naming: all meters live under the {@code cached_fs.*} prefix, dot-separated per
 * Micrometer convention; Prometheus exporters automatically rewrite dots to underscores. Reason
 * tags on {@code prefetch.skipped.bytes} come from {@link AggregatedIoStatistics#prefetchSkipped}'s
 * fixed reason set (queue_full / budget / heap_pressure / other), keeping cardinality bounded.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * MeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
 * CacheBootstrap b = CacheBootstrap.installIfNeeded(conf);
 * CachedFsMeterBinder.builder(b.aggregateIoStats())
 *     .scanTrackerEntries(b::scanTrackerEntries)
 *     .scanTrackerMaxEntries(b::scanTrackerMaxEntries)
 *     .pendingPrefetchBytes(AsyncDataCache.getInstance()::pendingPrefetchBytes)
 *     .maxPendingPrefetchBytes(b::maxPendingPrefetchBytes)
 *     .build()
 *     .bindTo(registry);
 * }</pre>
 */
public final class CachedFsMeterBinder implements MeterBinder {

  private static final String PREFIX = "cached_fs.";

  /**
   * Fixed reason set; mirrors IoStatistics.PREFETCH_SKIPPED_REASONS so tag cardinality is bounded.
   */
  private static final String[] PREFETCH_SKIPPED_REASONS = {
    "queue_full", "budget", "heap_pressure", "other"
  };

  private static final LongSupplier ZERO = () -> 0L;

  private final AggregatedIoStatistics agg;
  private final LongSupplier scanTrackerEntries;
  private final LongSupplier scanTrackerMaxEntries;
  private final LongSupplier scanTrackerEntriesRejected;
  private final LongSupplier pendingPrefetchBytes;
  private final LongSupplier maxPendingPrefetchBytes;

  private CachedFsMeterBinder(
      AggregatedIoStatistics agg,
      LongSupplier scanTrackerEntries,
      LongSupplier scanTrackerMaxEntries,
      LongSupplier scanTrackerEntriesRejected,
      LongSupplier pendingPrefetchBytes,
      LongSupplier maxPendingPrefetchBytes) {
    this.agg = Objects.requireNonNull(agg, "AggregatedIoStatistics");
    this.scanTrackerEntries = scanTrackerEntries;
    this.scanTrackerMaxEntries = scanTrackerMaxEntries;
    this.scanTrackerEntriesRejected = scanTrackerEntriesRejected;
    this.pendingPrefetchBytes = pendingPrefetchBytes;
    this.maxPendingPrefetchBytes = maxPendingPrefetchBytes;
  }

  /** Creates a binder builder around the given {@link AggregatedIoStatistics}. */
  public static Builder builder(AggregatedIoStatistics agg) {
    return new Builder(agg);
  }

  @Override
  public void bindTo(MeterRegistry registry) {
    // Cumulative counts → FunctionCounter so derived rates (rate(cached_fs_read_total[1m])) work.
    counter(registry, "read.total", "read operations served", agg, AggregatedIoStatistics::read);
    counter(
        registry,
        "read.bytes",
        "bytes returned to consumer",
        agg,
        AggregatedIoStatistics::readBytes);
    counter(registry, "ram.hit.total", "RAM-tier cache hits", agg, AggregatedIoStatistics::ramHit);
    counter(
        registry,
        "ram.hit.bytes",
        "bytes served from RAM tier",
        agg,
        AggregatedIoStatistics::ramHitBytes);
    counter(registry, "ssd.read.total", "SSD-tier reads", agg, AggregatedIoStatistics::ssdRead);
    counter(
        registry,
        "ssd.read.bytes",
        "bytes served from SSD tier",
        agg,
        AggregatedIoStatistics::ssdReadBytes);
    counter(
        registry,
        "prefetch.total",
        "prefetch tasks submitted",
        agg,
        AggregatedIoStatistics::prefetch);
    counter(
        registry, "prefetch.bytes", "bytes prefetched", agg, AggregatedIoStatistics::prefetchBytes);
    counter(
        registry,
        "raw.overread.bytes",
        "bytes pulled in by gap-coalescing that no requester wanted",
        agg,
        AggregatedIoStatistics::rawOverreadBytes);
    counter(
        registry,
        "stale.scan.id.recoveries",
        "ScanTracker recoveries after a stale scanId reuse (typically Spark thread reuse)",
        agg,
        AggregatedIoStatistics::staleScanIdRecoveries);
    counter(
        registry,
        "prefetch.evicted.before.use.bytes",
        "bytes prefetched then evicted by TTL before the consumer could observe them",
        agg,
        AggregatedIoStatistics::prefetchEvictedBeforeUse);
    counter(
        registry,
        "prefetch.eligible.suppressed.bytes",
        "bytes the admission gate suppressed solely on the density predicate",
        agg,
        AggregatedIoStatistics::prefetchEligibleSuppressedBytes);
    counter(
        registry,
        "seq.hwm.regime.resets",
        "sequential HWM CAS-loop regime resets",
        agg,
        AggregatedIoStatistics::seqHwmRegimeResets);

    // Tag-keyed prefetch-skipped reason buckets. Bounded reason set keeps cardinality predictable.
    for (String reason : PREFETCH_SKIPPED_REASONS) {
      FunctionCounter.builder(
              PREFIX + "prefetch.skipped.bytes", agg, a -> (double) a.prefetchSkipped(reason))
          .description("bytes the prefetch admission gate skipped, tagged by reason")
          .tag("reason", reason)
          .baseUnit("bytes")
          .register(registry);
    }

    // Latency totals are currently per-stream (IoStatistics) and not yet aggregated in
    // AggregatedIoStatistics — when the aggregator grows latency getters, register them here
    // as FunctionCounters so Prometheus can derive per-second latency rates.

    // Point-in-time gauges — bootstrap supplies these via LongSupplier so cached-fs-metrics
    // stays decoupled from cached-fs-hadoop / cached-fs-core internals.
    gauge(registry, "scan_tracker.entries", "live ScanTracker entries", scanTrackerEntries);
    gauge(
        registry,
        "scan_tracker.max_entries",
        "max ScanTracker entries observed since install",
        scanTrackerMaxEntries);
    gauge(
        registry,
        "scan_tracker.entries_rejected",
        "recordReference/recordRead calls dropped because the per-tracker entry cap was hit",
        scanTrackerEntriesRejected);
    gauge(
        registry,
        "prefetch.pending.bytes",
        "bytes currently pending prefetch admission",
        pendingPrefetchBytes);
    gauge(
        registry,
        "prefetch.budget.bytes",
        "max bytes the prefetch admission gate will allow in flight",
        maxPendingPrefetchBytes);
  }

  private static void counter(
      MeterRegistry registry,
      String name,
      String description,
      AggregatedIoStatistics agg,
      java.util.function.ToDoubleFunction<AggregatedIoStatistics> fn) {
    FunctionCounter.builder(PREFIX + name, agg, fn).description(description).register(registry);
  }

  private static void gauge(
      MeterRegistry registry, String name, String description, LongSupplier supplier) {
    Gauge.builder(PREFIX + name, supplier, s -> (double) s.getAsLong())
        .description(description)
        .register(registry);
  }

  /** Builder. Use the {@link CachedFsMeterBinder#builder(AggregatedIoStatistics)} static method. */
  public static final class Builder {
    private final AggregatedIoStatistics agg;
    private LongSupplier scanTrackerEntries = ZERO;
    private LongSupplier scanTrackerMaxEntries = ZERO;
    private LongSupplier scanTrackerEntriesRejected = ZERO;
    private LongSupplier pendingPrefetchBytes = ZERO;
    private LongSupplier maxPendingPrefetchBytes = ZERO;

    private Builder(AggregatedIoStatistics agg) {
      this.agg = agg;
    }

    public Builder scanTrackerEntries(LongSupplier supplier) {
      this.scanTrackerEntries = Objects.requireNonNull(supplier);
      return this;
    }

    public Builder scanTrackerMaxEntries(LongSupplier supplier) {
      this.scanTrackerMaxEntries = Objects.requireNonNull(supplier);
      return this;
    }

    public Builder scanTrackerEntriesRejected(LongSupplier supplier) {
      this.scanTrackerEntriesRejected = Objects.requireNonNull(supplier);
      return this;
    }

    public Builder pendingPrefetchBytes(LongSupplier supplier) {
      this.pendingPrefetchBytes = Objects.requireNonNull(supplier);
      return this;
    }

    public Builder maxPendingPrefetchBytes(LongSupplier supplier) {
      this.maxPendingPrefetchBytes = Objects.requireNonNull(supplier);
      return this;
    }

    public CachedFsMeterBinder build() {
      return new CachedFsMeterBinder(
          agg,
          scanTrackerEntries,
          scanTrackerMaxEntries,
          scanTrackerEntriesRejected,
          pendingPrefetchBytes,
          maxPendingPrefetchBytes);
    }
  }

  /** Test-only view of the reason set so a test can iterate the same tags this class binds. */
  static Map<String, String> reasonTagsForTesting() {
    java.util.LinkedHashMap<String, String> m = new java.util.LinkedHashMap<>();
    for (String r : PREFETCH_SKIPPED_REASONS) {
      m.put(r, r);
    }
    return java.util.Collections.unmodifiableMap(m);
  }
}
