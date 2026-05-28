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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.luciferyang.cachedfs.core.stats.AggregatedIoStatistics;
import io.github.luciferyang.cachedfs.core.stats.IoStatistics;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CachedFsMeterBinderTest {

  @Test
  @DisplayName("binder registers cumulative counters under cached_fs.* with descriptions")
  void registersCumulativeCounters() {
    AggregatedIoStatistics agg = new AggregatedIoStatistics();
    IoStatistics source = new IoStatistics();
    source.incRead(123);
    source.incRamHit(64);
    source.incSsdRead(48);
    source.incPrefetch(96);
    source.incRawOverreadBytes(8);
    agg.add(source);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CachedFsMeterBinder.builder(agg).build().bindTo(registry);

    // Sample a handful of counter values; each is a FunctionCounter pulling live from agg.
    assertThat(registry.get("cached_fs.read.total").functionCounter().count()).isEqualTo(1.0);
    assertThat(registry.get("cached_fs.read.bytes").functionCounter().count()).isEqualTo(123.0);
    assertThat(registry.get("cached_fs.ram.hit.total").functionCounter().count()).isEqualTo(1.0);
    assertThat(registry.get("cached_fs.ram.hit.bytes").functionCounter().count()).isEqualTo(64.0);
    assertThat(registry.get("cached_fs.ssd.read.total").functionCounter().count()).isEqualTo(1.0);
    assertThat(registry.get("cached_fs.ssd.read.bytes").functionCounter().count()).isEqualTo(48.0);
    assertThat(registry.get("cached_fs.prefetch.total").functionCounter().count()).isEqualTo(1.0);
    assertThat(registry.get("cached_fs.prefetch.bytes").functionCounter().count()).isEqualTo(96.0);
    assertThat(registry.get("cached_fs.raw.overread.bytes").functionCounter().count())
        .isEqualTo(8.0);
  }

  @Test
  @DisplayName("counters reflect live AggregatedIoStatistics updates without re-binding")
  void countersTrackLiveUpdates() {
    AggregatedIoStatistics agg = new AggregatedIoStatistics();
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CachedFsMeterBinder.builder(agg).build().bindTo(registry);

    assertThat(registry.get("cached_fs.read.bytes").functionCounter().count()).isEqualTo(0.0);

    IoStatistics source = new IoStatistics();
    source.incRead(1024);
    agg.add(source);

    // FunctionCounter pulls live: no need to re-register after the source updates.
    assertThat(registry.get("cached_fs.read.bytes").functionCounter().count()).isEqualTo(1024.0);
  }

  @Test
  @DisplayName("prefetch.skipped.bytes carries the documented reason tags with bounded cardinality")
  void prefetchSkippedReasonTagsBounded() {
    AggregatedIoStatistics agg = new AggregatedIoStatistics();
    IoStatistics source = new IoStatistics();
    source.incPrefetchSkipped("queue_full", 100);
    source.incPrefetchSkipped("budget", 200);
    source.incPrefetchSkipped("heap_pressure", 300);
    source.incPrefetchSkipped("other", 400);
    // Unknown reason routes to "other" inside IoStatistics — should still surface only as 4 meters.
    source.incPrefetchSkipped("typo", 500);
    agg.add(source);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CachedFsMeterBinder.builder(agg).build().bindTo(registry);

    List<Meter> reasonMeters =
        registry.find("cached_fs.prefetch.skipped.bytes").meters().stream().toList();
    assertThat(reasonMeters).as("exactly four reason tags — bounded cardinality").hasSize(4);
    assertThat(
            registry
                .get("cached_fs.prefetch.skipped.bytes")
                .tag("reason", "queue_full")
                .functionCounter()
                .count())
        .isEqualTo(100.0);
    assertThat(
            registry
                .get("cached_fs.prefetch.skipped.bytes")
                .tag("reason", "other")
                .functionCounter()
                .count())
        .as("unknown reason 'typo' was folded into 'other' upstream")
        .isEqualTo(900.0);
  }

  @Test
  @DisplayName("gauges read from suppliers and reflect live updates")
  void gaugesReadFromSuppliers() {
    AggregatedIoStatistics agg = new AggregatedIoStatistics();
    AtomicLong entries = new AtomicLong(0);
    AtomicLong maxEntries = new AtomicLong(0);
    AtomicLong pending = new AtomicLong(0);
    AtomicLong budget = new AtomicLong(16L * 1024 * 1024);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CachedFsMeterBinder.builder(agg)
        .scanTrackerEntries(entries::get)
        .scanTrackerMaxEntries(maxEntries::get)
        .pendingPrefetchBytes(pending::get)
        .maxPendingPrefetchBytes(budget::get)
        .build()
        .bindTo(registry);

    assertThat(registry.get("cached_fs.scan_tracker.entries").gauge().value()).isEqualTo(0.0);
    assertThat(registry.get("cached_fs.prefetch.budget.bytes").gauge().value())
        .isEqualTo((double) budget.get());

    entries.set(42);
    pending.set(8192);
    assertThat(registry.get("cached_fs.scan_tracker.entries").gauge().value()).isEqualTo(42.0);
    assertThat(registry.get("cached_fs.prefetch.pending.bytes").gauge().value()).isEqualTo(8192.0);
  }

  @Test
  @DisplayName("omitted suppliers default to zero so partial wiring still produces valid gauges")
  void omittedSuppliersDefaultToZero() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CachedFsMeterBinder.builder(new AggregatedIoStatistics()).build().bindTo(registry);

    // Build with no suppliers; gauges still register but read as 0.
    assertThat(registry.get("cached_fs.scan_tracker.entries").gauge().value()).isEqualTo(0.0);
    assertThat(registry.get("cached_fs.scan_tracker.max_entries").gauge().value()).isEqualTo(0.0);
    assertThat(registry.get("cached_fs.prefetch.pending.bytes").gauge().value()).isEqualTo(0.0);
    assertThat(registry.get("cached_fs.prefetch.budget.bytes").gauge().value()).isEqualTo(0.0);
  }
}
