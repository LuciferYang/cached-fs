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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AggregatedIoStatisticsTest {

  @Test
  @DisplayName("add(source) merges merged-from-stream counters")
  void addMergesStreamCounters() {
    AggregatedIoStatistics agg = new AggregatedIoStatistics();
    IoStatistics s1 = new IoStatistics();
    s1.incRead(1000);
    s1.incRamHit(800);
    s1.incPrefetch(200);
    s1.incRawOverreadBytes(50);

    IoStatistics s2 = new IoStatistics();
    s2.incRead(500);
    s2.incSsdRead(100);

    agg.add(s1);
    agg.add(s2);

    assertThat(agg.read()).isEqualTo(2);
    assertThat(agg.readBytes()).isEqualTo(1500);
    assertThat(agg.ramHit()).isEqualTo(1);
    assertThat(agg.ramHitBytes()).isEqualTo(800);
    assertThat(agg.prefetchBytes()).isEqualTo(200);
    assertThat(agg.ssdReadBytes()).isEqualTo(100);
    assertThat(agg.rawOverreadBytes()).isEqualTo(50);
  }

  @Test
  @DisplayName("add(NO_OP) leaves the aggregate unchanged")
  void addNoOpIsZero() {
    AggregatedIoStatistics agg = new AggregatedIoStatistics();
    agg.incStaleScanIdRecoveries();
    agg.add(IoStatistics.NO_OP);
    assertThat(agg.readBytes()).isZero();
    assertThat(agg.staleScanIdRecoveries()).isEqualTo(1);
  }

  @Test
  @DisplayName("incStaleScanIdRecoveries is independent of add(source) merges")
  void staleScanIdRecoveriesIsBootstrapOnly() {
    AggregatedIoStatistics agg = new AggregatedIoStatistics();
    agg.incStaleScanIdRecoveries();
    agg.incStaleScanIdRecoveries();

    IoStatistics s = new IoStatistics();
    s.incRead(1000);
    agg.add(s);
    agg.add(s); // double-add to confirm bootstrap counter doesn't budge

    assertThat(agg.staleScanIdRecoveries()).isEqualTo(2);
    assertThat(agg.readBytes()).isEqualTo(2000);
  }

  @Test
  @DisplayName("add(source) merges Phase 5c byte/event counters (prefetchSkipped buckets etc.)")
  void addMergesPhase5cCounters() {
    AggregatedIoStatistics agg = new AggregatedIoStatistics();
    IoStatistics s1 = new IoStatistics();
    s1.incPrefetchSkipped("queue_full", 100);
    s1.incPrefetchSkipped("budget", 200);
    s1.incPrefetchSkipped("heap_pressure", 50);
    s1.incPrefetchEvictedBeforeUse(40);
    s1.incPrefetchEligibleSuppressed(80);
    s1.incSeqHwmRegimeResets();
    s1.incSeqHwmRegimeResets();

    IoStatistics s2 = new IoStatistics();
    s2.incPrefetchSkipped("queue_full", 25);
    s2.incPrefetchSkipped("other", 5);

    agg.add(s1);
    agg.add(s2);

    assertThat(agg.prefetchSkipped("queue_full")).isEqualTo(125);
    assertThat(agg.prefetchSkipped("budget")).isEqualTo(200);
    assertThat(agg.prefetchSkipped("heap_pressure")).isEqualTo(50);
    assertThat(agg.prefetchSkipped("other")).isEqualTo(5);
    assertThat(agg.prefetchEvictedBeforeUse()).isEqualTo(40);
    assertThat(agg.prefetchEligibleSuppressedBytes()).isEqualTo(80);
    assertThat(agg.seqHwmRegimeResets()).isEqualTo(2);
    // Bootstrap-only counter remains untouched by stream merges.
    assertThat(agg.staleScanIdRecoveries()).isZero();
  }

  @Test
  @DisplayName("NO_OP IoStatistics short-circuits all inc*; getters return 0")
  void noOpStaysAtZero() {
    IoStatistics noop = IoStatistics.NO_OP;
    noop.incRead(1000);
    noop.incRamHit(500);
    noop.incPrefetch(200);
    noop.incSsdRead(100);
    noop.incRawOverreadBytes(50);

    assertThat(noop.read()).isZero();
    assertThat(noop.readBytes()).isZero();
    assertThat(noop.ramHit()).isZero();
    assertThat(noop.prefetch()).isZero();
    assertThat(noop.ssdRead()).isZero();
    assertThat(noop.rawOverreadBytes()).isZero();
  }
}
