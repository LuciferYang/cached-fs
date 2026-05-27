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
package io.github.luciferyang.cachedfs.spark.affinity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CachedFsSoftAffinityManagerTest {

  private CachedFsSoftAffinityManager mgr;

  @BeforeEach
  void setUp() {
    CachedFsSoftAffinityManager.resetForTesting();
    mgr = CachedFsSoftAffinityManager.getInstance();
    mgr.setEnabled(true);
  }

  @Test
  @DisplayName("disabled manager never returns preferred locations")
  void disabledReturnsEmpty() {
    mgr.setEnabled(false);
    mgr.handleExecutorAdded("e1", "h1");
    assertThat(mgr.askExecutorsForPath("file://path", 2)).isEmpty();
  }

  @Test
  @DisplayName("static mode: askExecutorsForPath returns up to replicationNum executors")
  void staticModeTopN() {
    mgr.handleExecutorAdded("e1", "h1");
    mgr.handleExecutorAdded("e2", "h2");
    mgr.handleExecutorAdded("e3", "h3");

    assertThat(mgr.askExecutorsForPath("hdfs://tbl/p0", 1)).hasSize(1);
    assertThat(mgr.askExecutorsForPath("hdfs://tbl/p0", 2)).hasSize(2);
    assertThat(mgr.askExecutorsForPath("hdfs://tbl/p0", 10)).hasSize(3);
  }

  @Test
  @DisplayName("static mode: same file path always maps to same primary (consistent-hash)")
  void staticModeDeterministic() {
    mgr.handleExecutorAdded("e1", "h1");
    mgr.handleExecutorAdded("e2", "h2");

    ExecutorNode first = mgr.askExecutorsForPath("hdfs://tbl/p0", 1).get(0);
    for (int i = 0; i < 50; i++) {
      assertThat(mgr.askExecutorsForPath("hdfs://tbl/p0", 1)).first().isEqualTo(first);
    }
  }

  @Test
  @DisplayName("handleExecutorRemoved drops the executor from lookups")
  void executorRemoval() {
    mgr.handleExecutorAdded("e1", "h1");
    mgr.handleExecutorAdded("e2", "h2");
    mgr.handleExecutorRemoved("e1");
    assertThat(mgr.executorCount()).isEqualTo(1);
    assertThat(mgr.askExecutorsForPath("hdfs://tbl/p0", 5))
        .extracting(ExecutorNode::executorId)
        .doesNotContain("e1");
  }

  @Test
  @DisplayName("shouldDeferToNativeLocality short-circuits when native hosts cover minTargetHosts")
  void minTargetHostsShortCircuit() {
    mgr.setMinTargetHosts(2);
    mgr.handleExecutorAdded("e1", "h-a");
    mgr.handleExecutorAdded("e2", "h-b");
    mgr.handleExecutorAdded("e3", "h-c");

    // Native hosts include 2+ live-executor hosts → defer.
    assertThat(mgr.shouldDeferToNativeLocality(new String[] {"h-a", "h-b", "h-other"})).isTrue();
    // Native hosts include only 1 live host → don't defer.
    assertThat(mgr.shouldDeferToNativeLocality(new String[] {"h-a", "h-other"})).isFalse();
    // Empty native locality → don't defer.
    assertThat(mgr.shouldDeferToNativeLocality(new String[0])).isFalse();
  }

  @Test
  @DisplayName("feedback mode off: askExecutorsForSplit always returns empty even after task ends")
  void feedbackModeOffNeverObserves() {
    mgr.setDetectDuplicateReading(false);
    mgr.handleExecutorAdded("e1", "h1");

    SplitKey s = new SplitKey(0, "hdfs://tbl/p0", 0, 1024);
    mgr.updateStageSubmitted(1, new int[] {42});
    mgr.updatePartitionMap(42, List.of(s));
    mgr.updateTaskEnd(1, 0, "e1", "h1");

    assertThat(mgr.askExecutorsForSplit(List.of(s))).isEmpty();
  }

  @Test
  @DisplayName(
      "feedback mode: task-end observation overrides consistent-hash on next askExecutorsForSplit")
  void feedbackModeObservationOverrides() {
    mgr.setDetectDuplicateReading(true);
    mgr.handleExecutorAdded("e1", "h1");
    mgr.handleExecutorAdded("e2", "h2");
    mgr.handleExecutorAdded("e3", "h3");

    SplitKey s = new SplitKey(0, "hdfs://tbl/p0", 0, 1024);
    mgr.updateStageSubmitted(7, new int[] {42});
    mgr.updatePartitionMap(42, List.of(s));
    // Pretend task ran on e2/h2.
    mgr.updateTaskEnd(7, 0, "e2", "h2");

    List<ExecutorNode> observed = mgr.askExecutorsForSplit(List.of(s));
    assertThat(observed).hasSize(1);
    assertThat(observed.get(0)).isEqualTo(new ExecutorNode("e2", "h2"));
  }

  @Test
  @DisplayName("feedback mode: observed entry tied to a removed executor is filtered out on read")
  void feedbackModeFiltersDeadExecutors() {
    mgr.setDetectDuplicateReading(true);
    mgr.handleExecutorAdded("e1", "h1");

    SplitKey s = new SplitKey(0, "hdfs://tbl/p0", 0, 1024);
    mgr.updateStageSubmitted(7, new int[] {42});
    mgr.updatePartitionMap(42, List.of(s));
    mgr.updateTaskEnd(7, 0, "e1", "h1");
    mgr.handleExecutorRemoved("e1");

    assertThat(mgr.askExecutorsForSplit(List.of(s))).isEmpty();
  }

  @Test
  @DisplayName(
      "feedback mode: cleanMiddleStatusMap drops state for completed stage but keeps observations")
  void feedbackModeStageCleanup() {
    mgr.setDetectDuplicateReading(true);
    mgr.handleExecutorAdded("e1", "h1");

    SplitKey s = new SplitKey(0, "hdfs://tbl/p0", 0, 1024);
    mgr.updateStageSubmitted(7, new int[] {42});
    mgr.updatePartitionMap(42, List.of(s));
    mgr.updateTaskEnd(7, 0, "e1", "h1");

    mgr.cleanMiddleStatusMap(7, new int[] {42});

    // Observations survive (they are the durable learning); intermediate stage/partition state
    // gets cleared.
    assertThat(mgr.askExecutorsForSplit(List.of(s))).hasSize(1);
    assertThat(mgr.snapshot().duplicateReadingEntries()).isEqualTo(1);
  }

  @Test
  @DisplayName("snapshot exposes the live tuple (enabled, repl, minTargetHosts, executor count)")
  void snapshotReflectsState() {
    mgr.setReplicationNum(3);
    mgr.setMinTargetHosts(2);
    mgr.setDetectDuplicateReading(true);
    mgr.handleExecutorAdded("e1", "h-a");
    mgr.handleExecutorAdded("e2", "h-b");

    var snap = mgr.snapshot();
    assertThat(snap.enabled()).isTrue();
    assertThat(snap.replicationNum()).isEqualTo(3);
    assertThat(snap.minTargetHosts()).isEqualTo(2);
    assertThat(snap.detectDuplicateReading()).isTrue();
    assertThat(snap.executorCount()).isEqualTo(2);
    assertThat(snap.hostCount()).isEqualTo(2);
  }

  @Test
  @DisplayName("composeKey produces a stable, sorted, separator-joined fingerprint")
  void composeKeyDeterministic() {
    SplitKey a = new SplitKey(0, "hdfs://a", 0, 100);
    SplitKey b = new SplitKey(0, "hdfs://b", 0, 200);
    // Order of input list doesn't affect the result.
    String k1 = CachedFsSoftAffinityManager.composeKey(List.of(a, b));
    String k2 = CachedFsSoftAffinityManager.composeKey(List.of(b, a));
    String expected =
        "hdfs://a"
            + CachedFsSoftAffinityManager.KEY_FIELD_SEPARATOR
            + "0"
            + CachedFsSoftAffinityManager.KEY_FIELD_SEPARATOR
            + "100"
            + CachedFsSoftAffinityManager.KEY_SPLIT_SEPARATOR
            + "hdfs://b"
            + CachedFsSoftAffinityManager.KEY_FIELD_SEPARATOR
            + "0"
            + CachedFsSoftAffinityManager.KEY_FIELD_SEPARATOR
            + "200";
    assertThat(k1).isEqualTo(k2).isEqualTo(expected);
  }

  @Test
  @DisplayName("handleExecutorAdded rejects empty/null host (would corrupt TaskLocation)")
  void rejectsEmptyHost() {
    mgr.handleExecutorAdded("e-empty", "");
    mgr.handleExecutorAdded("e-null", null);
    assertThat(mgr.executorCount()).isZero();
  }

  @Test
  @DisplayName("handleExecutorAdded rejects host containing '_' (would corrupt TaskLocation)")
  void rejectsUnderscoreHost() {
    mgr.handleExecutorAdded("e-bad", "host_with_underscore");
    assertThat(mgr.executorCount()).isZero();
  }

  @Test
  @DisplayName("initialize rebuilds the singleton when state is empty (lossless re-density tuning)")
  void initializeRebuildsOnEmptyState() {
    CachedFsSoftAffinityManager.resetForTesting();
    CachedFsSoftAffinityManager first = CachedFsSoftAffinityManager.initialize(50);
    assertThat(first.snapshot().virtualNodes()).isEqualTo(50);
    // No executors / observations attached → second initialize with a different density REBUILDS
    // the singleton with the new value. Lossless because there was no state to drop.
    CachedFsSoftAffinityManager rebuilt = CachedFsSoftAffinityManager.initialize(200);
    assertThat(rebuilt.snapshot().virtualNodes()).isEqualTo(200);
  }

  @Test
  @DisplayName("initialize rebuild preserves all five SparkConf-driven knobs across the swap")
  void initializeRebuildPreservesKnobs() {
    CachedFsSoftAffinityManager.resetForTesting();
    CachedFsSoftAffinityManager first = CachedFsSoftAffinityManager.initialize(50);
    // Mutate every preserved knob to a non-default value via configureAtomic (the production
    // façade path) so the test exercises the same code path the extension uses.
    first.configureAtomic(
        /* enabled */ true,
        /* replicationNum */ 3,
        /* minTargetHosts */ 2,
        /* detectDuplicateReading */ true,
        /* duplicateReadingMaxCacheItems */ 500);
    // No executors / observations attached → rebuild is loss-free. Knob values MUST carry over.
    CachedFsSoftAffinityManager rebuilt = CachedFsSoftAffinityManager.initialize(200);
    AffinitySnapshot snap = rebuilt.snapshot();
    assertThat(snap.virtualNodes()).isEqualTo(200);
    assertThat(snap.enabled()).isTrue();
    assertThat(snap.replicationNum()).isEqualTo(3);
    assertThat(snap.minTargetHosts()).isEqualTo(2);
    assertThat(snap.detectDuplicateReading()).isTrue();
    assertThat(snap.duplicateReadingMaxCacheItems()).isEqualTo(500);
  }

  @Test
  @DisplayName(
      "initialize keeps prior density when state is attached (lossy rebuild would be unsafe)")
  void initializeKeepsDensityWhenStateAttached() {
    CachedFsSoftAffinityManager.resetForTesting();
    CachedFsSoftAffinityManager first = CachedFsSoftAffinityManager.initialize(50);
    first.handleExecutorAdded("e1", "host-1"); // attach state
    // Second initialize with different density must NOT silently drop the executor.
    CachedFsSoftAffinityManager again = CachedFsSoftAffinityManager.initialize(200);
    assertThat(again).isSameAs(first);
    assertThat(again.snapshot().virtualNodes()).isEqualTo(50);
    assertThat(again.executorCount()).isEqualTo(1);
  }

  @Test
  @DisplayName("setDuplicateReadingMaxCacheItems shrinks ALL THREE feedback-state maps in-place")
  void setMaxCacheItemsShrinksAllThreeMaps() {
    mgr.setDetectDuplicateReading(true);
    mgr.handleExecutorAdded("e1", "h1");
    for (int i = 0; i < 10; i++) {
      mgr.updateStageSubmitted(i, new int[] {100 + i});
      SplitKey s = new SplitKey(0, "hdfs://tbl/p" + i, 0, 1024);
      mgr.updatePartitionMap(100 + i, List.of(s));
      mgr.updateTaskEnd(i, 0, "e1", "h1");
    }
    AffinitySnapshot before = mgr.snapshot();
    assertThat(before.duplicateReadingEntries()).isEqualTo(10);
    assertThat(before.stageRddsCount()).isEqualTo(10);
    assertThat(before.rddPartitionsCount()).isEqualTo(10);

    mgr.setDuplicateReadingMaxCacheItems(4);

    AffinitySnapshot after = mgr.snapshot();
    assertThat(after.duplicateReadingEntries()).isEqualTo(4);
    assertThat(after.stageRddsCount()).isEqualTo(4);
    assertThat(after.rddPartitionsCount()).isEqualTo(4);
    assertThat(after.duplicateReadingMaxCacheItems()).isEqualTo(4);
  }

  @Test
  @DisplayName("setReplicationNum rejects non-positive values with a clear exception")
  void setReplicationNumValidates() {
    assertThatThrownBy(() -> mgr.setReplicationNum(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("replicationNum");
  }
}
