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

import io.github.luciferyang.cachedfs.spark.affinity.CachedFsSoftAffinityManager.SplitKey;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CachedFsAffinityTest {

  @BeforeEach
  void setUp() {
    CachedFsSoftAffinityManager.resetForTesting();
  }

  @Test
  @DisplayName("disabled façade returns empty (caller falls back to native locality)")
  void disabledReturnsEmpty() {
    String[] locs = CachedFsAffinity.getPreferredLocations("file://x", new String[] {"h1"});
    assertThat(locs).isEmpty();
    assertThat(CachedFsAffinity.isActive()).isFalse();
  }

  @Test
  @DisplayName(
      "static mode: PROCESS_LOCAL strings are returned in executor_<host>_<execId> form Spark"
          + " recognizes")
  void executorCacheTaskLocationFormat() {
    CachedFsSoftAffinityManager mgr = CachedFsSoftAffinityManager.getInstance();
    mgr.setEnabled(true);
    mgr.setReplicationNum(2);
    mgr.handleExecutorAdded("exec-1", "host-a");
    mgr.handleExecutorAdded("exec-2", "host-b");
    mgr.handleExecutorAdded("exec-3", "host-c");

    String[] locs = CachedFsAffinity.getPreferredLocations("hdfs://tbl/part-0", new String[0]);
    assertThat(locs)
        .hasSize(2)
        .allSatisfy(loc -> assertThat(loc).startsWith("executor_").contains("_exec-"));
  }

  @Test
  @DisplayName("minTargetHosts: native hosts that already cover the threshold defer the hint")
  void deferToNativeLocality() {
    CachedFsSoftAffinityManager mgr = CachedFsSoftAffinityManager.getInstance();
    mgr.setEnabled(true);
    mgr.setMinTargetHosts(1);
    mgr.handleExecutorAdded("exec-1", "host-a");

    // Native locality already lists host-a (a live executor host) → defer.
    String[] locs = CachedFsAffinity.getPreferredLocations("hdfs://x", new String[] {"host-a"});
    assertThat(locs).isEmpty();

    // Native locality does NOT include a live executor host → return our hint.
    String[] locs2 =
        CachedFsAffinity.getPreferredLocations("hdfs://x", new String[] {"host-foreign"});
    assertThat(locs2).hasSize(1);
  }

  @Test
  @DisplayName("feedback mode: split-keyed observation wins over static ring lookup")
  void splitLookupObservedExecutor() {
    CachedFsSoftAffinityManager mgr = CachedFsSoftAffinityManager.getInstance();
    mgr.setEnabled(true);
    mgr.setReplicationNum(2);
    mgr.setDetectDuplicateReading(true);
    mgr.handleExecutorAdded("e1", "h1");
    mgr.handleExecutorAdded("e2", "h2");
    mgr.handleExecutorAdded("e3", "h3");

    SplitKey split = new SplitKey(0, "hdfs://tbl/p0", 0, 1024);
    mgr.updateStageSubmitted(11, new int[] {99});
    mgr.updatePartitionMap(99, List.of(split));
    mgr.updateTaskEnd(11, 0, "e2", "h2");

    String[] locs = CachedFsAffinity.getPreferredLocations(List.of(split), new String[0]);
    assertThat(locs).containsExactly("executor_h2_e2");
  }

  @Test
  @DisplayName("feedback mode with no observation falls back to first split's consistent hash")
  void splitLookupFallsBackToHash() {
    CachedFsSoftAffinityManager mgr = CachedFsSoftAffinityManager.getInstance();
    mgr.setEnabled(true);
    mgr.setReplicationNum(2);
    mgr.setDetectDuplicateReading(true);
    mgr.handleExecutorAdded("e1", "h1");
    mgr.handleExecutorAdded("e2", "h2");

    SplitKey split = new SplitKey(0, "hdfs://tbl/p0", 0, 1024);
    String[] locs = CachedFsAffinity.getPreferredLocations(List.of(split), new String[0]);
    assertThat(locs).hasSizeBetween(1, 2);
    assertThat(locs[0]).startsWith("executor_");
  }
}
