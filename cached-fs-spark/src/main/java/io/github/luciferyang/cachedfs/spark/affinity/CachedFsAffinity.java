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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Public façade for callers that build their own Spark {@code InputPartition}s and want to attach
 * cached-fs-aware preferred locations. The intended call sites are:
 *
 * <ul>
 *   <li>A custom DataSource v2 connector that owns its {@code Scan.planInputPartitions()} and knows
 *       the file path(s) up front. It calls {@link #getPreferredLocations(String, String[])} per
 *       partition and stashes the result on its {@code InputPartition.preferredLocations}.
 *   <li>An integration that injects preferred locations onto Spark's built-in {@code FilePartition}
 *       — the Spark extension registered by this module does that automatically under {@code
 *       spark.sql.extensions}.
 * </ul>
 *
 * <p>The returned strings follow Spark's {@code TaskLocation.apply} convention: {@code
 * "executor_<host>_<execId>"} ⇒ {@code ExecutorCacheTaskLocation} (PROCESS_LOCAL). When no executor
 * is identified, plain host strings ({@code NODE_LOCAL}) are returned. An empty array means: don't
 * override the native locality.
 */
public final class CachedFsAffinity {

  private CachedFsAffinity() {}

  /**
   * Single-path lookup, the form most custom DataSource connectors need.
   *
   * @param filePath qualified file URI as it appears in the underlying {@code FileStatus}
   * @param nativeHosts Hadoop-derived hosts from {@code BlockLocation.getHosts()} — used by the
   *     {@code minTargetHosts} short-circuit so soft affinity does NOT stomp better HDFS locality.
   *     Pass {@code null} or empty when there is no native hint (e.g. object stores).
   * @return up to {@code replicationNum} executor strings in PROCESS_LOCAL form, or an empty array
   *     when the manager is disabled / the native hint is sufficient.
   */
  public static String[] getPreferredLocations(String filePath, String[] nativeHosts) {
    CachedFsSoftAffinityManager mgr = CachedFsSoftAffinityManager.getInstance();
    if (!mgr.isEnabled() || filePath == null || filePath.isEmpty()) return new String[0];
    if (mgr.shouldDeferToNativeLocality(nativeHosts)) return new String[0];
    List<ExecutorNode> nodes = mgr.askExecutorsForPath(filePath, mgr.getReplicationNum());
    return toTaskLocations(nodes);
  }

  /**
   * Multi-split lookup with feedback override. Used by Spark's FileScan path where one
   * InputPartition groups multiple file splits. When {@code detectDuplicateReading} is on, the
   * observed (executor, host) recorded by prior runs of the SAME split-set wins over the
   * consistent-hash result.
   *
   * @param splits ordered file-split fingerprints for this partition
   * @param nativeHosts Hadoop-derived hosts; same contract as the single-path overload
   */
  public static String[] getPreferredLocations(
      List<CachedFsSoftAffinityManager.SplitKey> splits, String[] nativeHosts) {
    CachedFsSoftAffinityManager mgr = CachedFsSoftAffinityManager.getInstance();
    if (!mgr.isEnabled() || splits == null || splits.isEmpty()) return new String[0];
    if (mgr.shouldDeferToNativeLocality(nativeHosts)) return new String[0];
    List<ExecutorNode> observed =
        mgr.isDetectDuplicateReading() ? mgr.askExecutorsForSplit(splits) : Collections.emptyList();
    if (!observed.isEmpty()) {
      return toTaskLocations(observed);
    }
    // No feedback yet — fall back to the consistent-hash result keyed on the FIRST split's path.
    // Multi-split FilePartitions usually pack files from the same dataset, so the first-path key
    // is a stable proxy. (Gluten makes the same choice.)
    List<ExecutorNode> ring =
        mgr.askExecutorsForPath(splits.get(0).path(), mgr.getReplicationNum());
    return toTaskLocations(ring);
  }

  /** Whether the affinity feature is enabled and the ring has executors registered. */
  public static boolean isActive() {
    CachedFsSoftAffinityManager mgr = CachedFsSoftAffinityManager.getInstance();
    return mgr.isEnabled() && mgr.executorCount() > 0;
  }

  // --- helpers -----------------------------------------------------------

  private static String[] toTaskLocations(List<ExecutorNode> nodes) {
    if (nodes == null || nodes.isEmpty()) return new String[0];
    LinkedHashSet<String> out = new LinkedHashSet<>();
    for (ExecutorNode n : nodes) {
      out.add(n.toCacheTaskLocation());
    }
    List<String> list = new ArrayList<>(out);
    return list.toArray(new String[0]);
  }
}
