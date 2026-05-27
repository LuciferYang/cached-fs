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
 * is identified, an empty array is returned — caller falls back to native locality.
 *
 * <p><b>Spark caps preferred-locations at 3 hosts</b> per file (FilePartition.scala uses {@code
 * take(3)} after grouping by length). Setting {@code replication-num} higher than 3 has no effect
 * on Spark planning; the upper candidates are silently dropped.
 */
public final class CachedFsAffinity {

  private CachedFsAffinity() {}

  /**
   * Single-path lookup. Suitable for custom DataSource connectors operating on one file per
   * partition.
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
    return toTaskLocations(mgr.askExecutorsForPath(filePath, mgr.getReplicationNum()));
  }

  /**
   * Multi-split lookup with feedback override. When {@code detectDuplicateReading} is enabled and a
   * prior task placement for the same split-set was recorded via {@link #recordPartitionMap}, the
   * observed executor wins over the consistent-hash result.
   *
   * <p>When no observation exists yet, falls back to the consistent-hash lookup keyed on the first
   * split's path (Gluten makes the same choice). Acceptable proxy because multi-split
   * FilePartitions usually pack files from the same dataset.
   *
   * @param splits ordered file-split fingerprints for this partition
   * @param nativeHosts Hadoop-derived hosts; same contract as the single-path overload
   */
  public static String[] getPreferredLocations(List<SplitKey> splits, String[] nativeHosts) {
    CachedFsSoftAffinityManager mgr = CachedFsSoftAffinityManager.getInstance();
    if (!mgr.isEnabled() || splits == null || splits.isEmpty()) return new String[0];
    if (mgr.shouldDeferToNativeLocality(nativeHosts)) return new String[0];
    if (mgr.isDetectDuplicateReading()) {
      List<ExecutorNode> observed = mgr.askExecutorsForSplit(splits);
      if (!observed.isEmpty()) {
        return toTaskLocations(observed);
      }
    }
    return toTaskLocations(mgr.askExecutorsForPath(splits.get(0).path(), mgr.getReplicationNum()));
  }

  /** Whether the affinity feature is enabled and the ring has executors registered. */
  public static boolean isActive() {
    CachedFsSoftAffinityManager mgr = CachedFsSoftAffinityManager.getInstance();
    return mgr.isEnabled() && mgr.executorCount() > 0;
  }

  /**
   * Records the (rddId, splits) mapping that feedback mode needs to attribute task-end placements
   * back to the file region that drove the read. Integrators with custom DataSource v2 scans should
   * call this from their {@code Scan.planInputPartitions()} so the listener's {@code onTaskEnd} can
   * lookup the partition's file-splits when it fires.
   *
   * <p>Calling this when feedback mode is disabled is a no-op. Calling it from Spark's built-in
   * {@code FileSourceScanExec} path is not possible (Spark does not expose its planned mapping to
   * external code) — only static-mode affinity applies in that case.
   */
  public static void recordPartitionMap(int rddId, List<SplitKey> splits) {
    CachedFsSoftAffinityManager.getInstance().updatePartitionMap(rddId, splits);
  }

  // --- configuration façade (used by the Scala extension) ----------------

  /**
   * Configures the manager's knobs from already-parsed SparkConf values. Bundles all five
   * SparkConf-driven settings into one atomic call so the extension can't observe a half-applied
   * state. Validates inputs and rethrows clear messages naming the SparkConf key.
   */
  public static void configure(
      boolean enabled,
      int replicationNum,
      int minTargetHosts,
      boolean detectDuplicateReading,
      int duplicateReadingMaxCacheItems) {
    if (replicationNum <= 0) {
      throw new IllegalArgumentException(
          CachedFsAffinityConfig.REPLICATION_NUM + " must be positive, got " + replicationNum);
    }
    if (minTargetHosts < 0) {
      throw new IllegalArgumentException(
          CachedFsAffinityConfig.MIN_TARGET_HOSTS + " must be non-negative, got " + minTargetHosts);
    }
    if (duplicateReadingMaxCacheItems <= 0) {
      throw new IllegalArgumentException(
          CachedFsAffinityConfig.DUPLICATE_READING_MAX_CACHE_ITEMS
              + " must be positive, got "
              + duplicateReadingMaxCacheItems);
    }
    if (replicationNum > SPARK_PREFERRED_LOCATIONS_CAP) {
      org.slf4j.LoggerFactory.getLogger(CachedFsAffinity.class)
          .warn(
              "{}={} exceeds Spark's FilePartition.preferredLocations cap of {}; the upper "
                  + "candidates will be silently dropped by Spark's planner.",
              CachedFsAffinityConfig.REPLICATION_NUM,
              replicationNum,
              SPARK_PREFERRED_LOCATIONS_CAP);
    }
    // Single write-lock acquisition under the hood — no half-applied state visible to readers.
    CachedFsSoftAffinityManager.getInstance()
        .configureAtomic(
            enabled,
            replicationNum,
            minTargetHosts,
            detectDuplicateReading,
            duplicateReadingMaxCacheItems);
  }

  /**
   * Spark's {@code FilePartition.preferredLocations} caps at 3 hosts per file (FilePartition.scala
   * groups by length then {@code take(3)}). Setting {@code replication-num} higher has no effect on
   * Spark's planner — extra candidates are silently dropped. We warn at {@link #configure} when the
   * operator exceeds this.
   */
  static final int SPARK_PREFERRED_LOCATIONS_CAP = 3;

  /**
   * Initializes the manager singleton with the requested ring density. Idempotent: if the singleton
   * already exists with a different value, the existing density is preserved (and a WARN is logged
   * by the manager). Callers MUST invoke this BEFORE any code path causes the singleton to
   * materialize via {@link CachedFsSoftAffinityManager#getInstance}.
   */
  public static void initializeWithVirtualNodes(int virtualNodes) {
    CachedFsSoftAffinityManager.initialize(virtualNodes);
  }

  // notifyApplicationEnd is moved below to the @hidden listener-internal block — it has NO
  // identity guard of its own and an external caller invoking it would wipe shared state
  // belonging to whichever SparkContext currently owns the listener registration. Production
  // callers must let the registered listener fire it via its identity-guarded onApplicationEnd.

  // --- listener entry points (called by CachedFsSoftAffinityListener) -------
  //
  // These bridge SparkListener events to the manager's package-private mutators. Putting them
  // here keeps the manager's listener-coupled surface (handleExecutorAdded etc.) package-private
  // while still letting the Scala listener in a sibling package fire events. External Java /
  // Spark integrators should NOT call these — they are internal wiring for the cached-fs Spark
  // extension. Use configure / getPreferredLocations / recordPartitionMap / notifyApplicationEnd
  // for the supported public surface.

  /**
   * @hidden listener-internal.
   */
  public static void onExecutorAdded(String executorId, String host) {
    CachedFsSoftAffinityManager.getInstance().handleExecutorAdded(executorId, host);
  }

  /**
   * @hidden listener-internal.
   */
  public static void onExecutorRemoved(String executorId) {
    CachedFsSoftAffinityManager.getInstance().handleExecutorRemoved(executorId);
  }

  /**
   * @hidden listener-internal.
   */
  public static void onStageSubmitted(int stageId, int[] rddIds) {
    CachedFsSoftAffinityManager.getInstance().updateStageSubmitted(stageId, rddIds);
  }

  /**
   * @hidden listener-internal.
   */
  public static void onStageCompleted(int stageId, int[] rddIds) {
    CachedFsSoftAffinityManager.getInstance().cleanMiddleStatusMap(stageId, rddIds);
  }

  /**
   * @hidden listener-internal.
   */
  public static void onTaskEnd(int stageId, int partitionId, String executorId, String host) {
    CachedFsSoftAffinityManager.getInstance().updateTaskEnd(stageId, partitionId, executorId, host);
  }

  /**
   * @hidden listener-internal. Per-SparkContext reset hook. Called by {@code
   *     CachedFsSoftAffinityListener.onApplicationEnd} AFTER the identity-CAS confirms that the
   *     calling listener is the registered owner. Direct callers MUST NOT invoke this — there is no
   *     identity guard here, and wiping shared state when a different SparkContext is the active
   *     owner corrupts that context's affinity hints.
   */
  public static void notifyApplicationEnd() {
    CachedFsSoftAffinityManager.getInstance().resetForApplicationEnd();
  }

  // --- helpers -----------------------------------------------------------

  private static String[] toTaskLocations(List<ExecutorNode> nodes) {
    if (nodes == null || nodes.isEmpty()) return new String[0];
    LinkedHashSet<String> out = new LinkedHashSet<>();
    for (ExecutorNode n : nodes) {
      out.add(n.toCacheTaskLocation());
    }
    return new ArrayList<>(out).toArray(new String[0]);
  }
}
