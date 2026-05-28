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

/**
 * Configuration keys + defaults for the Spark soft-affinity layer. All keys live under {@code
 * spark.cached-fs.affinity.*} and are read from {@code SparkConf} by the listener / extension
 * during initialization. Centralizing them here keeps the manager pure (no SparkEnv dependency) and
 * makes the surface easy to document.
 */
public final class CachedFsAffinityConfig {

  private CachedFsAffinityConfig() {}

  /**
   * Master switch. When false, the manager is initialized but never returns preferred locations.
   */
  public static final String ENABLED = "spark.cached-fs.affinity.enabled";

  public static final boolean DEFAULT_ENABLED = false;

  /**
   * Number of executor candidates returned per file. The first is the primary; subsequent entries
   * provide fault-tolerance when an executor is busy / dead at task-launch time. Mirrors Gluten's
   * {@code spark.gluten.soft-affinity.replications.num}.
   */
  public static final String REPLICATION_NUM = "spark.cached-fs.affinity.replication-num";

  public static final int DEFAULT_REPLICATION_NUM = 2;

  /**
   * When HDFS-native locality already covers at least this many target hosts, skip the soft hint
   * and let Spark schedule against native locality. Prevents soft-affinity from stomping better
   * data-locality information on HDFS clusters. Mirrors Gluten's {@code
   * spark.gluten.soft-affinity.min.target-hosts}.
   */
  public static final String MIN_TARGET_HOSTS = "spark.cached-fs.affinity.min-target-hosts";

  public static final int DEFAULT_MIN_TARGET_HOSTS = 1;

  /**
   * Number of virtual partitions per physical executor on the ring. Higher values give smoother key
   * distribution at the cost of more memory + slightly slower add/remove. 100 is the sweet spot
   * lance-spark-zb picked; Gluten defaults to {@code softAffinityReplicationNum} which conflates
   * replication with virtualization. We keep them separate so operators can tune density without
   * affecting fan-out.
   */
  public static final String VIRTUAL_NODES = "spark.cached-fs.affinity.virtual-nodes";

  public static final int DEFAULT_VIRTUAL_NODES = 100;

  /**
   * Opt-in feedback mode. When true, a SparkListener records the (executor, host) that actually ran
   * each FilePartition split, keyed by {@code "path_start_length"}. Subsequent reads of the same
   * split prefer that executor over the consistent-hash candidate. Mirrors Gluten's {@code
   * spark.gluten.soft-affinity.duplicateReadingDetect.enabled}.
   */
  public static final String DUPLICATE_READING_DETECT_ENABLED =
      "spark.cached-fs.affinity.duplicate-reading-detect.enabled";

  public static final boolean DEFAULT_DUPLICATE_READING_DETECT_ENABLED = false;

  /**
   * Maximum entries retained in each of the three feedback-mode bounded maps (stage RDDs, RDD
   * partitions, duplicate-read observations). Keeps memory bounded on long-running drivers. Old
   * entries are evicted FIFO once the cap is reached (insertion-order LinkedHashMap); the per-stage
   * cleanup hook also drops entries proactively when the stage finishes.
   */
  public static final String DUPLICATE_READING_MAX_CACHE_ITEMS =
      "spark.cached-fs.affinity.duplicate-reading.max-cache-items";

  public static final int DEFAULT_DUPLICATE_READING_MAX_CACHE_ITEMS = 10_000;
}
