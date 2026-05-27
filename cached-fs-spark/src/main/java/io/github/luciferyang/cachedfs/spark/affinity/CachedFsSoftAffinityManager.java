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

import io.github.luciferyang.cachedfs.spark.hash.ConsistentHash;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver-side singleton that powers Spark soft-affinity scheduling for cached-fs. Two cooperating
 * modes:
 *
 * <ol>
 *   <li><b>Static (consistent-hash):</b> {@link #askExecutorsForPath(String, int)} hashes the file
 *       path against the ring and returns the top-N executors. Deterministic, no feedback, kicks in
 *       immediately on the first run. Suitable for whole-file co-location.
 *   <li><b>Feedback (duplicate-reading detect):</b> when {@link #setDetectDuplicateReading(boolean)
 *       enabled}, {@link #updateTaskEnd(int, int, String, String)} records the actual {@code
 *       (executor, host)} that ran each split (keyed by {@code "path_start_length"}). Subsequent
 *       calls to {@link #askExecutorsForSplit(java.util.List)} return the OBSERVED executor
 *       overriding the consistent-hash candidate.
 * </ol>
 *
 * <p>Executors are managed via {@link #handleExecutorAdded(String, String)} / {@link
 * #handleExecutorRemoved(String)} from a SparkListener — the manager is decoupled from Spark itself
 * so this class is unit-testable without a SparkContext.
 *
 * <p>Adapted from Apache Gluten's {@code SoftAffinityManager}. Differences:
 *
 * <ul>
 *   <li>Pure Java; no Scala collections in the public surface.
 *   <li>Separates {@code replicationNum} (top-N candidates per lookup) from {@code virtualNodes}
 *       (ring density). Gluten reuses one knob for both.
 *   <li>{@link SplitKey} for the feedback dimension instead of Tuple4/string concatenation.
 *   <li>{@link AffinitySnapshot} for callers that need an atomic, read-only view of state.
 * </ul>
 */
public final class CachedFsSoftAffinityManager {

  private static final Logger LOG = LoggerFactory.getLogger(CachedFsSoftAffinityManager.class);

  // Singleton holder. Tests reset via resetForTesting().
  private static volatile CachedFsSoftAffinityManager instance;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final ConsistentHash<ExecutorNode> ring;
  private final Map<String, ExecutorNode> executorById = new LinkedHashMap<>();
  // host -> set<executorId>; used to compute hostsKnown() for the minTargetHosts check.
  private final Map<String, java.util.Set<String>> executorsByHost = new LinkedHashMap<>();

  // Knobs (mutable; set by the extension/listener at startup from SparkConf).
  private volatile boolean enabled = CachedFsAffinityConfig.DEFAULT_ENABLED;
  private volatile int replicationNum = CachedFsAffinityConfig.DEFAULT_REPLICATION_NUM;
  private volatile int minTargetHosts = CachedFsAffinityConfig.DEFAULT_MIN_TARGET_HOSTS;
  private volatile boolean detectDuplicateReading =
      CachedFsAffinityConfig.DEFAULT_DUPLICATE_READING_DETECT_ENABLED;
  private volatile int duplicateReadingMaxCacheItems =
      CachedFsAffinityConfig.DEFAULT_DUPLICATE_READING_MAX_CACHE_ITEMS;

  // Feedback-mode state. All three are bounded LRU maps protected by the same lock so a single
  // stage-completion event can drop entries atomically across all three.
  // rddId -> [(partitionId, path, start, length), ...]
  private final BoundedLinkedHashMap<Integer, List<SplitKey>> rddPartitions =
      new BoundedLinkedHashMap<>(() -> duplicateReadingMaxCacheItems);
  // stageId -> [rddId, ...]
  private final BoundedLinkedHashMap<Integer, int[]> stageRdds =
      new BoundedLinkedHashMap<>(() -> duplicateReadingMaxCacheItems);
  // "path1_start1_length1,path2_start2_length2" (sorted) -> [(execId, host), ...]
  private final BoundedLinkedHashMap<String, List<ExecutorNode>> duplicateReadingObservations =
      new BoundedLinkedHashMap<>(() -> duplicateReadingMaxCacheItems);

  private CachedFsSoftAffinityManager(int virtualNodes) {
    this.ring = new ConsistentHash<>(virtualNodes);
  }

  public static CachedFsSoftAffinityManager getInstance() {
    CachedFsSoftAffinityManager local = instance;
    if (local == null) {
      synchronized (CachedFsSoftAffinityManager.class) {
        local = instance;
        if (local == null) {
          local = new CachedFsSoftAffinityManager(CachedFsAffinityConfig.DEFAULT_VIRTUAL_NODES);
          instance = local;
        }
      }
    }
    return local;
  }

  /** Test-only. Discards the singleton so the next {@code getInstance()} rebuilds the ring. */
  public static void resetForTesting() {
    synchronized (CachedFsSoftAffinityManager.class) {
      instance = null;
    }
  }

  // --- knob setters (called by the listener/extension at startup) --------

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setReplicationNum(int replicationNum) {
    if (replicationNum <= 0) {
      throw new IllegalArgumentException("replicationNum must be positive, got " + replicationNum);
    }
    this.replicationNum = replicationNum;
  }

  public int getReplicationNum() {
    return replicationNum;
  }

  public void setMinTargetHosts(int minTargetHosts) {
    if (minTargetHosts < 0) {
      throw new IllegalArgumentException(
          "minTargetHosts must be non-negative, got " + minTargetHosts);
    }
    this.minTargetHosts = minTargetHosts;
  }

  public int getMinTargetHosts() {
    return minTargetHosts;
  }

  public void setDetectDuplicateReading(boolean detectDuplicateReading) {
    this.detectDuplicateReading = detectDuplicateReading;
  }

  public boolean isDetectDuplicateReading() {
    return detectDuplicateReading;
  }

  public void setDuplicateReadingMaxCacheItems(int cap) {
    if (cap <= 0) {
      throw new IllegalArgumentException("max-cache-items must be positive, got " + cap);
    }
    this.duplicateReadingMaxCacheItems = cap;
  }

  // --- executor lifecycle ------------------------------------------------

  public void handleExecutorAdded(String executorId, String host) {
    if (executorId == null || host == null) return;
    lock.writeLock().lock();
    try {
      if (executorById.containsKey(executorId)) return;
      ExecutorNode node = new ExecutorNode(executorId, host);
      ring.addNode(node);
      executorById.put(executorId, node);
      executorsByHost.computeIfAbsent(host, h -> new java.util.LinkedHashSet<>()).add(executorId);
      LOG.info(
          "Executor added to affinity ring: {}@{} (ring size {})",
          executorId,
          host,
          ring.nodeCount());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void handleExecutorRemoved(String executorId) {
    if (executorId == null) return;
    lock.writeLock().lock();
    try {
      ExecutorNode node = executorById.remove(executorId);
      if (node == null) {
        LOG.debug("Executor {} not in affinity ring on removal", executorId);
        return;
      }
      ring.removeNode(node);
      java.util.Set<String> peers = executorsByHost.get(node.host());
      if (peers != null) {
        peers.remove(executorId);
        if (peers.isEmpty()) {
          executorsByHost.remove(node.host());
        }
      }
      LOG.info(
          "Executor removed from affinity ring: {}@{} (ring size {})",
          executorId,
          node.host(),
          ring.nodeCount());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public int executorCount() {
    lock.readLock().lock();
    try {
      return executorById.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  // --- static-mode lookup ------------------------------------------------

  /**
   * Returns the top {@code count} executors for {@code filePath} from the consistent-hash ring.
   * {@link #isEnabled() Disabled} or empty-ring cases return an empty list — callers should fall
   * back to native locality.
   *
   * @param count target number of preferred executors (clamped to the ring size)
   */
  public List<ExecutorNode> askExecutorsForPath(String filePath, int count) {
    if (!enabled || filePath == null || count <= 0) return Collections.emptyList();
    lock.readLock().lock();
    try {
      if (ring.nodeCount() == 0) return Collections.emptyList();
      return ring.allocateNodes(filePath, count);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the OBSERVED executors for a FilePartition's split key (feedback mode). Returns empty
   * when the mode is disabled or no observation has been recorded yet. Caller usually combines this
   * with {@link #askExecutorsForPath(String, int)} as a fallback.
   */
  public List<ExecutorNode> askExecutorsForSplit(List<SplitKey> splits) {
    if (!enabled || !detectDuplicateReading || splits == null || splits.isEmpty()) {
      return Collections.emptyList();
    }
    String key = composeKey(splits);
    lock.readLock().lock();
    try {
      List<ExecutorNode> observed = duplicateReadingObservations.get(key);
      if (observed == null || observed.isEmpty()) return Collections.emptyList();
      // Filter to live executors only — an entry can outlive its executor on long-running drivers.
      List<ExecutorNode> alive = new ArrayList<>(observed.size());
      for (ExecutorNode e : observed) {
        if (executorById.containsKey(e.executorId())) {
          alive.add(e);
        }
      }
      return alive;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns true if Spark's native locality already covers at least {@link #minTargetHosts} hosts
   * matching live executors — meaning soft-affinity should NOT override the native hint. When the
   * ring is empty the check is vacuously true (we have nothing useful to say).
   */
  public boolean shouldDeferToNativeLocality(String[] nativeHosts) {
    if (nativeHosts == null || nativeHosts.length == 0) return false;
    lock.readLock().lock();
    try {
      if (executorsByHost.isEmpty()) return true;
      int min = Math.min(minTargetHosts, nativeHosts.length);
      if (min <= 0) return false;
      int matched = 0;
      for (String h : nativeHosts) {
        if (executorsByHost.containsKey(h)) matched++;
        if (matched >= min) return true;
      }
      return false;
    } finally {
      lock.readLock().unlock();
    }
  }

  // --- feedback-mode listener entry points -------------------------------

  /**
   * Records the mapping {@code stageId -> rddIds} so a subsequent {@link #updateTaskEnd} can walk
   * up to the file-split partitions belonging to the RDDs in this stage.
   */
  public void updateStageSubmitted(int stageId, int[] rddIds) {
    if (!detectDuplicateReading || rddIds == null) return;
    lock.writeLock().lock();
    try {
      stageRdds.put(stageId, rddIds.clone());
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Records a single FilePartition's split fingerprints under its RDD id. */
  public void updatePartitionMap(int rddId, List<SplitKey> splits) {
    if (!detectDuplicateReading || splits == null || splits.isEmpty()) return;
    lock.writeLock().lock();
    try {
      List<SplitKey> existing = rddPartitions.get(rddId);
      if (existing == null) {
        rddPartitions.put(rddId, new ArrayList<>(splits));
      } else {
        existing.addAll(splits);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Records the actual {@code (executor, host)} that ran one task. Looks up the FilePartition keys
   * via the chain {@code stageId -> rddIds -> partitionId -> splits} so we can attribute the
   * placement back to the file-split fingerprint that drove the read.
   */
  public void updateTaskEnd(int stageId, int taskPartitionId, String executorId, String host) {
    if (!detectDuplicateReading || executorId == null || host == null) return;
    lock.writeLock().lock();
    try {
      int[] rddIds = stageRdds.get(stageId);
      if (rddIds == null) return;
      for (int rddId : rddIds) {
        List<SplitKey> all = rddPartitions.get(rddId);
        if (all == null) continue;
        List<SplitKey> belonging = new ArrayList<>();
        for (SplitKey s : all) {
          if (s.partitionId() == taskPartitionId) belonging.add(s);
        }
        if (belonging.isEmpty()) continue;
        String key = composeKey(belonging);
        List<ExecutorNode> prior = duplicateReadingObservations.get(key);
        ExecutorNode obs = new ExecutorNode(executorId, host);
        List<ExecutorNode> next = prior == null ? new ArrayList<>(1) : new ArrayList<>(prior);
        if (!next.contains(obs)) next.add(obs);
        duplicateReadingObservations.put(key, next);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Per-stage cleanup hook. Drops cached split / rdd state for the completed stage. */
  public void cleanMiddleStatusMap(int stageId, int[] rddIds) {
    if (!detectDuplicateReading) return;
    lock.writeLock().lock();
    try {
      stageRdds.remove(stageId);
      if (rddIds != null) {
        for (int id : rddIds) {
          rddPartitions.remove(id);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  // --- read-only snapshot ------------------------------------------------

  /** Atomic, read-only snapshot of the ring + feedback state. For metrics + tests. */
  public AffinitySnapshot snapshot() {
    lock.readLock().lock();
    try {
      return new AffinitySnapshot(
          enabled,
          replicationNum,
          minTargetHosts,
          detectDuplicateReading,
          executorById.size(),
          executorsByHost.size(),
          duplicateReadingObservations.size());
    } finally {
      lock.readLock().unlock();
    }
  }

  // --- helpers -----------------------------------------------------------

  static String composeKey(List<SplitKey> splits) {
    List<String> parts = new ArrayList<>(splits.size());
    for (SplitKey s : splits) {
      parts.add(s.path() + "_" + s.start() + "_" + s.length());
    }
    Collections.sort(parts);
    return String.join(",", parts);
  }

  /** One file-split fingerprint inside a FilePartition. */
  public record SplitKey(int partitionId, String path, long start, long length) {
    public SplitKey {
      java.util.Objects.requireNonNull(path, "path");
    }
  }

  /** Immutable counters/state useful for metrics + tests. */
  public record AffinitySnapshot(
      boolean enabled,
      int replicationNum,
      int minTargetHosts,
      boolean detectDuplicateReading,
      int executorCount,
      int hostCount,
      int duplicateReadingEntries) {}

  /**
   * Tiny bounded LRU map. Guava is intentionally NOT pulled in here — the cap is rarely hit (we get
   * proactive cleanup on stage-completion) and a static one-class LRU costs less than a Guava
   * cache's class-loading overhead in a Spark driver where this code runs once per JVM.
   */
  private static final class BoundedLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
    private final java.util.function.IntSupplier capSupplier;

    BoundedLinkedHashMap(java.util.function.IntSupplier capSupplier) {
      super(16, 0.75f, /* accessOrder= */ true);
      this.capSupplier = capSupplier;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() > capSupplier.getAsInt();
    }
  }

  /** Read-only view of the executor set (sorted by add-order). Test + diagnostics helper. */
  public List<ExecutorNode> executorsForTesting() {
    lock.readLock().lock();
    try {
      return new ArrayList<>(executorById.values());
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Reset to an empty state without rebuilding the singleton. Test helper. */
  public void resetStateForTesting() {
    lock.writeLock().lock();
    try {
      // Re-build the ring fresh so virtualNodes changes also take effect.
      for (ExecutorNode n : new ArrayList<>(executorById.values())) {
        ring.removeNode(n);
      }
      executorById.clear();
      executorsByHost.clear();
      rddPartitions.clear();
      stageRdds.clear();
      duplicateReadingObservations.clear();
      enabled = CachedFsAffinityConfig.DEFAULT_ENABLED;
      replicationNum = CachedFsAffinityConfig.DEFAULT_REPLICATION_NUM;
      minTargetHosts = CachedFsAffinityConfig.DEFAULT_MIN_TARGET_HOSTS;
      detectDuplicateReading = CachedFsAffinityConfig.DEFAULT_DUPLICATE_READING_DETECT_ENABLED;
      duplicateReadingMaxCacheItems =
          CachedFsAffinityConfig.DEFAULT_DUPLICATE_READING_MAX_CACHE_ITEMS;
    } finally {
      lock.writeLock().unlock();
    }
  }

  // Defensive: keep array clones from leaking. unused-but-kept for clarity.
  private static int[] copy(int[] in) {
    return Arrays.copyOf(in, in.length);
  }
}
