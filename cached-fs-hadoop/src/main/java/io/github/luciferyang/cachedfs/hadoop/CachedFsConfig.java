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

import io.github.luciferyang.cachedfs.core.AsyncDataCache;
import io.github.luciferyang.cachedfs.core.ssd.SsdCache;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/**
 * Hadoop {@link Configuration} keys for the cached-fs decorator, plus parsers that convert them
 * into the core library's typed config records.
 *
 * <p>All keys are flat namespace-scoped under {@code fs.cached.*} so a single deployment can enable
 * caching globally via {@code fs.cached.enabled=true} without per-scheme plumbing.
 */
public final class CachedFsConfig {

  private CachedFsConfig() {}

  // --- master switch -------------------------------------------------------

  /** {@code fs.cached.enabled} — master toggle; when false the decorator is a pass-through. */
  public static final String ENABLED = "fs.cached.enabled";

  public static final boolean DEFAULT_ENABLED = false;

  /**
   * {@code fs.cached.inner.impl} — fully-qualified class name of the inner {@link
   * org.apache.hadoop.fs.FileSystem} the decorator wraps. Required when the decorator replaces a
   * scheme's default impl (e.g. {@code fs.s3a.impl=CachedFileSystem} + {@code
   * fs.cached.inner.impl=org.apache.hadoop.fs.s3a.S3AFileSystem}).
   */
  public static final String INNER_IMPL = "fs.cached.inner.impl";

  // --- RAM cache -----------------------------------------------------------

  /** {@code fs.cached.ram.shards} — power-of-two count, default 4. */
  public static final String RAM_SHARDS = "fs.cached.ram.shards";

  public static final int DEFAULT_RAM_SHARDS = AsyncDataCache.DEFAULT_NUM_SHARDS;

  /** {@code fs.cached.ram.max-write-ratio} — fraction (0,1], default 0.7. */
  public static final String RAM_MAX_WRITE_RATIO = "fs.cached.ram.max-write-ratio";

  public static final double DEFAULT_RAM_MAX_WRITE_RATIO = 0.7;

  /** {@code fs.cached.ram.ssd-savable-ratio} — fraction, default 0.125. */
  public static final String RAM_SSD_SAVABLE_RATIO = "fs.cached.ram.ssd-savable-ratio";

  public static final double DEFAULT_RAM_SSD_SAVABLE_RATIO = 0.125;

  /** {@code fs.cached.ram.min-ssd-savable-bytes} — default 16 MiB. */
  public static final String RAM_MIN_SSD_SAVABLE_BYTES = "fs.cached.ram.min-ssd-savable-bytes";

  public static final long DEFAULT_RAM_MIN_SSD_SAVABLE_BYTES = 16L << 20;

  /** {@code fs.cached.ram.ssd-flush-threshold-bytes} — default 0 (disabled). */
  public static final String RAM_SSD_FLUSH_THRESHOLD_BYTES =
      "fs.cached.ram.ssd-flush-threshold-bytes";

  public static final long DEFAULT_RAM_SSD_FLUSH_THRESHOLD_BYTES = 0L;

  // --- SSD cache -----------------------------------------------------------

  /**
   * {@code fs.cached.ssd.paths} — comma-separated list of writable directories. Empty disables the
   * SSD tier; otherwise every listed directory must exist and be writable at open time (fail-fast).
   */
  public static final String SSD_PATHS = "fs.cached.ssd.paths";

  /** {@code fs.cached.ssd.shards} — default 4. */
  public static final String SSD_SHARDS = "fs.cached.ssd.shards";

  public static final int DEFAULT_SSD_SHARDS = 4;

  /** {@code fs.cached.ssd.shard-prefix} — filename prefix per shard, default "ssd". */
  public static final String SSD_SHARD_PREFIX = "fs.cached.ssd.shard-prefix";

  public static final String DEFAULT_SSD_SHARD_PREFIX = "ssd";

  /** {@code fs.cached.ssd.regions-per-shard} — 64 MiB regions, default 16 (1 GiB per shard). */
  public static final String SSD_REGIONS_PER_SHARD = "fs.cached.ssd.regions-per-shard";

  public static final int DEFAULT_SSD_REGIONS_PER_SHARD = 16;

  /** {@code fs.cached.ssd.max-entries-per-shard} — 0 means unbounded. */
  public static final String SSD_MAX_ENTRIES_PER_SHARD = "fs.cached.ssd.max-entries-per-shard";

  public static final int DEFAULT_SSD_MAX_ENTRIES_PER_SHARD = 0;

  /** {@code fs.cached.ssd.checkpoint-interval-bytes} — cache-wide; 0 disables checkpointing. */
  public static final String SSD_CHECKPOINT_INTERVAL_BYTES =
      "fs.cached.ssd.checkpoint-interval-bytes";

  public static final long DEFAULT_SSD_CHECKPOINT_INTERVAL_BYTES = 0L;

  /** {@code fs.cached.ssd.checksum.enabled} — per-entry CRC32C, default false. */
  public static final String SSD_CHECKSUM_ENABLED = "fs.cached.ssd.checksum.enabled";

  public static final boolean DEFAULT_SSD_CHECKSUM_ENABLED = false;

  /** {@code fs.cached.ssd.checksum.read-verify} — verify on read, requires checksum.enabled. */
  public static final String SSD_CHECKSUM_READ_VERIFY = "fs.cached.ssd.checksum.read-verify";

  public static final boolean DEFAULT_SSD_CHECKSUM_READ_VERIFY = false;

  // --- read path -----------------------------------------------------------

  /** {@code fs.cached.load-quantum-bytes} — cache chunk size, default 8 MiB (velox parity). */
  public static final String LOAD_QUANTUM_BYTES = "fs.cached.load-quantum-bytes";

  public static final int DEFAULT_LOAD_QUANTUM_BYTES = 8 << 20;

  /** {@code fs.cached.handle-cache-capacity} — open-handle LRU size, default 1024. */
  public static final String HANDLE_CACHE_CAPACITY = "fs.cached.handle-cache-capacity";

  public static final int DEFAULT_HANDLE_CACHE_CAPACITY = 1024;

  // --- scan tracking + IO stats ---------------------------------

  /**
   * {@code fs.cached.scan-id} — per-file scan identifier. Resolved at {@link
   * org.apache.hadoop.fs.FileSystem#open} via the precedence in {@link
   * CacheBootstrap#currentScanId()} → this conf key → {@code "default"}.
   */
  public static final String SCAN_ID = "fs.cached.scan-id";

  /**
   * {@code fs.cached.scan-tracker.enabled} — master toggle for the per-scan {@code ScanTracker};
   * default {@code true}. When false, {@link CacheBootstrap#trackerFor} returns {@code
   * ScanTracker.DISABLED} and all density-tracking is a no-op.
   */
  public static final String SCAN_TRACKER_ENABLED = "fs.cached.scan-tracker.enabled";

  public static final boolean DEFAULT_SCAN_TRACKER_ENABLED = true;

  /**
   * {@code fs.cached.scan-tracker.max-entries-per-tracker} — soft cap on the distinct fileNums one
   * {@link io.github.luciferyang.cachedfs.core.tracker.ScanTracker} will admit before silently
   * rejecting new entries. {@code 0} (or negative) disables the cap; default {@code 10_000} matches
   * the threshold called out in reader-glue follow-up R3. Sized so a single ~50k-file partitioned
   * scan exposes the cap (observable via {@link CacheBootstrap#scanTrackerEntriesRejected()}) and
   * operators choose between widening the cap and splitting the scan.
   */
  public static final String SCAN_TRACKER_MAX_ENTRIES_PER_TRACKER =
      "fs.cached.scan-tracker.max-entries-per-tracker";

  public static final int DEFAULT_SCAN_TRACKER_MAX_ENTRIES_PER_TRACKER = 10_000;

  /**
   * {@code fs.cached.scan-trackers.max-count} — soft cap on the number of distinct {@link
   * io.github.luciferyang.cachedfs.core.tracker.ScanTracker}s {@link CacheBootstrap} will retain.
   * Once the cap is hit, {@link CacheBootstrap#trackerFor(String)} returns {@code
   * ScanTracker.DISABLED} for new scanIds and bumps {@link CacheBootstrap#scanTrackersRejected()};
   * existing trackers keep tracking normally. {@code 0} (or negative) disables the cap.
   *
   * <p>With M2.1 wiring per-task scanIds, leaked trackers (e.g. from crashed tasks before the
   * plugin's onTaskSucceeded fires) accumulate; the cap is the safety net that prevents unbounded
   * growth on a long-lived executor.
   */
  public static final String SCAN_TRACKERS_MAX_COUNT = "fs.cached.scan-trackers.max-count";

  public static final int DEFAULT_SCAN_TRACKERS_MAX_COUNT = 10_000;

  /**
   * {@code fs.cached.recent-streams.capacity} — capacity of the ring buffer that retains per-stream
   * {@link io.github.luciferyang.cachedfs.core.stats.IoStatisticsSnapshot}s for post-hoc debugging.
   * Each closed stream pushes its snapshot into the ring; the oldest snapshot is overwritten when
   * the ring wraps. {@code 0} disables the ring (snapshots are dropped on the floor).
   */
  public static final String RECENT_STREAMS_CAPACITY = "fs.cached.recent-streams.capacity";

  public static final int DEFAULT_RECENT_STREAMS_CAPACITY = 64;

  /**
   * {@code fs.cached.metrics.enabled} — master toggle for per-stream {@link
   * io.github.luciferyang.cachedfs.core.stats.IoStatistics}; default {@code true}. When false,
   * streams are constructed with {@code IoStatistics.NO_OP}.
   */
  public static final String METRICS_ENABLED = "fs.cached.metrics.enabled";

  public static final boolean DEFAULT_METRICS_ENABLED = true;

  // --- coalescing -----------------------------------------------

  /**
   * {@code fs.cached.coalesce.enabled} — master toggle for multi-chunk coalescing. Default {@code
   * true}, but auto-disabled when the auto-scaled cap (see {@link #COALESCE_MAX_CHUNKS_PER_GROUP})
   * reaches 2 AND {@link #COALESCE_ALWAYS_ON} is false — small caches (under ~384 MiB at the
   * default load quantum) gain nothing from coalescing and pay non-trivial per-call overhead, so
   * the default flips off automatically.
   */
  public static final String COALESCE_ENABLED = "fs.cached.coalesce.enabled";

  public static final boolean DEFAULT_COALESCE_ENABLED = true;

  /**
   * {@code fs.cached.coalesce.always-on} — when true, ignores the auto-disable rule on small caches
   * and respects {@link #COALESCE_ENABLED} verbatim. Use only for benchmarking on intentionally
   * under-sized caches.
   */
  public static final String COALESCE_ALWAYS_ON = "fs.cached.coalesce.always-on";

  public static final boolean DEFAULT_COALESCE_ALWAYS_ON = false;

  /**
   * {@code fs.cached.coalesce.max-gap-bytes} — maximum byte gap absorbed when grouping consecutive
   * Exclusive chunks for a single {@code preadv}. Default {@code min(512 KiB, loadQuantumBytes /
   * 16)} — scales down on operators tuning the load quantum small.
   */
  public static final String COALESCE_MAX_GAP_BYTES = "fs.cached.coalesce.max-gap-bytes";

  /**
   * {@code fs.cached.coalesce.max-chunks-per-group} — maximum chunks per coalesced {@code preadv}
   * call. Default {@code max(2, min(16, totalRamBytes / loadQuantumBytes / 16))}.
   */
  public static final String COALESCE_MAX_CHUNKS_PER_GROUP =
      "fs.cached.coalesce.max-chunks-per-group";

  /**
   * {@code fs.cached.coalesce.max-restarts} — bound on the abort-and-restart loop when a {@link
   * io.github.luciferyang.cachedfs.core.FindResult.Waiting Waiting} chunk forces the coalescer to
   * release its pins and retry. Default {@code 3}; on exceeding, the read falls back to the
   * per-chunk path and completes correctly without coalescing.
   */
  public static final String COALESCE_MAX_RESTARTS = "fs.cached.coalesce.max-restarts";

  public static final int DEFAULT_COALESCE_MAX_RESTARTS = 3;

  // --- prefetch -------------------------------------------------

  /**
   * {@code fs.cached.prefetch.enabled} — master toggle for the async prefetch path. Default {@code
   * true}. When false, the prefetch executor is never built and the admission gate is a no-op;
   * consumers fall back to the synchronous per-chunk path.
   */
  public static final String PREFETCH_ENABLED = "fs.cached.prefetch.enabled";

  public static final boolean DEFAULT_PREFETCH_ENABLED = true;

  /**
   * {@code fs.cached.prefetch.threads} — fixed-size prefetch thread pool. Default {@code
   * Runtime.getRuntime().availableProcessors()}.
   */
  public static final String PREFETCH_THREADS = "fs.cached.prefetch.threads";

  /**
   * {@code fs.cached.prefetch.queue} — bounded {@code ArrayBlockingQueue} capacity backing the
   * prefetch executor. Default {@code 64}. Sized for backpressure, not throughput — {@link
   * DiscardAndCountHandler} is the steady-state safety valve.
   */
  public static final String PREFETCH_QUEUE = "fs.cached.prefetch.queue";

  public static final int DEFAULT_PREFETCH_QUEUE = 64;

  /**
   * {@code fs.cached.prefetch.heap-pressure-check.enabled} — gate the admission predicate on the
   * heap-pressure check. Default {@code true}. When false the admission gate ignores heap pressure
   * entirely (still gated by the byte budget).
   */
  public static final String PREFETCH_HEAP_PRESSURE_CHECK_ENABLED =
      "fs.cached.prefetch.heap-pressure-check.enabled";

  public static final boolean DEFAULT_PREFETCH_HEAP_PRESSURE_CHECK_ENABLED = true;

  /**
   * {@code fs.cached.prefetch.heap-pressure-ttl-ms} — refresh interval for the cached {@link
   * java.lang.management.MemoryMXBean MemoryMXBean} heap-pressure read. Default 100 ms. One MBean
   * call per TTL window across the entire JVM (single-CAS-winner pattern).
   */
  public static final String PREFETCH_HEAP_PRESSURE_TTL_MS =
      "fs.cached.prefetch.heap-pressure-ttl-ms";

  public static final long DEFAULT_PREFETCH_HEAP_PRESSURE_TTL_MS = 100L;

  /**
   * {@code fs.cached.prefetch.rejection-backoff-ms} — per-stream backoff after a queue-full
   * rejection before the admission gate will re-attempt a prefetch submit. Default 100 ms.
   */
  public static final String PREFETCH_REJECTION_BACKOFF_MS =
      "fs.cached.prefetch.rejection-backoff-ms";

  public static final long DEFAULT_PREFETCH_REJECTION_BACKOFF_MS = 100L;

  /**
   * {@code fs.cached.prefetch.trigger-tail-fraction} — fraction of the way into the current chunk
   * at which the admission gate considers the read "near the end" and submits a prefetch for the
   * next chunk. Default 0.5 (chunk midpoint). Higher values delay the trigger; lower values fire
   * earlier and over-prefetch on consumers that abandon reads.
   */
  public static final String PREFETCH_TRIGGER_TAIL_FRACTION =
      "fs.cached.prefetch.trigger-tail-fraction";

  public static final double DEFAULT_PREFETCH_TRIGGER_TAIL_FRACTION = 0.5;

  /**
   * {@code fs.cached.prefetch.density-threshold-pct} — minimum {@code readPct} (0-100) required
   * before the admission gate's density predicate will pass. Default 80 — streams that have
   * actually read 80%+ of their referenced bytes are deemed dense enough to benefit from
   * speculative prefetch.
   */
  public static final String PREFETCH_DENSITY_THRESHOLD_PCT =
      "fs.cached.prefetch.density-threshold-pct";

  public static final int DEFAULT_PREFETCH_DENSITY_THRESHOLD_PCT = 80;

  /**
   * {@code fs.cached.prefetch.max-pending-bytes} — JVM-wide byte budget for in-flight prefetches.
   * Default {@code loadQuantum * prefetchThreads * 4} (computed via {@link
   * #defaultPrefetchMaxPendingBytes}). Operators with tight RAM budgets should lower this.
   */
  public static final String PREFETCH_MAX_PENDING_BYTES = "fs.cached.prefetch.max-pending-bytes";

  /**
   * {@code fs.cached.prefetch.max-pending-multiplier} — the {@code N} in {@code defaultPrefetchMax
   * PendingBytes(loadQuantum, threads) = loadQuantum × threads × N}. Default {@code 4}: each
   * prefetch thread is allowed 4 chunks queued before back-pressure kicks in. The constant {@code
   * 4} is a heuristic; the right value depends on workload (read pattern, downstream consumer cost,
   * storage latency). Operators can override this without touching cached-fs source. Ignored when
   * {@link #PREFETCH_MAX_PENDING_BYTES} is set explicitly.
   *
   * <p>R1.1 (deferred): build a JMH benchmark harness, measure throughput across multipliers on a
   * representative workload, and update the default if a better number is found.
   */
  public static final String PREFETCH_MAX_PENDING_MULTIPLIER =
      "fs.cached.prefetch.max-pending-multiplier";

  public static final int DEFAULT_PREFETCH_MAX_PENDING_MULTIPLIER = 4;

  // --- parsers -------------------------------------------------------------

  /** True if {@link #ENABLED} is set to {@code true}. */
  public static boolean isEnabled(Configuration conf) {
    return conf.getBoolean(ENABLED, DEFAULT_ENABLED);
  }

  /** Parses RAM-tier knobs. Validates power-of-two shards and ratio bounds via the record. */
  public static AsyncDataCache.Options ramOptions(Configuration conf) {
    return new AsyncDataCache.Options(
        conf.getInt(RAM_SHARDS, DEFAULT_RAM_SHARDS),
        conf.getDouble(RAM_MAX_WRITE_RATIO, DEFAULT_RAM_MAX_WRITE_RATIO),
        conf.getDouble(RAM_SSD_SAVABLE_RATIO, DEFAULT_RAM_SSD_SAVABLE_RATIO),
        conf.getLong(RAM_MIN_SSD_SAVABLE_BYTES, DEFAULT_RAM_MIN_SSD_SAVABLE_BYTES),
        conf.getLong(RAM_SSD_FLUSH_THRESHOLD_BYTES, DEFAULT_RAM_SSD_FLUSH_THRESHOLD_BYTES));
  }

  /**
   * Parses SSD-tier knobs. Returns {@code null} when {@link #SSD_PATHS} is empty or unset — the
   * caller should interpret that as "no SSD tier" and skip {@link SsdCache} construction.
   */
  public static SsdCache.Config ssdConfig(Configuration conf) {
    List<Path> paths = parsePaths(conf.getTrimmed(SSD_PATHS, ""));
    if (paths.isEmpty()) {
      return null;
    }
    return new SsdCache.Config(
        paths,
        conf.getTrimmed(SSD_SHARD_PREFIX, DEFAULT_SSD_SHARD_PREFIX),
        conf.getInt(SSD_SHARDS, DEFAULT_SSD_SHARDS),
        conf.getInt(SSD_REGIONS_PER_SHARD, DEFAULT_SSD_REGIONS_PER_SHARD),
        conf.getInt(SSD_MAX_ENTRIES_PER_SHARD, DEFAULT_SSD_MAX_ENTRIES_PER_SHARD),
        conf.getLong(SSD_CHECKPOINT_INTERVAL_BYTES, DEFAULT_SSD_CHECKPOINT_INTERVAL_BYTES),
        conf.getBoolean(SSD_CHECKSUM_ENABLED, DEFAULT_SSD_CHECKSUM_ENABLED),
        conf.getBoolean(SSD_CHECKSUM_READ_VERIFY, DEFAULT_SSD_CHECKSUM_READ_VERIFY));
  }

  public static int loadQuantumBytes(Configuration conf) {
    int v = conf.getInt(LOAD_QUANTUM_BYTES, DEFAULT_LOAD_QUANTUM_BYTES);
    if (v <= 0) {
      throw new IllegalArgumentException(LOAD_QUANTUM_BYTES + " must be > 0: " + v);
    }
    return v;
  }

  public static int scanTrackerMaxEntriesPerTracker(Configuration conf) {
    int v =
        conf.getInt(
            SCAN_TRACKER_MAX_ENTRIES_PER_TRACKER, DEFAULT_SCAN_TRACKER_MAX_ENTRIES_PER_TRACKER);
    return Math.max(0, v); // negative → unlimited; clamp at 0 to be defensive
  }

  public static int scanTrackersMaxCount(Configuration conf) {
    int v = conf.getInt(SCAN_TRACKERS_MAX_COUNT, DEFAULT_SCAN_TRACKERS_MAX_COUNT);
    return Math.max(0, v); // negative → unlimited
  }

  public static int recentStreamsCapacity(Configuration conf) {
    int v = conf.getInt(RECENT_STREAMS_CAPACITY, DEFAULT_RECENT_STREAMS_CAPACITY);
    return Math.max(0, v); // negative → disabled
  }

  public static boolean scanTrackerEnabled(Configuration conf) {
    return conf.getBoolean(SCAN_TRACKER_ENABLED, DEFAULT_SCAN_TRACKER_ENABLED);
  }

  public static boolean metricsEnabled(Configuration conf) {
    return conf.getBoolean(METRICS_ENABLED, DEFAULT_METRICS_ENABLED);
  }

  // --- coalescing parsers --------------------------------------------------

  /**
   * Auto-scaled default cap on chunks-per-group: {@code max(2, min(16, totalRamBytes /
   * loadQuantumBytes / 16))}. Operators with caches below ~384 MiB get 2; ~2 GiB gets 16.
   */
  public static int defaultCoalesceMaxChunksPerGroup(long totalRamBytes, int loadQuantumBytes) {
    long scaled = totalRamBytes / (long) loadQuantumBytes / 16L;
    int clamped = (int) Math.min(16L, Math.max(2L, scaled));
    return clamped;
  }

  public static int coalesceMaxChunksPerGroup(
      Configuration conf, long totalRamBytes, int loadQuantumBytes) {
    int autoDefault = defaultCoalesceMaxChunksPerGroup(totalRamBytes, loadQuantumBytes);
    int v = conf.getInt(COALESCE_MAX_CHUNKS_PER_GROUP, autoDefault);
    if (v < 1) {
      throw new IllegalArgumentException(COALESCE_MAX_CHUNKS_PER_GROUP + " must be >= 1: " + v);
    }
    return v;
  }

  /** Auto-scaled default max-gap: {@code min(512 KiB, loadQuantumBytes / 16)}. */
  public static int defaultCoalesceMaxGapBytes(int loadQuantumBytes) {
    return (int) Math.min(512L << 10, loadQuantumBytes / 16L);
  }

  public static int coalesceMaxGapBytes(Configuration conf, int loadQuantumBytes) {
    int v = conf.getInt(COALESCE_MAX_GAP_BYTES, defaultCoalesceMaxGapBytes(loadQuantumBytes));
    if (v < 0) {
      throw new IllegalArgumentException(COALESCE_MAX_GAP_BYTES + " must be >= 0: " + v);
    }
    return v;
  }

  public static int coalesceMaxRestarts(Configuration conf) {
    int v = conf.getInt(COALESCE_MAX_RESTARTS, DEFAULT_COALESCE_MAX_RESTARTS);
    if (v < 0) {
      throw new IllegalArgumentException(COALESCE_MAX_RESTARTS + " must be >= 0: " + v);
    }
    return v;
  }

  /**
   * Resolves the effective coalesce-enabled value: the master toggle gated by the small-cache
   * auto-disable rule (cap==2 AND always-on=false → disabled).
   */
  public static boolean coalesceEnabled(
      Configuration conf, long totalRamBytes, int loadQuantumBytes) {
    if (!conf.getBoolean(COALESCE_ENABLED, DEFAULT_COALESCE_ENABLED)) {
      return false;
    }
    if (conf.getBoolean(COALESCE_ALWAYS_ON, DEFAULT_COALESCE_ALWAYS_ON)) {
      return true;
    }
    int cap = coalesceMaxChunksPerGroup(conf, totalRamBytes, loadQuantumBytes);
    return cap > 2;
  }

  // --- prefetch parsers ----------------------------------------------------

  public static boolean prefetchEnabled(Configuration conf) {
    return conf.getBoolean(PREFETCH_ENABLED, DEFAULT_PREFETCH_ENABLED);
  }

  public static int prefetchThreads(Configuration conf) {
    int v = conf.getInt(PREFETCH_THREADS, Runtime.getRuntime().availableProcessors());
    if (v <= 0) {
      throw new IllegalArgumentException(PREFETCH_THREADS + " must be > 0: " + v);
    }
    return v;
  }

  public static int prefetchQueue(Configuration conf) {
    int v = conf.getInt(PREFETCH_QUEUE, DEFAULT_PREFETCH_QUEUE);
    if (v <= 0) {
      throw new IllegalArgumentException(PREFETCH_QUEUE + " must be > 0: " + v);
    }
    return v;
  }

  public static boolean prefetchHeapPressureCheckEnabled(Configuration conf) {
    return conf.getBoolean(
        PREFETCH_HEAP_PRESSURE_CHECK_ENABLED, DEFAULT_PREFETCH_HEAP_PRESSURE_CHECK_ENABLED);
  }

  public static long prefetchHeapPressureTtlMs(Configuration conf) {
    long v = conf.getLong(PREFETCH_HEAP_PRESSURE_TTL_MS, DEFAULT_PREFETCH_HEAP_PRESSURE_TTL_MS);
    if (v <= 0L) {
      throw new IllegalArgumentException(PREFETCH_HEAP_PRESSURE_TTL_MS + " must be > 0: " + v);
    }
    return v;
  }

  public static long prefetchRejectionBackoffMs(Configuration conf) {
    long v = conf.getLong(PREFETCH_REJECTION_BACKOFF_MS, DEFAULT_PREFETCH_REJECTION_BACKOFF_MS);
    if (v < 0L) {
      throw new IllegalArgumentException(PREFETCH_REJECTION_BACKOFF_MS + " must be >= 0: " + v);
    }
    return v;
  }

  public static double prefetchTriggerTailFraction(Configuration conf) {
    double v =
        conf.getDouble(PREFETCH_TRIGGER_TAIL_FRACTION, DEFAULT_PREFETCH_TRIGGER_TAIL_FRACTION);
    if (v <= 0.0 || v > 1.0) {
      throw new IllegalArgumentException(
          PREFETCH_TRIGGER_TAIL_FRACTION + " must be in (0, 1]: " + v);
    }
    return v;
  }

  public static int prefetchDensityThresholdPct(Configuration conf) {
    int v = conf.getInt(PREFETCH_DENSITY_THRESHOLD_PCT, DEFAULT_PREFETCH_DENSITY_THRESHOLD_PCT);
    if (v < 0 || v > 100) {
      throw new IllegalArgumentException(
          PREFETCH_DENSITY_THRESHOLD_PCT + " must be in [0, 100]: " + v);
    }
    return v;
  }

  /**
   * Default {@code loadQuantum × threads × 4}: a heuristic that gives each prefetch thread enough
   * in-flight budget to keep 4 chunks queued before back-pressure kicks in.
   */
  public static long defaultPrefetchMaxPendingBytes(int loadQuantumBytes, int prefetchThreads) {
    return defaultPrefetchMaxPendingBytes(
        loadQuantumBytes, prefetchThreads, DEFAULT_PREFETCH_MAX_PENDING_MULTIPLIER);
  }

  /** Multiplier-aware overload — the auto-default that {@link #prefetchMaxPendingBytes} uses. */
  public static long defaultPrefetchMaxPendingBytes(
      int loadQuantumBytes, int prefetchThreads, int multiplier) {
    return (long) loadQuantumBytes * prefetchThreads * (long) multiplier;
  }

  public static int prefetchMaxPendingMultiplier(Configuration conf) {
    int v = conf.getInt(PREFETCH_MAX_PENDING_MULTIPLIER, DEFAULT_PREFETCH_MAX_PENDING_MULTIPLIER);
    if (v <= 0) {
      throw new IllegalArgumentException(PREFETCH_MAX_PENDING_MULTIPLIER + " must be > 0: " + v);
    }
    return v;
  }

  public static long prefetchMaxPendingBytes(
      Configuration conf, int loadQuantumBytes, int prefetchThreads) {
    int multiplier = prefetchMaxPendingMultiplier(conf);
    long autoDefault =
        defaultPrefetchMaxPendingBytes(loadQuantumBytes, prefetchThreads, multiplier);
    long v = conf.getLong(PREFETCH_MAX_PENDING_BYTES, autoDefault);
    if (v <= 0L) {
      throw new IllegalArgumentException(PREFETCH_MAX_PENDING_BYTES + " must be > 0: " + v);
    }
    return v;
  }

  public static int handleCacheCapacity(Configuration conf) {
    int v = conf.getInt(HANDLE_CACHE_CAPACITY, DEFAULT_HANDLE_CACHE_CAPACITY);
    if (v <= 0) {
      throw new IllegalArgumentException(HANDLE_CACHE_CAPACITY + " must be > 0: " + v);
    }
    return v;
  }

  static List<Path> parsePaths(String csv) {
    if (csv == null || csv.isBlank()) {
      return List.of();
    }
    List<Path> out = new ArrayList<>();
    for (String token : csv.split(",")) {
      String trimmed = token.trim();
      if (!trimmed.isEmpty()) {
        out.add(Paths.get(trimmed));
      }
    }
    return List.copyOf(out);
  }
}
