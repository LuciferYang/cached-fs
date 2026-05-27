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

  // --- scan tracking + IO stats (Phase 5a) ---------------------------------

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
   * {@code fs.cached.metrics.enabled} — master toggle for per-stream {@link
   * io.github.luciferyang.cachedfs.core.stats.IoStatistics}; default {@code true}. When false,
   * streams are constructed with {@code IoStatistics.NO_OP}.
   */
  public static final String METRICS_ENABLED = "fs.cached.metrics.enabled";

  public static final boolean DEFAULT_METRICS_ENABLED = true;

  // --- coalescing (Phase 5b) -----------------------------------------------

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

  // --- prefetch (Phase 5c) -------------------------------------------------

  /**
   * {@code fs.cached.prefetch.enabled} — master toggle for the Phase 5c async prefetch path.
   * Default {@code true}. When false, the prefetch executor is never built and the admission gate
   * is a no-op; consumers fall back to the synchronous per-chunk path.
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
