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
 * Hadoop {@link Configuration} keys for the cached-fs decorator, plus parsers that convert them into
 * the core library's typed config records.
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
