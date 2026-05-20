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
package io.github.luciferyang.cachedfs.core;

import io.github.luciferyang.cachedfs.core.stats.CacheStats;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Process-wide RAM cache. Mirrors velox {@code AsyncDataCache} for the RAM tier; the SSD tier ships
 * in Phase 2.
 *
 * <p>Sharded by {@code key.hashCode() & shardMask}. Default 4 shards (must be a power of 2). Each
 * shard owns its own mutex, entries, and counters. The cache itself is essentially a thin router
 * that picks the right shard for each request.
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls.
 */
public final class AsyncDataCache implements AutoCloseable {

  /** Velox: {@code kDefaultNumShards = 4}. */
  public static final int DEFAULT_NUM_SHARDS = 4;

  /** Configuration knobs. Mirror velox {@code AsyncDataCache::Options}. */
  public record Options(
      int numShards,
      double maxWriteRatio,
      double ssdSavableRatio,
      long minSsdSavableBytes,
      long ssdFlushThresholdBytes) {

    public Options {
      if (numShards <= 0 || (numShards & (numShards - 1)) != 0) {
        throw new IllegalArgumentException("numShards must be a power of 2: " + numShards);
      }
      if (maxWriteRatio < 0.0 || maxWriteRatio > 1.0) {
        throw new IllegalArgumentException("maxWriteRatio must be in [0,1]: " + maxWriteRatio);
      }
    }

    public static Options defaults() {
      return new Options(DEFAULT_NUM_SHARDS, 0.7, 0.125, 16L << 20, 0L);
    }
  }

  private static final AtomicReference<AsyncDataCache> INSTANCE = new AtomicReference<>();

  /** Returns the process-wide cache, or empty if none has been installed. */
  public static Optional<AsyncDataCache> getInstance() {
    return Optional.ofNullable(INSTANCE.get());
  }

  /**
   * Installs the process-wide cache. Rejects {@code null}; use {@link #clearInstance()} to
   * uninstall. The caller is responsible for ensuring no live cache entries reference the previous
   * instance before calling this.
   */
  public static void setInstance(AsyncDataCache cache) {
    Objects.requireNonNull(cache, "cache");
    INSTANCE.set(cache);
  }

  /** Uninstalls the process-wide cache. Idempotent; matches the {@link #setInstance} pair. */
  public static void clearInstance() {
    INSTANCE.set(null);
  }

  private final Options options;
  private final int shardMask;
  private final CacheShard[] shards;
  private final AtomicBoolean closed = new AtomicBoolean();

  public AsyncDataCache(Options options) {
    this.options = options;
    this.shardMask = options.numShards() - 1;
    this.shards = new CacheShard[options.numShards()];
    for (int i = 0; i < shards.length; i++) {
      shards[i] = new CacheShard(this);
    }
  }

  public Options options() {
    return options;
  }

  public int numShards() {
    return shards.length;
  }

  /** Routes a key to its shard. Mirrors velox sharding by full {@code (fileNum, offset)} hash. */
  public CacheShard shardFor(RawFileCacheKey key) {
    return shards[key.hashCode() & shardMask];
  }

  public FindResult findOrCreate(RawFileCacheKey key, int size, boolean contiguous) {
    return shardFor(key).findOrCreate(key, size, contiguous);
  }

  public Optional<FindResult> find(RawFileCacheKey key) {
    return shardFor(key).find(key);
  }

  public boolean exists(RawFileCacheKey key) {
    return shardFor(key).exists(key);
  }

  public void makeEvictable(RawFileCacheKey key) {
    shardFor(key).makeEvictable(key);
  }

  /** Drops all unpinned entries across all shards. */
  public void clear() {
    for (CacheShard s : shards) {
      s.clear();
    }
  }

  /** Aggregates per-shard counters into a {@link CacheStats} snapshot. */
  public CacheStats refreshStats() {
    CacheShard.StatsAccumulator acc = new CacheShard.StatsAccumulator();
    for (CacheShard s : shards) {
      s.appendShardStats(acc);
    }
    return new CacheStats(
        acc.tinySize,
        acc.largeSize,
        0L, // tinyPadding (Phase 2 — Java avoids per-page padding by allocating exact remainder)
        0L, // largePadding (Phase 2)
        acc.numEntries,
        acc.numTinyEntries,
        acc.numLargeEntries,
        acc.numEmptyEntries,
        acc.numShared,
        acc.numExclusive,
        acc.numPrefetch,
        acc.prefetchBytes,
        acc.sharedPinnedBytes,
        acc.exclusivePinnedBytes,
        acc.numHit,
        acc.hitBytes,
        acc.numNew,
        acc.numEvict,
        acc.numSavableEvict,
        acc.numEvictChecks,
        acc.numWaitExclusive,
        0L, // numAgedOut (Phase 2 — TTL controller)
        acc.numStales,
        0L, // allocClocks (Phase 2)
        acc.sumEvictScore,
        0L); // numSkippedSaves (Phase 2 — SSD tier)
  }

  /**
   * Releases all shard state. Idempotent — only the first concurrent call performs the shutdown;
   * subsequent calls are no-ops. If this instance is the registered singleton it is also
   * unregistered.
   */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    for (CacheShard s : shards) {
      s.shutdownInternal();
    }
    // Atomic CAS — only nulls the singleton if it still references this instance, avoiding a
    // TOCTOU race with a concurrent setInstance(otherCache).
    INSTANCE.compareAndSet(this, null);
  }
}
