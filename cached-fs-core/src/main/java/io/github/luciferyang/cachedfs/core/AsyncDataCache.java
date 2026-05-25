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
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process-wide RAM cache. Mirrors velox {@code AsyncDataCache} for the RAM tier; the SSD tier ships
 * in {@link io.github.luciferyang.cachedfs.core.ssd.SsdCache}.
 *
 * <p><b>Driver model:</b> reactive. The cache exposes a synchronous lookup/insert API and runs no
 * background threads. RAM growth is bounded by the configured shard count and the eviction policy
 * inside each {@link CacheShard}. Aging is externally driven via {@link
 * io.github.luciferyang.cachedfs.core.ttl.CacheTTLController}.
 *
 * <p><b>Sharding:</b> by {@code key.hashCode() & shardMask}. Default 4 shards (must be a power of
 * 2). Each shard owns its own mutex, entries map, and counters. The cache itself is a thin router
 * that picks the right shard for each request.
 *
 * <p><b>Singleton lifecycle:</b> {@link #setInstance} / {@link #getInstance} / {@link
 * #clearInstance} expose a process-wide reference. Hadoop embedders typically install via {@link
 * io.github.luciferyang.cachedfs.hadoop.CacheBootstrap}; pure-Java embedders may install directly.
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls. Per-shard mutexes
 * serialize within a shard; cross-shard operations have no global ordering.
 *
 * <p><b>Velox-parity divergences:</b>
 *
 * <ul>
 *   <li>{@link #removeFileEntries} fails soft per shard: if a shard throws, its targets surface as
 *       retained so the caller (TTL controller) retries next cycle. Velox returns a bool {@code
 *       success} signal; the Java port surfaces the failure via the retained set + WARN log.
 *   <li>Some Phase-2 fields in {@link CacheStats} are hardcoded to {@code 0L} pending SSD-tier
 *       wiring — see the {@link #refreshStats()} body for the current set.
 *   <li>{@link #close} is idempotent (see {@link AtomicBoolean} guard); velox's {@code shutdown} is
 *       not.
 * </ul>
 */
public final class AsyncDataCache implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncDataCache.class);

  /** Velox: {@code kDefaultNumShards = 4}. */
  public static final int DEFAULT_NUM_SHARDS = 4;

  /**
   * Configuration knobs. Mirror velox {@code AsyncDataCache::Options}.
   *
   * @param numShards number of per-shard mutex/entries containers. Must be a power of 2; higher
   *     values reduce mutex contention at the cost of per-shard overhead. Default 4 (velox {@code
   *     kDefaultNumShards}).
   * @param maxWriteRatio fraction of RAM the cache may use for write-side staging (range [0, 1]).
   *     Default 0.7 (velox {@code kDefaultMaxWriteRatio}).
   * @param ssdSavableRatio fraction of new entries that the SSD tier is asked to absorb (range [0,
   *     1]). Default 0.125. A value of 0 disables SSD save-back entirely.
   * @param minSsdSavableBytes lower bound on a write batch before it is flushed to SSD; below this
   *     the entries stay RAM-resident. Must be {@code >= 0}. Default 16 MiB.
   * @param ssdFlushThresholdBytes pending-write bytes that trigger an SSD flush. Must be {@code >=
   *     0}. Default 0 (rely on {@code minSsdSavableBytes} alone).
   */
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
      if (ssdSavableRatio < 0.0 || ssdSavableRatio > 1.0) {
        throw new IllegalArgumentException("ssdSavableRatio must be in [0,1]: " + ssdSavableRatio);
      }
      if (minSsdSavableBytes < 0L) {
        throw new IllegalArgumentException(
            "minSsdSavableBytes must be >= 0: " + minSsdSavableBytes);
      }
      if (ssdFlushThresholdBytes < 0L) {
        throw new IllegalArgumentException(
            "ssdFlushThresholdBytes must be >= 0: " + ssdFlushThresholdBytes);
      }
    }

    /** Returns the velox-parity default settings. */
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

  /**
   * Constructs a cache with the given {@link Options}. The cache is ready to serve once this
   * returns. Pure-Java embedders may call {@link #setInstance} to publish the cache to the
   * process-wide singleton; Hadoop embedders should go through {@link
   * io.github.luciferyang.cachedfs.hadoop.CacheBootstrap}.
   *
   * @param options configuration; must not be null
   * @throws NullPointerException if {@code options} is null
   */
  public AsyncDataCache(Options options) {
    Objects.requireNonNull(options, "options");
    this.options = options;
    this.shardMask = options.numShards() - 1;
    this.shards = new CacheShard[options.numShards()];
    for (int i = 0; i < shards.length; i++) {
      shards[i] = new CacheShard(this);
    }
  }

  /**
   * @return the configuration this cache was constructed with.
   */
  public Options options() {
    return options;
  }

  /**
   * @return the number of shards.
   */
  public int numShards() {
    return shards.length;
  }

  /**
   * Routes {@code key} to its owning shard. Mirrors velox sharding by full {@code (fileNum,
   * offset)} hash. The returned shard reference is stable for the cache's lifetime; callers MUST
   * NOT close it.
   */
  public CacheShard shardFor(RawFileCacheKey key) {
    return shards[key.hashCode() & shardMask];
  }

  /**
   * Returns an existing entry's pin or installs an exclusive placeholder for a new one —
   * single-flight: only one caller observes the {@link FindResult.Exclusive} placeholder per key,
   * all others either see a {@link FindResult.Hit} (entry already loaded, shared-pinned) or a
   * {@link FindResult.Waiting} (someone else holds the exclusive pin; await the future). Mirrors
   * velox {@code AsyncDataCache::findOrCreate}.
   *
   * @param key cache key (file id + offset)
   * @param size the entry's intended payload size in bytes; must be {@code > 0}
   * @param contiguous if {@code true} request contiguous memory (velox kLarge tier); {@code false}
   *     allows the cache to allocate a small (kTiny) buffer where appropriate
   * @return one of: {@link FindResult.Hit} (shared pin on a resident entry), {@link
   *     FindResult.Exclusive} (caller MUST initialize and promote via {@link
   *     CachePin#exclusiveToShared(boolean)}), or {@link FindResult.Waiting} (await the future,
   *     then retry — see {@link FindResult} class javadoc).
   * @throws IllegalArgumentException if {@code size <= 0}
   */
  public FindResult findOrCreate(RawFileCacheKey key, int size, boolean contiguous) {
    return shardFor(key).findOrCreate(key, size, contiguous);
  }

  /**
   * Returns a shared pin for {@code key} if an entry exists and is not exclusively held; empty
   * otherwise. Non-blocking — does NOT wait for an in-flight load to complete.
   *
   * @return the shared pin, or empty if absent / mid-load
   */
  public Optional<FindResult> find(RawFileCacheKey key) {
    return shardFor(key).find(key);
  }

  /**
   * @return {@code true} if an entry for {@code key} is currently in the cache (pinned or
   *     evictable). Does not affect pin counts.
   */
  public boolean exists(RawFileCacheKey key) {
    return shardFor(key).exists(key);
  }

  /**
   * Marks the entry for {@code key} as eligible for eviction. No-op when the entry is absent.
   * Equivalent to releasing the last shared pin without ever having had one — used by readers that
   * load into the cache and never intend to keep an exclusive hold.
   */
  public void makeEvictable(RawFileCacheKey key) {
    shardFor(key).makeEvictable(key);
  }

  /**
   * Drops all unpinned entries across all shards. Pinned entries are kept; the eviction is
   * best-effort, not strict. Use {@link #removeFileEntries} or {@link
   * io.github.luciferyang.cachedfs.core.ttl.CacheTTLController#applyTTL} for targeted removal.
   */
  public void clear() {
    for (CacheShard s : shards) {
      s.clear();
    }
  }

  /**
   * Removes all unpinned entries whose {@code fileNum} is in {@code filesToRemove}, fanning out to
   * every RAM shard. Returns the union of retained file ids — entries still pinned at the moment of
   * the call. The returned set is immutable; the TTL controller is the primary caller.
   *
   * <p><b>RAM-only:</b> unlike velox's {@code AsyncDataCache::removeFileEntries}, this method does
   * NOT fan out to the SSD tier. The two-tier orchestration lives in {@link
   * io.github.luciferyang.cachedfs.core.ttl.CacheTTLController#applyTTL}, which calls RAM here and
   * SSD via {@link
   * io.github.luciferyang.cachedfs.core.ssd.SsdCache#removeFileEntries(java.util.Set)}. Direct
   * callers other than the TTL controller MUST invoke the SSD tier themselves if they want two-tier
   * removal.
   *
   * <p>Per-shard exceptions are caught and the target set is added to the retained result so the
   * caller retries those files next cycle. The remaining shards continue. Matches velox's per-shard
   * try/catch + {@code success &= false} fail-soft behavior so one flaky shard does not abort the
   * whole tier pass.
   */
  public Set<Long> removeFileEntries(Set<Long> filesToRemove) {
    Objects.requireNonNull(filesToRemove, "filesToRemove");
    Set<Long> targets = Set.copyOf(filesToRemove);
    Set<Long> retained = new HashSet<>();
    for (int i = 0; i < shards.length; i++) {
      CacheShard s = shards[i];
      try {
        retained.addAll(s.removeFileEntries(targets));
      } catch (RuntimeException ex) {
        // Per-shard fail-soft: the shard failed to clean its entries, so all targets are
        // conservatively reported as retained for THIS shard. The TTL controller's cleanUp will
        // keep them marked → next cycle retries. Other shards continue. Error (OOM, SOE) is NOT
        // caught here — those are unrecoverable and propagating is the right call.
        LOG.warn("RAM shard {} removeFileEntries failed; reporting all targets as retained", i, ex);
        retained.addAll(targets);
      }
    }
    return Set.copyOf(retained);
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
        acc.numAgedOut,
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
