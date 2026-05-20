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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One mutex-protected partition of the RAM cache. Mirrors velox {@code CacheShard}.
 *
 * <p>Holds a {@code key → entry} map plus a dense {@code entries} list used by the clock-sweep
 * eviction. Mutex strategy uses a {@link ReentrantLock} (NOT {@code synchronized}) so we can {@code
 * tryLock} from arbitration paths.
 *
 * <p>Slot-reuse model (mirrors velox): {@code entries} holds {@code null} for evicted slots, and
 * {@code emptySlots} tracks indices available for reuse. This keeps {@code clockHand} indices
 * stable across removal and makes removal {@code O(1)}.
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls. Internal methods
 * suffixed with {@code Locked} require the caller to already hold {@link #mutex}.
 */
public final class CacheShard {

  /** Velox: {@code kMaxEvictionSamples = 10}. */
  private static final int MAX_EVICTION_SAMPLES = 10;

  /** Velox: {@code kEvictionPercentile = 80}. */
  private static final int EVICTION_PERCENTILE = 80;

  /** Velox: {@code kNoThreshold = INT_MAX} initial value. */
  private static final int NO_THRESHOLD = Integer.MAX_VALUE;

  private final AsyncDataCache cache;

  private final ReentrantLock mutex = new ReentrantLock();
  private final Map<RawFileCacheKey, CacheEntry> entryMap = new HashMap<>();

  /** Sparse list — slots may contain {@code null} for evicted entries. */
  private final List<CacheEntry> entries = new ArrayList<>();

  private final Deque<Integer> emptySlots = new ArrayDeque<>();
  private final Deque<CacheEntry> freeEntries = new ArrayDeque<>();

  // --- clock + calibration ---
  private int clockHand;
  private int eventCounter;
  private int evictionThreshold = NO_THRESHOLD;

  // --- counters ---
  private long numHit;
  private long hitBytes;
  private long numNew;
  private long numEvict;
  private long numSavableEvict;
  private long numEvictChecks;
  private long numWaitExclusive;
  private long numStales;
  private long sumEvictScore;

  CacheShard(AsyncDataCache cache) {
    this.cache = cache;
  }

  /**
   * Finds or creates a cache entry. Mirrors velox {@code CacheShard::findOrCreate}.
   *
   * @param key cache key (file + offset)
   * @param size requested entry size in bytes
   * @param contiguous whether to allocate as a single contiguous range
   * @return one of {@link FindResult.Hit}, {@link FindResult.Exclusive}, {@link FindResult.Waiting}
   */
  public FindResult findOrCreate(RawFileCacheKey key, int size, boolean contiguous) {
    mutex.lock();
    CacheEntry entryToInit;
    try {
      Optional<FindResult> hit = lookupLocked(key, size);
      if (hit.isPresent()) {
        return hit.get();
      }
      // Miss path — allocate placeholder under the mutex.
      CacheEntry e = takeFreeEntryLocked();
      e.markExclusiveLocked();
      entryMap.put(key, e);
      placeInEntriesLocked(e);
      e.initKey(key);
      e.setFirstUse(); // matches velox AsyncDataCache.cpp:323
      numNew++;
      entryToInit = e;
    } finally {
      mutex.unlock();
    }
    // Allocate backing storage outside the mutex. If initialization throws (e.g. size<=0,
    // OOM in ByteBuffer.allocateDirect), the exclusive placeholder is still in entryMap and
    // would deadlock all waiters forever. Unwind it via releaseFailedExclusive: that removes
    // the entry from entryMap, frees the slot, and completes any waiter promise so they retry.
    try {
      entryToInit.initialize(size, contiguous);
    } catch (Throwable t) {
      releaseFailedExclusive(entryToInit);
      throw t;
    }
    return FindResult.exclusive(CachePin.exclusive(this, entryToInit));
  }

  /** Pure look-up. Mirrors velox {@code CacheShard::find}. Returns empty on miss. */
  public Optional<FindResult> find(RawFileCacheKey key) {
    mutex.lock();
    try {
      return lookupLocked(key, 0);
    } finally {
      mutex.unlock();
    }
  }

  public boolean exists(RawFileCacheKey key) {
    mutex.lock();
    try {
      CacheEntry e = entryMap.get(key);
      if (e != null) {
        e.touchLocked();
        return true;
      }
      return false;
    } finally {
      mutex.unlock();
    }
  }

  public void makeEvictable(RawFileCacheKey key) {
    mutex.lock();
    try {
      CacheEntry e = entryMap.get(key);
      if (e != null) {
        e.makeEvictableLocked();
      }
    } finally {
      mutex.unlock();
    }
  }

  /** lookupLocked. Caller must hold {@link #mutex}. */
  private Optional<FindResult> lookupLocked(RawFileCacheKey key, int size) {
    eventCounter++;
    CacheEntry found = entryMap.get(key);
    if (found == null) {
      return Optional.empty();
    }
    if (found.isExclusive()) {
      numWaitExclusive++;
      CompletableFuture<Void> future = found.getOrCreatePromiseLocked();
      return Optional.of(FindResult.waiting(future));
    }
    if (size > 0 && found.size() < size) {
      // Stale: a larger quantum is requested than what we cached. Velox's lazy pattern: clear
      // the key + erase from the map; the next eviction sweep reaps the entry slot. This avoids
      // O(n) list compaction on the hot lookup path.
      numStales++;
      entryMap.remove(key);
      found.clearKey();
      return Optional.empty();
    }
    found.touchLocked();
    if (found.isPrefetch()) {
      found.setFirstUse();
      found.setPrefetch(false);
    } else {
      numHit++;
      hitBytes += found.size();
    }
    found.addSharedPinLocked();
    return Optional.of(FindResult.hit(CachePin.shared(this, found)));
  }

  void completeExclusive(CacheEntry entry, boolean ssdSavable) {
    CompletableFuture<Void> promise;
    mutex.lock();
    try {
      entry.exclusiveToShared();
      promise = entry.movePromiseLocked();
      // SSD-source de-dup: only mark saveable when the entry is not already SSD-resident.
      // Complemented by CacheEntry.setSsdFile() which clears ssdSaveable as the entry transitions
      // to SSD-resident (matches velox AsyncDataCache.h:277-281). Together: an entry is saveable
      // exactly when it was filled from a non-SSD source and has not yet been written to SSD.
      if (ssdSavable && entry.ssdFile() == null) {
        entry.setSsdSaveable(true);
        // TODO Phase 2: bump cache-wide ssdSaveable byte counter via cache.possibleSsdSave(size).
      }
    } finally {
      mutex.unlock();
    }
    if (promise != null) {
      promise.complete(null);
    }
  }

  void releaseShared(CacheEntry entry) {
    entry.releaseSharedPin();
  }

  /**
   * Called when a {@link CachePin#exclusive(CacheShard, CacheEntry) exclusive pin} is closed
   * without being promoted to shared (load failed). Removes the entry and wakes waiters so they
   * retry. Mirrors velox {@code release()} on a {@code kExclusive} entry.
   */
  void releaseFailedExclusive(CacheEntry entry) {
    CompletableFuture<Void> promise;
    mutex.lock();
    try {
      removeEntryLocked(entry);
      promise = entry.movePromiseLocked();
    } finally {
      mutex.unlock();
    }
    if (promise != null) {
      promise.complete(null);
    }
  }

  /**
   * Removes {@code entry} from the index map and releases its slot for reuse. Caller must hold the
   * mutex. Recycles into the free pool (capped at {@link CacheEntry#MAX_FREE_ENTRIES}).
   */
  private void removeEntryLocked(CacheEntry entry) {
    if (entry.hasKey()) {
      entryMap.remove(entry.key());
    }
    int idx = findSlotIndexLocked(entry);
    if (idx >= 0) {
      entries.set(idx, null);
      emptySlots.addLast(idx);
    }
    entry.resetForReuse();
    if (freeEntries.size() < CacheEntry.MAX_FREE_ENTRIES) {
      freeEntries.addLast(entry);
    }
  }

  /** O(n) but only used during failed-exclusive unwind. Hot-path eviction uses the slot index. */
  private int findSlotIndexLocked(CacheEntry entry) {
    for (int i = 0; i < entries.size(); i++) {
      if (entries.get(i) == entry) {
        return i;
      }
    }
    return -1;
  }

  private void placeInEntriesLocked(CacheEntry e) {
    // LIFO over emptySlots (push_back / pop_back) to match velox's `emptySlots_` (a std::vector
    // used as a stack at AsyncDataCache.cpp).
    Integer slot = emptySlots.pollLast();
    if (slot != null) {
      entries.set(slot, e);
    } else {
      entries.add(e);
    }
  }

  private CacheEntry takeFreeEntryLocked() {
    CacheEntry e = freeEntries.pollLast();
    return e != null ? e : new CacheEntry();
  }

  // --- eviction --------------------------------------------------------------

  /**
   * Evicts entries to free up to {@code bytesToFree} bytes. Returns the actually freed bytes.
   * Mirrors velox {@code CacheShard::evict}.
   *
   * @param bytesToFree target bytes
   * @param evictAllUnpinned when true, every unpinned entry is evictable regardless of score
   */
  public long evict(long bytesToFree, boolean evictAllUnpinned) {
    long freed = 0;
    mutex.lock();
    try {
      int sweepBudget = entries.size();
      if (sweepBudget == 0) {
        return 0L;
      }
      int counter = 0;
      int numChecked = 0;
      int now = AccessStats.now();
      while (counter++ < sweepBudget && freed < bytesToFree) {
        if (clockHand >= entries.size()) {
          clockHand = 0;
        }
        int idx = clockHand;
        CacheEntry candidate = entries.get(idx);
        clockHand++;
        numEvictChecks++;
        if (candidate == null) {
          continue;
        }
        numChecked++;
        // Calibration triggers (velox §3.5 step 2).
        if (evictionThreshold == NO_THRESHOLD
            || eventCounter > entries.size() / 4
            || numChecked > entries.size() / 8) {
          calibrateThresholdLocked();
          numChecked = 0;
          eventCounter = 0;
          now = AccessStats.now();
        }
        // numPins != 0 covers both shared (>0) and exclusive (-10000) — eligible iff exactly 0.
        if (candidate.numPins() != 0) {
          continue;
        }
        int score = 0;
        boolean evictable =
            !candidate.hasKey()
                || evictAllUnpinned
                || (score = candidate.score(now)) >= evictionThreshold;
        if (!evictable) {
          continue;
        }
        long bytes = candidate.size();
        if (candidate.ssdSaveable()) {
          numSavableEvict++;
        }
        // Velox slot-reuse: leave a null in the slot rather than shifting indices. This keeps
        // clockHand stable across removal so the sweep never silently skips an entry.
        if (candidate.hasKey()) {
          entryMap.remove(candidate.key());
        }
        candidate.resetForReuse();
        if (freeEntries.size() < CacheEntry.MAX_FREE_ENTRIES) {
          freeEntries.addLast(candidate);
        }
        entries.set(idx, null);
        emptySlots.addLast(idx);
        numEvict++;
        if (score > 0) {
          sumEvictScore += score;
        }
        freed += bytes;
      }
    } finally {
      mutex.unlock();
    }
    return freed;
  }

  private void calibrateThresholdLocked() {
    int n = entries.size();
    if (n == 0) {
      return;
    }
    int numSamples = Math.min(MAX_EVICTION_SAMPLES, n);
    int step = Math.max(1, n / numSamples);
    int now = AccessStats.now();
    int idx = clockHand % n;
    int[] samples = new int[numSamples];
    for (int i = 0; i < numSamples; i++) {
      CacheEntry e = entries.get(idx);
      // Null slots score as 0 (NOT INT_MAX) — matches velox AsyncDataCache.cpp:633. Using
      // INT_MAX would inflate the percentile and effectively prevent eviction of real entries.
      samples[i] = e == null ? 0 : e.score(now);
      idx = (idx + step) % n;
    }
    Arrays.sort(samples);
    evictionThreshold = samples[(samples.length * EVICTION_PERCENTILE) / 100];
  }

  /** Drops all unpinned entries. Mirrors velox {@code AsyncDataCache::clear()}. */
  public void clear() {
    evict(Long.MAX_VALUE, true);
  }

  // --- introspection ---------------------------------------------------------

  public int size() {
    mutex.lock();
    try {
      return entryMap.size();
    } finally {
      mutex.unlock();
    }
  }

  /** Returns a snapshot of live entries (skips empty slots). Test-only. */
  List<CacheEntry> testingEntries() {
    mutex.lock();
    try {
      List<CacheEntry> snapshot = new ArrayList<>(entries.size());
      for (CacheEntry e : entries) {
        if (e != null) snapshot.add(e);
      }
      return Collections.unmodifiableList(snapshot);
    } finally {
      mutex.unlock();
    }
  }

  void appendShardStats(StatsAccumulator acc) {
    mutex.lock();
    try {
      acc.numHit += numHit;
      acc.hitBytes += hitBytes;
      acc.numNew += numNew;
      acc.numEvict += numEvict;
      acc.numSavableEvict += numSavableEvict;
      acc.numEvictChecks += numEvictChecks;
      acc.numWaitExclusive += numWaitExclusive;
      acc.numStales += numStales;
      acc.sumEvictScore += sumEvictScore;
      // Mirror velox: null slots and key-less entries both count as "empty"; exclusive entries
      // are accounted only into exclusivePinnedBytes/numExclusive (their size may still be in
      // flight through initialize()) and are NOT counted in numEntries.
      for (CacheEntry e : entries) {
        if (e == null) {
          acc.numEmptyEntries++;
          continue;
        }
        if (!e.hasKey()) {
          acc.numEmptyEntries++;
          continue;
        }
        if (e.isExclusive()) {
          acc.numExclusive++;
          acc.exclusivePinnedBytes += e.size();
          continue;
        }
        acc.numEntries++;
        if (e.size() < CacheEntry.TINY_DATA_SIZE) {
          acc.numTinyEntries++;
          acc.tinySize += e.size();
        } else {
          acc.numLargeEntries++;
          acc.largeSize += e.size();
        }
        if (e.isShared()) {
          acc.numShared++;
          acc.sharedPinnedBytes += e.size();
        }
        if (e.isPrefetch()) {
          acc.numPrefetch++;
          acc.prefetchBytes += e.size();
        }
      }
    } finally {
      mutex.unlock();
    }
  }

  /** Cache itself ↔ shard back-reference, used by SSD integration in later phases. */
  AsyncDataCache cache() {
    return cache;
  }

  /** Used by {@code AsyncDataCache.shutdown()} to reset internal state. */
  void shutdownInternal() {
    mutex.lock();
    try {
      entries.clear();
      emptySlots.clear();
      entryMap.clear();
      freeEntries.clear();
    } finally {
      mutex.unlock();
    }
  }

  /** Mutable accumulator used by {@link AsyncDataCache#refreshStats()}. */
  static final class StatsAccumulator {
    long tinySize, largeSize;
    int numEntries, numTinyEntries, numLargeEntries, numEmptyEntries;
    int numShared, numExclusive, numPrefetch;
    long sharedPinnedBytes, exclusivePinnedBytes, prefetchBytes;
    long numHit, hitBytes, numNew, numEvict, numSavableEvict, numEvictChecks;
    long numWaitExclusive, numStales, sumEvictScore;
  }
}
