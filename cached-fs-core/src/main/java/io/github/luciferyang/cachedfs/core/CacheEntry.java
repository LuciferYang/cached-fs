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

import io.github.luciferyang.cachedfs.core.tracker.TrackingId;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * One cached byte range. Mirrors velox {@code AsyncDataCacheEntry}.
 *
 * <p>Three storage paths chosen by size and the caller's {@code contiguous} flag:
 *
 * <ul>
 *   <li>{@code size < TINY_DATA_SIZE (2048)} → {@code tinyData} — on-heap {@code byte[]} (skips the
 *       off-heap path entirely)
 *   <li>{@code size >= 2048 AND contiguous} → {@code contiguousData} — single direct {@link
 *       ByteBuffer}
 *   <li>{@code size >= 2048 AND !contiguous} → {@code pages} — page run of direct buffers
 * </ul>
 *
 * <p><b>Off-heap memory choice:</b> v1 uses {@link ByteBuffer#allocateDirect(int)} because {@link
 * java.lang.foreign.MemorySegment} is still a preview API in JDK 21 (finalized only in JDK 22).
 * When the project moves the floor to JDK 22+ we will switch to {@code MemorySegment} for explicit
 * lifecycle and lower per-allocation overhead. Until then direct buffers are freed via {@code
 * Cleaner} on GC.
 *
 * <p>State is encoded in {@code numPins}:
 *
 * <ul>
 *   <li>{@link #EXCLUSIVE} ({@code -10000}) — being filled by one thread; everyone else waits on
 *       {@code promise}
 *   <li>{@code 0} — evictable
 *   <li>{@code > 0} — number of shared readers (hits)
 * </ul>
 *
 * <p><b>Thread safety:</b> publicly read-only accessors (size/pins/score/flags) are safe for
 * concurrent reads; payload visibility for shared readers is provided by the shard mutex acquired
 * in {@code CacheShard.completeExclusive}/{@code lookupLocked}, not by the pin counter. Mutator
 * methods are package-private and require holding the shard mutex unless documented otherwise.
 *
 * @implNote Velox: {@code AsyncDataCacheEntry} (AsyncDataCache.h:156-393). The pin counter race
 *     against the lock-free {@link #releaseSharedPin()} is broken by using {@code compareAndSet} in
 *     both {@link #addSharedPinLocked()} and {@link #releaseSharedPin()}.
 */
public final class CacheEntry {

  /** "Tiny data" threshold in bytes: entries strictly below this go on-heap as {@code byte[]}. */
  public static final int TINY_DATA_SIZE = 2048;

  /** Sentinel for {@code numPins} indicating an entry currently being filled exclusively. */
  public static final int EXCLUSIVE = -10000;

  /** Default page size for non-contiguous allocations. */
  static final int PAGE_SIZE = 4 * 1024;

  /** Cap on per-shard reusable {@link CacheEntry} pool. */
  static final int MAX_FREE_ENTRIES = 1024;

  private static final VarHandle NUM_PINS;
  private static final VarHandle FIRST_USE;

  static {
    try {
      var lookup = MethodHandles.lookup();
      NUM_PINS = lookup.findVarHandle(CacheEntry.class, "numPins", int.class);
      FIRST_USE = lookup.findVarHandle(CacheEntry.class, "isFirstUse", boolean.class);
    } catch (NoSuchFieldException | IllegalAccessException ex) {
      throw new ExceptionInInitializerError(ex);
    }
  }

  // --- identity ---
  private RawFileCacheKey key;
  private long groupId;
  private TrackingId trackingId = TrackingId.EMPTY;

  // --- storage (mutually exclusive) ---
  private byte[] tinyData;
  private ByteBuffer contiguousData;
  private List<ByteBuffer> pages;
  // Volatile so that CacheShard.appendShardStats, which reads size() under the shard mutex
  // for entries still in EXCLUSIVE state (initialize() runs outside the mutex), never observes
  // a stale or torn-looking value. Storage fields (tinyData/contiguousData/pages) are only
  // dereferenced by pin holders after exclusive→shared, which is published via the mutex.
  private volatile int size;

  // --- state ---
  @SuppressWarnings({"FieldMayBeFinal", "unused"}) // accessed via NUM_PINS VarHandle
  private volatile int numPins = 0;

  private final AccessStats accessStats = new AccessStats();

  /**
   * Promise woken when this entry transitions exclusive -> shared. Field guarded by shard mutex.
   */
  private CompletableFuture<Void> promise;

  /** True if this entry was speculatively loaded; cleared on first reuse. */
  private volatile boolean isPrefetch;

  /**
   * True from the moment a fresh exclusive entry is published or a prefetched entry is hit, until
   * {@link #getAndClearFirstUseFlag()} reads it. Modified via VarHandle for race-free clearing.
   */
  @SuppressWarnings({"FieldMayBeFinal", "unused"}) // accessed via FIRST_USE VarHandle
  private volatile boolean isFirstUse;

  // --- SSD coordination ---
  /** Non-null when this entry has an SSD copy already (Object-typed to keep core SSD-free). */
  private volatile Object ssdFile;

  private volatile long ssdOffset;
  private volatile boolean ssdSaveable;

  /** Constructs an empty (no key, no data) entry. Used by the shard's free-entry pool. */
  CacheEntry() {}

  // --- key / identity --------------------------------------------------------

  RawFileCacheKey key() {
    return key;
  }

  public long fileNum() {
    return key == null ? 0 : key.fileNum();
  }

  public long offset() {
    return key == null ? 0 : key.offset();
  }

  public boolean hasKey() {
    return key != null;
  }

  void clearKey() {
    this.key = null;
  }

  void initKey(RawFileCacheKey newKey) {
    this.key = newKey;
  }

  public long groupId() {
    return groupId;
  }

  void setGroupId(long groupId) {
    this.groupId = groupId;
  }

  public TrackingId trackingId() {
    return trackingId;
  }

  void setTrackingId(TrackingId trackingId) {
    this.trackingId = trackingId;
  }

  // --- size / data -----------------------------------------------------------

  public int size() {
    return size;
  }

  /**
   * Allocates backing storage for this entry. The entry must currently hold no data and be in
   * {@link #EXCLUSIVE} state. Caller must hold the shard mutex when establishing exclusivity but
   * the actual allocation happens outside the mutex per velox's design.
   */
  void initialize(int size, boolean contiguous) {
    if (size <= 0) {
      throw new IllegalArgumentException("size must be > 0: " + size);
    }
    if (this.size != 0 || tinyData != null || contiguousData != null || pages != null) {
      throw new IllegalStateException("CacheEntry already initialized");
    }
    this.size = size;
    if (size < TINY_DATA_SIZE) {
      this.tinyData = new byte[size];
    } else if (contiguous) {
      this.contiguousData = ByteBuffer.allocateDirect(size);
    } else {
      // Each page is allocated for its actual chunk, so the last page may be smaller than
      // PAGE_SIZE — there is no padding (in contrast to velox where pages are full-sized and
      // tinyPadding/largePadding accumulate the tail). Cast to long FIRST to avoid int overflow
      // when size is near Integer.MAX_VALUE.
      int numPages = Math.toIntExact(((long) size + PAGE_SIZE - 1) / PAGE_SIZE);
      List<ByteBuffer> p = new ArrayList<>(numPages);
      int remaining = size;
      for (int i = 0; i < numPages; i++) {
        int chunk = Math.min(PAGE_SIZE, remaining);
        p.add(ByteBuffer.allocateDirect(chunk));
        remaining -= chunk;
      }
      this.pages = p;
    }
  }

  /**
   * Returns writable buffer ranges covering the first {@code length} bytes. Mirrors velox's {@code
   * dataRanges()}. Returns an empty list if {@code length == 0}.
   *
   * <p>Buffers alias entry storage and must not be retained past pin close. Writes are permitted
   * only while holding an exclusive pin.
   */
  public List<ByteBuffer> dataRanges(int length) {
    if (length < 0 || length > size) {
      throw new IllegalArgumentException("length " + length + " not in [0, " + size + "]");
    }
    if (length == 0) {
      return List.of();
    }
    if (tinyData != null) {
      return List.of(ByteBuffer.wrap(tinyData, 0, length));
    }
    if (contiguousData != null) {
      ByteBuffer dup = contiguousData.duplicate();
      dup.position(0).limit(length);
      return List.of(dup);
    }
    if (pages != null) {
      List<ByteBuffer> out = new ArrayList<>(pages.size());
      int remaining = length;
      for (ByteBuffer p : pages) {
        if (remaining <= 0) break;
        ByteBuffer dup = p.duplicate();
        int chunk = Math.min(dup.capacity(), remaining);
        dup.position(0).limit(chunk);
        out.add(dup);
        remaining -= chunk;
      }
      return out;
    }
    return List.of();
  }

  /** Frees all data storage and zeroes {@code size}. Direct buffers are returned to Cleaner. */
  void freeData() {
    tinyData = null;
    contiguousData = null;
    pages = null;
    size = 0;
  }

  /**
   * Resets all per-owner state so this entry can be safely returned to the free pool and reused.
   * Caller must hold the shard mutex.
   */
  void resetForReuse() {
    freeData();
    clearKey();
    groupId = 0;
    trackingId = TrackingId.EMPTY;
    accessStats.reset();
    isPrefetch = false;
    FIRST_USE.setVolatile(this, false);
    ssdFile = null;
    ssdOffset = 0;
    ssdSaveable = false;
    NUM_PINS.setVolatile(this, 0); // defensive: callers expect a fresh-from-pool entry to be at 0
  }

  // --- pin state -------------------------------------------------------------

  public int numPins() {
    return (int) NUM_PINS.getVolatile(this);
  }

  public boolean isShared() {
    return numPins() > 0;
  }

  public boolean isExclusive() {
    return numPins() == EXCLUSIVE;
  }

  /**
   * Sets {@code numPins} to {@link #EXCLUSIVE}. Caller must hold the shard mutex. Asserts the entry
   * is currently unpinned (numPins == 0); a freshly-taken or recycled entry only.
   */
  void markExclusiveLocked() {
    if (!NUM_PINS.compareAndSet(this, 0, EXCLUSIVE)) {
      throw new IllegalStateException("markExclusiveLocked: numPins=" + numPins());
    }
  }

  /**
   * Atomically increments the shared pin count via CAS. Caller must hold the shard mutex.
   *
   * <p>The shard mutex serializes <em>increments</em> against eviction (which reads {@code numPins}
   * under the mutex); however {@link #releaseSharedPin()} runs <strong>without </strong> the shard
   * mutex, so the increment must be CAS-atomic against a concurrent decrement. Otherwise a
   * lost-decrement race could either leak a pin (entry never evictable) or under-count a pin
   * (use-after-free).
   */
  void addSharedPinLocked() {
    while (true) {
      int cur = (int) NUM_PINS.getVolatile(this);
      if (cur < 0) {
        throw new IllegalStateException("addSharedPinLocked on exclusive entry");
      }
      if (NUM_PINS.compareAndSet(this, cur, cur + 1)) {
        return;
      }
    }
  }

  /** Decrements the shared pin count by one via CAS. Returns the post-decrement value. */
  public int releaseSharedPin() {
    while (true) {
      int cur = (int) NUM_PINS.getVolatile(this);
      if (cur <= 0) {
        throw new IllegalStateException("releaseSharedPin: numPins=" + cur);
      }
      if (NUM_PINS.compareAndSet(this, cur, cur - 1)) {
        return cur - 1;
      }
    }
  }

  /** Resets pin count from {@link #EXCLUSIVE} to a single shared pin (the producer's). */
  void exclusiveToShared() {
    if (!NUM_PINS.compareAndSet(this, EXCLUSIVE, 1)) {
      throw new IllegalStateException("exclusiveToShared: not exclusive");
    }
  }

  // --- access stats / scoring ------------------------------------------------

  /** Caller must hold the shard mutex. */
  void touchLocked() {
    accessStats.touch();
  }

  public int score(int now) {
    return accessStats.score(now, size);
  }

  /**
   * Package-private — exposes the mutable AccessStats only to colocated cache code that already
   * holds the shard mutex. External callers must use {@link #score(int)}, {@link #touchLocked()},
   * or {@link #makeEvictableLocked()}.
   */
  AccessStats accessStats() {
    return accessStats;
  }

  /** Caller must hold the shard mutex. */
  void makeEvictableLocked() {
    accessStats.makeEvictable();
  }

  // --- promise (for EXCLUSIVE wakeup) ----------------------------------------

  /** Caller must hold the shard mutex. Only valid while this entry is exclusive. */
  CompletableFuture<Void> getOrCreatePromiseLocked() {
    if (promise == null) {
      promise = new CompletableFuture<>();
    }
    return promise;
  }

  /** Caller must hold the shard mutex. Detaches and returns the promise so it can be completed. */
  CompletableFuture<Void> movePromiseLocked() {
    CompletableFuture<Void> p = promise;
    promise = null;
    return p;
  }

  // --- prefetch flags --------------------------------------------------------

  public boolean isPrefetch() {
    return isPrefetch;
  }

  void setPrefetch(boolean value) {
    this.isPrefetch = value;
  }

  /**
   * Atomically clears and returns the first-use flag. Multiple concurrent readers will see at most
   * one {@code true} return value via CAS.
   */
  public boolean getAndClearFirstUseFlag() {
    return FIRST_USE.compareAndSet(this, true, false);
  }

  void setFirstUse() {
    FIRST_USE.setVolatile(this, true);
  }

  // --- SSD coordination ------------------------------------------------------

  public Object ssdFile() {
    return ssdFile;
  }

  public long ssdOffset() {
    return ssdOffset;
  }

  /**
   * Records that this entry has an SSD copy at {@code offset} on {@code ssdFile}. Clears {@link
   * #ssdSaveable()} so the entry is not subsequently re-flushed to SSD as if it were RAM-only —
   * matches velox {@code AsyncDataCacheEntry::setSsdFile} (AsyncDataCache.h:277-281).
   */
  void setSsdFile(Object ssdFile, long offset) {
    this.ssdFile = ssdFile;
    this.ssdOffset = offset;
    this.ssdSaveable = false;
  }

  public boolean ssdSaveable() {
    return ssdSaveable;
  }

  void setSsdSaveable(boolean v) {
    this.ssdSaveable = v;
  }
}
