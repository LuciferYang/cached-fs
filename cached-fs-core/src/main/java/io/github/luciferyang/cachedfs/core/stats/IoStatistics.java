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
package io.github.luciferyang.cachedfs.core.stats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-reader IO counters. Mirrors velox {@code IoStatistics} (velox/common/io/IoStatistics.h),
 * scoped per-instance and typically attached to one operator / query thread.
 *
 * <p>Complementary to the cache-wide {@link CacheStats}. Counters here describe what one reader saw
 * (RAM hits, SSD reads, gap-overread bytes, latencies); {@link CacheStats} describes what the cache
 * as a whole did (number of new entries, evictions, etc.).
 */
public final class IoStatistics {

  /**
   * Shared off-switch sentinel. All {@code inc*} calls short-circuit; all getters return 0.
   * Returned by {@code CachedFileSystem.open()} when {@code fs.cached.metrics.enabled=false}. NOT
   * thread-isolated: every consumer that gets {@code NO_OP} shares this single instance.
   */
  public static final IoStatistics NO_OP = new IoStatistics(true);

  private final boolean disabled;

  private final AtomicLong ramHit = new AtomicLong();
  private final AtomicLong ramHitBytes = new AtomicLong();

  private final AtomicLong read = new AtomicLong();
  private final AtomicLong readBytes = new AtomicLong();

  private final AtomicLong prefetch = new AtomicLong();
  private final AtomicLong prefetchBytes = new AtomicLong();

  private final AtomicLong ssdRead = new AtomicLong();
  private final AtomicLong ssdReadBytes = new AtomicLong();

  /** Bytes pulled in by gap-spanning coalescing that no requester actually wanted. */
  private final AtomicLong rawOverreadBytes = new AtomicLong();

  private final AtomicLong queryThreadIoLatencyUs = new AtomicLong();
  private final AtomicLong storageReadLatencyUs = new AtomicLong();
  private final AtomicLong ssdCacheReadLatencyUs = new AtomicLong();
  private final AtomicLong cacheWaitLatencyUs = new AtomicLong();
  private final AtomicLong coalescedSsdLoadLatencyUs = new AtomicLong();
  private final AtomicLong coalescedStorageLoadLatencyUs = new AtomicLong();

  /** Default constructor — counters are live and writable. */
  public IoStatistics() {
    this(false);
  }

  private IoStatistics(boolean disabled) {
    this.disabled = disabled;
  }

  public void incRamHit(long bytes) {
    if (disabled) return;
    ramHit.incrementAndGet();
    ramHitBytes.addAndGet(bytes);
  }

  public void incRead(long bytes) {
    if (disabled) return;
    read.incrementAndGet();
    readBytes.addAndGet(bytes);
  }

  public void incPrefetch(long bytes) {
    if (disabled) return;
    prefetch.incrementAndGet();
    prefetchBytes.addAndGet(bytes);
  }

  public void incSsdRead(long bytes) {
    if (disabled) return;
    ssdRead.incrementAndGet();
    ssdReadBytes.addAndGet(bytes);
  }

  public void incRawOverreadBytes(long bytes) {
    if (disabled) return;
    rawOverreadBytes.addAndGet(bytes);
  }

  public void incQueryThreadIoLatencyUs(long us) {
    if (disabled) return;
    queryThreadIoLatencyUs.addAndGet(us);
  }

  public void incStorageReadLatencyUs(long us) {
    if (disabled) return;
    storageReadLatencyUs.addAndGet(us);
  }

  public void incSsdCacheReadLatencyUs(long us) {
    if (disabled) return;
    ssdCacheReadLatencyUs.addAndGet(us);
  }

  public void incCacheWaitLatencyUs(long us) {
    if (disabled) return;
    cacheWaitLatencyUs.addAndGet(us);
  }

  public void incCoalescedSsdLoadLatencyUs(long us) {
    if (disabled) return;
    coalescedSsdLoadLatencyUs.addAndGet(us);
  }

  public void incCoalescedStorageLoadLatencyUs(long us) {
    if (disabled) return;
    coalescedStorageLoadLatencyUs.addAndGet(us);
  }

  public long ramHit() {
    return ramHit.get();
  }

  public long ramHitBytes() {
    return ramHitBytes.get();
  }

  public long read() {
    return read.get();
  }

  public long readBytes() {
    return readBytes.get();
  }

  public long prefetch() {
    return prefetch.get();
  }

  public long prefetchBytes() {
    return prefetchBytes.get();
  }

  public long ssdRead() {
    return ssdRead.get();
  }

  public long ssdReadBytes() {
    return ssdReadBytes.get();
  }

  public long rawOverreadBytes() {
    return rawOverreadBytes.get();
  }

  public long queryThreadIoLatencyUs() {
    return queryThreadIoLatencyUs.get();
  }

  public long storageReadLatencyUs() {
    return storageReadLatencyUs.get();
  }

  public long ssdCacheReadLatencyUs() {
    return ssdCacheReadLatencyUs.get();
  }

  public long cacheWaitLatencyUs() {
    return cacheWaitLatencyUs.get();
  }

  public long coalescedSsdLoadLatencyUs() {
    return coalescedSsdLoadLatencyUs.get();
  }

  public long coalescedStorageLoadLatencyUs() {
    return coalescedStorageLoadLatencyUs.get();
  }
}
