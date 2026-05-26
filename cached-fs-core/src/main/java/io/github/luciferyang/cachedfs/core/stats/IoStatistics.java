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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-reader IO counters. Mirrors velox {@code IoStatistics} (velox/common/io/IoStatistics.h),
 * scoped per-instance and typically attached to one operator / query thread.
 *
 * <p>Complementary to the cache-wide {@link CacheStats}. Counters here describe what one reader saw
 * (RAM hits, SSD reads, gap-overread bytes, latencies); {@link CacheStats} describes what the cache
 * as a whole did (number of new entries, evictions, etc.).
 */
public final class IoStatistics {

  private static final Logger LOG = LoggerFactory.getLogger(IoStatistics.class);

  /**
   * Phase 5c prefetch-skipped reason buckets. Fixed set so the metrics surface has bounded
   * cardinality. Unknown reasons silently route to {@code "other"} AND log a deduped WARN once per
   * unknown key (via {@link #seenUnknownReasons}) so contributor bugs (new rejection mode lacking a
   * registered reason) surface rather than getting silently absorbed.
   */
  private static final String[] PREFETCH_SKIPPED_REASONS = {
    "queue_full", "budget", "heap_pressure", "other"
  };

  private static final java.util.Set<String> KNOWN_PREFETCH_SKIPPED_REASONS =
      java.util.Set.of(PREFETCH_SKIPPED_REASONS);

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

  // --- Phase 5c counters -------------------------------------------------

  /**
   * Bytes skipped by reason. Reason set is fixed at construction (see {@link
   * #PREFETCH_SKIPPED_REASONS}); unknown reasons route to {@code "other"} with a deduped WARN.
   * Reason → AtomicLong; the map structure is immutable, only the AtomicLong values mutate.
   */
  private final Map<String, AtomicLong> prefetchSkippedByReason;

  /** Set of unknown reason strings already WARN-logged; bounded by unique typos. */
  private final java.util.Map<String, Boolean> seenUnknownReasons = new ConcurrentHashMap<>();

  /** Bytes prefetched then evicted by TTL before the consumer's await could observe them. */
  private final AtomicLong prefetchEvictedBeforeUse = new AtomicLong();

  /**
   * Bytes the admission gate suppressed when the density predicate ({@code readPct &lt; threshold})
   * was the sole reason a position-eligible, backoff-elapsed chunk was dropped. Union signal — see
   * §Decisions §6 in the reader-glue plan for operator-attribution guidance.
   */
  private final AtomicLong prefetchEligibleSuppressedBytes = new AtomicLong();

  /**
   * Count of regime-change resets on the sequential read path's HWM CAS loop (Phase 5c). Bumped
   * exactly once per logical regime-change event regardless of CAS retries.
   */
  private final AtomicLong seqHwmRegimeResets = new AtomicLong();

  /** Default constructor — counters are live and writable. */
  public IoStatistics() {
    this(false);
  }

  private IoStatistics(boolean disabled) {
    this.disabled = disabled;
    Map<String, AtomicLong> reasons = new java.util.HashMap<>();
    for (String reason : PREFETCH_SKIPPED_REASONS) {
      reasons.put(reason, new AtomicLong());
    }
    this.prefetchSkippedByReason = Map.copyOf(reasons);
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

  // --- Phase 5c inc methods ----------------------------------------------

  /**
   * Bumps the prefetch-skipped counter for {@code reason} by {@code bytes}. Unknown reasons
   * silently route to the {@code "other"} bucket AND log a deduped WARN once per unknown key.
   */
  public void incPrefetchSkipped(String reason, long bytes) {
    if (disabled) return;
    AtomicLong c = prefetchSkippedByReason.get(reason);
    if (c == null) {
      if (seenUnknownReasons.putIfAbsent(reason, Boolean.TRUE) == null) {
        LOG.warn(
            "unknown prefetchSkipped reason '{}' routed to 'other' bucket — register it in"
                + " PREFETCH_SKIPPED_REASONS",
            reason);
      }
      c = prefetchSkippedByReason.get("other");
    }
    c.addAndGet(bytes);
  }

  public void incPrefetchEvictedBeforeUse(long bytes) {
    if (disabled) return;
    prefetchEvictedBeforeUse.addAndGet(bytes);
  }

  public void incPrefetchEligibleSuppressed(long bytes) {
    if (disabled) return;
    prefetchEligibleSuppressedBytes.addAndGet(bytes);
  }

  public void incSeqHwmRegimeResets() {
    if (disabled) return;
    seqHwmRegimeResets.incrementAndGet();
  }

  /** Reason → bytes view of the prefetchSkipped buckets. Live, not a snapshot. */
  public Map<String, AtomicLong> prefetchSkippedByReasonView() {
    return prefetchSkippedByReason;
  }

  public long prefetchSkipped(String reason) {
    AtomicLong c = prefetchSkippedByReason.get(reason);
    return c == null ? 0L : c.get();
  }

  public long prefetchEvictedBeforeUse() {
    return prefetchEvictedBeforeUse.get();
  }

  public long prefetchEligibleSuppressedBytes() {
    return prefetchEligibleSuppressedBytes.get();
  }

  public long seqHwmRegimeResets() {
    return seqHwmRegimeResets.get();
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
