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
package io.github.luciferyang.cachedfs.core.ttl;

import io.github.luciferyang.cachedfs.core.AsyncDataCache;
import io.github.luciferyang.cachedfs.core.ssd.SsdCache;
import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Time-based controller for per-file cache aging. Mirrors velox {@code CacheTTLController}.
 *
 * <p><b>Driver model:</b> externally driven. This class does NOT start any background thread. The
 * embedding application (e.g. a Spark executor, an admin tool) calls {@link #applyTTL} on its own
 * schedule — typical use cases are PII age-out for compliance and invalidating files known to have
 * been replaced upstream.
 *
 * <p><b>What it tracks:</b> a {@code fileNum -> openTimeSeconds} map keyed by the {@code
 * StringIdLease} id minted at handle-open time. {@link #recordOpen} is wired by {@code
 * CachedFileSystem.open} so the controller sees every file the cache has been asked to serve.
 *
 * <p><b>Two-tier removal:</b> on each {@code applyTTL} call, the controller computes the set of
 * files whose open-time is older than {@code now - ttl}, calls {@link
 * AsyncDataCache#removeFileEntries} (RAM tier first), then {@link SsdCache#removeFileEntries} (SSD
 * tier). Pinned entries in either tier come back as {@code retained} — the controller keeps those
 * file ids in its tracking map so a later cycle can retry; non-retained file ids are pruned via
 * {@link #cleanUp}.
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls. {@code recordOpen} is
 * a hot-path call (every read of a fresh file); {@code applyTTL} is intended to be infrequent.
 */
public final class CacheTTLController {

  private final AsyncDataCache ramCache;
  private final SsdCache ssdCache; // nullable
  private final Clock clock;

  private final ConcurrentMap<Long, OpenInfo> openTimes = new ConcurrentHashMap<>();
  private final AtomicLong numAppliedCycles = new AtomicLong();

  /**
   * Per-file tracking record. Instance identity matters: {@link #cleanUp} uses {@link
   * ConcurrentMap#remove(Object, Object)} to compare-and-remove against the {@code OpenInfo}
   * captured at the start of the cycle, so a concurrent {@link #recordOpen} that re-inserts a fresh
   * {@code OpenInfo} during the cycle is preserved (its identity is different).
   */
  private static final class OpenInfo {
    final long openTimeSeconds;

    OpenInfo(long openTimeSeconds) {
      this.openTimeSeconds = openTimeSeconds;
    }
  }

  /**
   * @param ramCache RAM tier to drive (required)
   * @param ssdCache SSD tier to drive (optional — pass {@code null} when SSD is not configured)
   * @param clock time source; {@link Clock#systemUTC()} in production, an injectable fake in tests
   */
  public CacheTTLController(AsyncDataCache ramCache, SsdCache ssdCache, Clock clock) {
    this.ramCache = Objects.requireNonNull(ramCache, "ramCache");
    this.ssdCache = ssdCache; // may be null when SSD tier is disabled
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /**
   * Records the open-time for {@code fileNum} if not already tracked. Subsequent calls for the same
   * file are no-ops — the first observed open-time is the one TTL compares against (matches velox's
   * "files keep their original open timestamp" semantic).
   */
  public void recordOpen(long fileNum) {
    long now = clock.instant().getEpochSecond();
    openTimes.putIfAbsent(fileNum, new OpenInfo(now));
  }

  /**
   * Removes the tracking entry for {@code fileNum}. Used when an external caller knows the file
   * will never be read again (e.g. it was deleted upstream). Not normally needed — {@link
   * #applyTTL} handles steady-state pruning.
   */
  public void forget(long fileNum) {
    openTimes.remove(fileNum);
  }

  /**
   * Drops every cache entry whose owning file's open-time is older than {@code now - ttlSeconds}.
   * Runs RAM-tier removal first, then SSD-tier removal (skipped if no SSD tier was configured).
   * Pinned entries in either tier are kept in the controller's tracking map so a subsequent {@code
   * applyTTL} cycle can retry them; non-retained file ids whose entries are now gone are pruned
   * from the map.
   *
   * <p>The cycle counter is incremented in a {@code finally} block so {@link #appliedCycles}
   * remains an accurate health signal even when a tier's {@code removeFileEntries} throws
   * unexpectedly. On exception, RAM-side drops that already succeeded are kept; {@code openTimes}
   * is left as it was so the next cycle replays the remaining work.
   *
   * @param ttlSeconds aging threshold in seconds; files whose first-observed open-time is {@code
   *     now - ttlSeconds} or older are eligible for removal.
   * @return the number of files dropped from at least one tier on this cycle. Files that came back
   *     fully retained (pinned in RAM or SSD) are not counted.
   * @throws IllegalArgumentException if {@code ttlSeconds} is negative.
   */
  public int applyTTL(long ttlSeconds) {
    if (ttlSeconds < 0) {
      throw new IllegalArgumentException("ttlSeconds must be >= 0: " + ttlSeconds);
    }
    long cutoff = clock.instant().getEpochSecond() - ttlSeconds;
    // Capture the OpenInfo reference along with the fileNum so cleanUp can compare-and-remove
    // against the exact snapshot. A concurrent recordOpen that re-inserts a different OpenInfo
    // during the cycle keeps its tracking entry — see ConcurrentMap.remove(key, value) contract.
    Map<Long, OpenInfo> snapshot = new HashMap<>();
    // ConcurrentHashMap.entrySet() is weakly-consistent: no CME, may miss newly-inserted entries
    // (which are young and not aged-out anyway), may report just-removed entries (whose
    // removeFileEntries call becomes a no-op). Both are acceptable.
    for (var entry : openTimes.entrySet()) {
      OpenInfo info = entry.getValue();
      if (info.openTimeSeconds <= cutoff) {
        snapshot.put(entry.getKey(), info);
      }
    }
    try {
      if (snapshot.isEmpty()) {
        return 0;
      }
      Set<Long> filesToRemove = snapshot.keySet();
      // RAM tier first — matches velox AsyncDataCache.cpp:1107-1126 order.
      Set<Long> ramRetained = ramCache.removeFileEntries(filesToRemove);
      // SSD tier next, but only for files that RAM was willing to drop (no point removing SSD-side
      // if the RAM-side handle is still being read — velox semantics).
      Set<Long> ssdRetained = Set.of();
      if (ssdCache != null) {
        Set<Long> ssdTargets = new HashSet<>(filesToRemove);
        ssdTargets.removeAll(ramRetained);
        if (!ssdTargets.isEmpty()) {
          ssdRetained = ssdCache.removeFileEntries(ssdTargets);
        }
      }
      // Either tier retaining a file keeps it tracked, so the NEXT cycle can retry the side whose
      // entries are still pinned. Dropping an SSD-only-retained file from openTimes would orphan
      // its SSD entries (the next cycle wouldn't see the file in openTimes and would skip SSD).
      Set<Long> stillRetained = new HashSet<>(ramRetained);
      stillRetained.addAll(ssdRetained);
      cleanUp(snapshot, stillRetained);
      return filesToRemove.size() - stillRetained.size();
    } finally {
      numAppliedCycles.incrementAndGet();
    }
  }

  /** Returns the number of files currently tracked. Visible for tests and operational tooling. */
  public int trackedFileCount() {
    return openTimes.size();
  }

  /**
   * Returns the open-time of the oldest tracked file (epoch seconds), or empty if no files are
   * tracked. Useful as an observability signal for "how far behind is the TTL cycle running?".
   *
   * <p>Reads {@code openTimes} without a lock; ConcurrentHashMap weak-consistency means a fully
   * concurrent {@code applyTTL} that empties the map mid-iteration produces {@code empty}, never
   * {@code Long.MAX_VALUE}. O(n) in the tracking map size.
   */
  public OptionalLong oldestOpenTimeSeconds() {
    long oldest = Long.MAX_VALUE;
    for (OpenInfo info : openTimes.values()) {
      if (info.openTimeSeconds < oldest) {
        oldest = info.openTimeSeconds;
      }
    }
    return oldest == Long.MAX_VALUE ? OptionalLong.empty() : OptionalLong.of(oldest);
  }

  /** Number of completed {@link #applyTTL} cycles. Visible for tests. */
  public long appliedCycles() {
    return numAppliedCycles.get();
  }

  /**
   * Prunes the tracking map: for each file in {@code attempted} whose recorded {@link OpenInfo}
   * snapshot has NOT been replaced and that does not appear in {@code stillRetained}, remove the
   * entry. {@link ConcurrentMap#remove(Object, Object)} compares by value identity so a concurrent
   * {@link #recordOpen} that re-inserted a fresh {@code OpenInfo} during the cycle is preserved.
   */
  private void cleanUp(Map<Long, OpenInfo> attempted, Set<Long> stillRetained) {
    for (var entry : attempted.entrySet()) {
      Long fn = entry.getKey();
      if (!stillRetained.contains(fn)) {
        openTimes.remove(fn, entry.getValue());
      }
    }
  }
}
