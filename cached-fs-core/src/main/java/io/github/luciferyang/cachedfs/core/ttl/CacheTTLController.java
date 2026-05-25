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
 * <p><b>What it tracks:</b> a {@code fileNum -> openTime} map keyed by the {@link
 * io.github.luciferyang.cachedfs.core.id.StringIdMap} id minted at handle-open time. {@link
 * #recordOpen} is wired by {@code CachedFileSystem.open} so the controller sees every file the
 * cache has been asked to serve.
 *
 * <p><b>Two-tier removal:</b> on each {@code applyTTL} call, the controller computes the set of
 * files whose first-observed open-time is strictly older than {@code now - ttl} (the boundary
 * predicate matches velox: a file opened exactly at the cutoff is retained), marks each as
 * <i>remove-in-progress</i>, then calls {@link AsyncDataCache#removeFileEntries} (RAM) and {@link
 * SsdCache#removeFileEntries} (SSD) with the full target set — SSD is NOT gated on RAM retention,
 * matching velox's contract that the SSD tier independently tracks pinned regions. Files reported
 * back as retained (pinned in either tier) keep their tracking entry for retry on a future cycle.
 *
 * <p><b>Race-free re-open:</b> a reader that calls {@link #recordOpen} for a file currently in the
 * remove-in-progress state replaces the tracking entry with a fresh open-time and clears the flag.
 * The cycle's {@code cleanUp} uses compare-and-remove so the replaced entry survives — preventing
 * the case where the cycle drops a cache entry, a reader re-populates it, and tracking is then
 * cleaned up leaving the fresh entry invisible to subsequent TTL cycles.
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls — the tracking map's
 * invariants hold under concurrent {@link #recordOpen}, {@link #applyTTL}, and {@link #forget}.
 * Caveat on the {@code int} return value of {@link #applyTTL}: when two threads call {@code
 * applyTTL} simultaneously, both may see a carried-over marked entry and both will count its
 * removal in their own return value — so the return value is meaningful as a per-cycle metric only
 * when {@code applyTTL} is invoked serially. The shard-level {@code numAgedOut} counter in {@link
 * AsyncDataCache#refreshStats()} is correctly serialized by per-shard mutexes and is the
 * authoritative aggregate signal.
 *
 * <p><b>Memory:</b> {@code openTimes} grows monotonically until {@link #applyTTL} prunes aged
 * entries or {@link #forget} explicitly drops one. A long-lived JVM that opens many distinct files
 * without ever invoking {@code applyTTL} will accumulate a tracking-map entry per file for the
 * process lifetime. Production embedders MUST either schedule {@code applyTTL} on a cadence that
 * matches their open() throughput or call {@code forget} at known end-of-life points.
 */
public final class CacheTTLController {

  private final AsyncDataCache ramCache;
  private final SsdCache ssdCache; // nullable
  private final Clock clock;

  private final ConcurrentMap<Long, OpenInfo> openTimes = new ConcurrentHashMap<>();
  private final AtomicLong numAppliedCycles = new AtomicLong();

  /**
   * Per-file tracking record. Instance identity matters: {@link #cleanUp} uses {@link
   * ConcurrentMap#remove(Object, Object)} and {@link ConcurrentMap#replace(Object, Object, Object)}
   * which compare with {@code equals}. {@code OpenInfo} keeps {@code Object}'s identity-based
   * {@code equals}/{@code hashCode}, so two {@code OpenInfo(100L, false)} instances are NOT equal —
   * the compare-and-swap protects against a concurrent {@link #recordOpen} that re-inserts a fresh
   * instance during the cycle.
   *
   * <p>Intentionally NOT a {@code record}: a record's structural {@code equals} would silently
   * break the identity-based compare-and-swap.
   */
  private static final class OpenInfo {
    final long openTimeSeconds;
    final boolean removeInProgress;

    OpenInfo(long openTimeSeconds, boolean removeInProgress) {
      this.openTimeSeconds = openTimeSeconds;
      this.removeInProgress = removeInProgress;
    }
  }

  /**
   * @param ramCache RAM tier to drive (required)
   * @param ssdCache SSD tier to drive (optional — pass {@code null} when SSD is not configured)
   * @param clock time source; {@link Clock#systemUTC()} in production, an injectable fake in tests
   * @throws NullPointerException if {@code ramCache} or {@code clock} is null
   */
  public CacheTTLController(AsyncDataCache ramCache, SsdCache ssdCache, Clock clock) {
    this.ramCache = Objects.requireNonNull(ramCache, "ramCache");
    this.ssdCache = ssdCache; // may be null when SSD tier is disabled
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /**
   * Records the open-time for {@code fileNum}. Mirrors velox {@code addOpenFileInfo}: if no
   * tracking entry exists, insert one at the current clock time and return {@code true}. If a
   * tracking entry exists but is in the remove-in-progress state (the next-to-run {@link #applyTTL}
   * cycle would drop it), REPLACE it with a fresh open-time and clear the flag — this keeps the
   * file tracked for the freshly-loaded cache entries the caller is about to populate. Otherwise,
   * no-op and return {@code false} so the first-observed open-time wins.
   *
   * @param fileNum the {@link io.github.luciferyang.cachedfs.core.id.StringIdMap} id of the file
   *     the cache has just been asked to serve. Must be a real id; passing {@code 0} or any value
   *     from a different id namespace is undefined behavior.
   * @return {@code true} when this call installed or refreshed the tracking entry; {@code false}
   *     when an existing not-in-progress entry was preserved.
   * @apiNote The return type was widened from {@code void} to {@code boolean} in 0.1 to expose the
   *     install-vs-preserve signal for observability / metrics. The Hadoop decorator ({@code
   *     CachedFileSystem.open}) ignores the value today; downstream embedders MAY use it to drive a
   *     churn-rate metric.
   */
  public boolean recordOpen(long fileNum) {
    long now = clock.instant().getEpochSecond();
    while (true) {
      OpenInfo existing = openTimes.get(fileNum);
      if (existing == null) {
        OpenInfo prev = openTimes.putIfAbsent(fileNum, new OpenInfo(now, false));
        if (prev == null) {
          return true;
        }
        // Race lost — retry on the now-present entry.
        continue;
      }
      if (!existing.removeInProgress) {
        // Keep the original timestamp. Matches velox 'first open wins' semantic.
        return false;
      }
      // The cycle that marked this entry must not drop our about-to-be-populated cache entries.
      // CAS-replace: if a concurrent cleanUp already cleared the flag (cycle finished and decided
      // to retain), or a concurrent recordOpen already replaced it, retry.
      if (openTimes.replace(fileNum, existing, new OpenInfo(now, false))) {
        return true;
      }
    }
  }

  /**
   * Removes the tracking entry for {@code fileNum}. Use when an external caller knows the file will
   * never be read again (e.g. it was deleted upstream). This DOES NOT drop the file's cache entries
   * — call {@link #applyTTL} or the explicit eviction APIs to release cached bytes. {@code
   * forget()} on an untracked id is a silent no-op.
   *
   * <p>{@code forget} unconditionally removes the tracking entry regardless of its {@code
   * removeInProgress} state — it bypasses the mark-and-CAS protocol used by {@link #recordOpen} and
   * {@link #applyTTL}. A {@code forget} that races with an in-flight {@code applyTTL} cycle orphans
   * the tracking entry but the cycle's tier-removal still completes for the file; this is benign
   * because the file is, by the caller's contract, never going to be read again.
   *
   * <p>This is a Java-port extension and has no velox equivalent.
   */
  public void forget(long fileNum) {
    openTimes.remove(fileNum);
  }

  /**
   * Drops every cache entry whose owning file's first-observed open-time is strictly older than
   * {@code now - ttlSeconds}. Boundary semantic matches velox: a file opened exactly at the cutoff
   * is retained.
   *
   * <p>Algorithm:
   *
   * <ol>
   *   <li>Snapshot all eligible files AND mark each tracking entry as remove-in-progress, so a
   *       concurrent {@link #recordOpen} for the same file can CAS-replace with a fresh entry
   *       (which {@code cleanUp} below will leave alone).
   *   <li>Call {@link AsyncDataCache#removeFileEntries} for the RAM tier and (if configured) {@link
   *       SsdCache#removeFileEntries} for the SSD tier. Both calls receive the full target set —
   *       SSD is NOT gated on RAM retention, matching velox's {@code AsyncDataCache::
   *       removeFileEntries} which always fans the request out to both tiers.
   *   <li>{@code cleanUp(retained)}: for each file that came back retained by EITHER tier, clear
   *       its remove-in-progress flag so a future cycle can retry; for each file not retained,
   *       compare-and-remove the tracking entry against the marked value.
   * </ol>
   *
   * <p>The cycle counter is incremented in a {@code finally} block so {@link #appliedCycles}
   * remains an accurate health signal even when a tier's {@code removeFileEntries} throws. On
   * exception (cycle did NOT reach cleanUp): the snapshot's marked entries are reset by clearing
   * the {@code removeInProgress} flag while preserving the original open-time — mirroring velox
   * {@code CacheTTLController::reset()}. This keeps recovery semantics aligned with velox: a
   * subsequent {@link #recordOpen} on the now-unmarked entry returns {@code false} (preserves the
   * original open-time) so the next {@code applyTTL} cycle re-evaluates the file against its
   * original openTime. RAM-side entries already dropped in the failed cycle stay dropped.
   *
   * @param ttlSeconds aging threshold in seconds; files whose first-observed open-time is strictly
   *     older than {@code now - ttlSeconds} are eligible for removal. {@code ttlSeconds = 0} means
   *     "drop every file opened in any earlier second". Values larger than the current epoch are
   *     rejected (the subtraction would underflow and silently drop everything — see {@link
   *     IllegalArgumentException} below).
   * @return the number of files for which neither tier retained any pin on this cycle. A file that
   *     is pinned in EITHER tier counts as retained and is NOT included in the return value. Files
   *     with no entries in either tier still count as "dropped" — the controller cannot distinguish
   *     that case from a successful eviction. Reliable as a per-cycle metric only when {@code
   *     applyTTL} is invoked from a single thread; see class-level javadoc for the concurrent-
   *     caller caveat.
   * @throws IllegalArgumentException if {@code ttlSeconds} is negative OR larger than the current
   *     epoch second (the cutoff arithmetic would underflow).
   */
  public int applyTTL(long ttlSeconds) {
    if (ttlSeconds < 0) {
      throw new IllegalArgumentException("ttlSeconds must be >= 0: " + ttlSeconds);
    }
    long now = clock.instant().getEpochSecond();
    if (ttlSeconds > now) {
      throw new IllegalArgumentException(
          "ttlSeconds (" + ttlSeconds + ") exceeds current epoch (" + now + "); cutoff underflows");
    }
    long cutoff = now - ttlSeconds;
    // Snapshot AND mark: replace each candidate with a marked OpenInfo so a concurrent recordOpen
    // can CAS-replace with a fresh entry and survive cleanUp. Velox's getAndMarkAgedOutFiles does
    // the same thing under a write lock; ConcurrentHashMap.replace gives us per-key atomicity
    // without a global lock. Size-hint the snapshot to avoid rehashing on large tracking maps.
    Map<Long, OpenInfo> snapshot = new HashMap<>(Math.min(openTimes.size(), 1024));
    for (var entry : openTimes.entrySet()) {
      OpenInfo info = entry.getValue();
      // Velox semantic: strict less-than. Already-marked entries (a previous cycle's retried
      // files) are also picked up so retries make progress.
      if (info.removeInProgress) {
        snapshot.put(entry.getKey(), info);
        continue;
      }
      if (info.openTimeSeconds < cutoff) {
        OpenInfo marked = new OpenInfo(info.openTimeSeconds, true);
        if (openTimes.replace(entry.getKey(), info, marked)) {
          snapshot.put(entry.getKey(), marked);
        }
        // If replace failed, a concurrent recordOpen replaced this entry with a fresh
        // not-in-progress one — it's no longer aged out, skip.
      }
    }
    boolean cleanedUp = false;
    try {
      if (snapshot.isEmpty()) {
        cleanedUp = true;
        return 0;
      }
      Set<Long> filesToRemove = snapshot.keySet();
      Set<Long> ramRetained = ramCache.removeFileEntries(filesToRemove);
      Set<Long> ssdRetained = Set.of();
      if (ssdCache != null) {
        // Velox AsyncDataCache::removeFileEntries fans out to SSD with the full target set, not
        // (filesToRemove - ramRetained). The SSD tier independently tracks pinned regions; a RAM
        // pin does not imply the SSD copy is in use.
        ssdRetained = ssdCache.removeFileEntries(filesToRemove);
      }
      HashSet<Long> stillRetained = new HashSet<>(ramRetained);
      stillRetained.addAll(ssdRetained);
      cleanUp(snapshot, stillRetained);
      cleanedUp = true;
      return filesToRemove.size() - stillRetained.size();
    } finally {
      if (!cleanedUp) {
        // Velox-parity reset: clear removeInProgress on every entry the failed cycle marked,
        // preserving the original open-time. Without this, a recordOpen between the failure and
        // the next applyTTL cycle would CAS-replace the marked entry with a fresh open-time and
        // reset the aging clock — diverging from velox's compliance-safe behavior.
        reset(snapshot);
      }
      numAppliedCycles.incrementAndGet();
    }
  }

  /**
   * Velox-parity reset: clears the {@code removeInProgress} flag on each marked entry from the
   * failed cycle's snapshot, preserving the original open-time. CAS-based so a concurrent {@link
   * #recordOpen} that already replaced the entry is left alone.
   */
  private void reset(Map<Long, OpenInfo> snapshot) {
    for (var entry : snapshot.entrySet()) {
      OpenInfo marked = entry.getValue();
      if (marked.removeInProgress) {
        openTimes.replace(entry.getKey(), marked, new OpenInfo(marked.openTimeSeconds, false));
      }
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
   * concurrent {@link #applyTTL} that empties the map mid-iteration produces {@code empty}, never
   * {@code Long.MAX_VALUE}. Marked (in-progress-of-removal) entries are included in the search
   * because they still have a well-defined original open-time. O(n) in the tracking map size.
   *
   * @return the oldest tracked open-time in epoch seconds, or {@link OptionalLong#empty()} if the
   *     tracking map is empty (or was emptied during iteration).
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
   * Test seam: force the tracking entry for {@code fileNum} into the {@code removeInProgress=true}
   * state without running a full {@link #applyTTL} cycle. This lets a single-threaded test exercise
   * the {@link #recordOpen} CAS-replace branch (which fires only when an entry is already marked)
   * deterministically. No-op if the file is not tracked or is already marked.
   */
  void markForTesting(long fileNum) {
    OpenInfo cur = openTimes.get(fileNum);
    if (cur != null && !cur.removeInProgress) {
      openTimes.replace(fileNum, cur, new OpenInfo(cur.openTimeSeconds, true));
    }
  }

  /** Test seam: returns true if the tracking entry exists and is marked remove-in-progress. */
  boolean isMarkedForTesting(long fileNum) {
    OpenInfo cur = openTimes.get(fileNum);
    return cur != null && cur.removeInProgress;
  }

  /**
   * For each file in the snapshot:
   *
   * <ul>
   *   <li>If retained by either tier, CAS-replace the marked entry with one whose {@code
   *       removeInProgress} flag is cleared (so a future cycle can retry). If a concurrent {@link
   *       #recordOpen} already replaced the entry, the replace is a no-op — that's fine, the
   *       record-open already cleared the flag and refreshed the time.
   *   <li>Otherwise, compare-and-remove the marked entry. A concurrent {@link #recordOpen} that
   *       replaced it (different identity, removeInProgress=false) is preserved — the freshly
   *       populated cache entries stay tracked.
   * </ul>
   */
  private void cleanUp(Map<Long, OpenInfo> attempted, Set<Long> stillRetained) {
    for (var entry : attempted.entrySet()) {
      Long fn = entry.getKey();
      OpenInfo marked = entry.getValue();
      if (stillRetained.contains(fn)) {
        openTimes.replace(fn, marked, new OpenInfo(marked.openTimeSeconds, false));
      } else {
        openTimes.remove(fn, marked);
      }
    }
  }
}
