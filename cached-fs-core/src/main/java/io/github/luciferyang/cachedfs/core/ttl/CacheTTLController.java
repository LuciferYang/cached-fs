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
import java.util.HashSet;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
  private volatile long numAppliedCycles;

  /** Per-file tracking record. */
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
   * @return the number of files that were dropped from at least one tier on this cycle. Files that
   *     came back fully retained (couldn't be dropped from either tier) are not counted.
   */
  public int applyTTL(long ttlSeconds) {
    if (ttlSeconds < 0) {
      throw new IllegalArgumentException("ttlSeconds must be >= 0: " + ttlSeconds);
    }
    long cutoff = clock.instant().getEpochSecond() - ttlSeconds;
    Set<Long> filesToRemove = new HashSet<>();
    for (var entry : openTimes.entrySet()) {
      if (entry.getValue().openTimeSeconds <= cutoff) {
        filesToRemove.add(entry.getKey());
      }
    }
    if (filesToRemove.isEmpty()) {
      numAppliedCycles++;
      return 0;
    }
    // RAM tier first — matches velox AsyncDataCache.cpp:1107-1126 order.
    Set<Long> ramRetained = ramCache.removeFileEntries(filesToRemove);
    // SSD tier next — only for files that RAM was willing to drop (no point removing SSD-side if
    // the RAM-side handle is still being read). velox: cache_.removeFileEntries() fans out RAM
    // then SSD internally; here the two tiers are separately owned so we sequence ourselves.
    Set<Long> ssdRetained = Set.of();
    if (ssdCache != null) {
      Set<Long> ssdTargets = new HashSet<>(filesToRemove);
      ssdTargets.removeAll(ramRetained);
      if (!ssdTargets.isEmpty()) {
        ssdRetained = ssdCache.removeFileEntries(ssdTargets);
      }
    }
    // A file is fully retained only if RAM kept it pinned; once it cleared RAM, we'd already have
    // dropped it from the controller's map even if SSD kept it (SSD pins are short-lived and the
    // file won't be re-tracked unless another open() arrives).
    Set<Long> stillRetained = new HashSet<>(ramRetained);
    stillRetained.addAll(ssdRetained);
    cleanUp(filesToRemove, stillRetained);
    numAppliedCycles++;
    return filesToRemove.size() - stillRetained.size();
  }

  /** Returns the number of files currently tracked. Visible for tests and operational tooling. */
  public int trackedFileCount() {
    return openTimes.size();
  }

  /**
   * Returns the open-time of the oldest tracked file (epoch seconds), or empty if no files are
   * tracked. Useful as an observability signal for "how far behind is the TTL cycle running?".
   */
  public OptionalLong oldestOpenTimeSeconds() {
    long oldest = Long.MAX_VALUE;
    boolean any = false;
    for (OpenInfo info : openTimes.values()) {
      if (info.openTimeSeconds < oldest) {
        oldest = info.openTimeSeconds;
      }
      any = true;
    }
    return any ? OptionalLong.of(oldest) : OptionalLong.empty();
  }

  /** Number of completed {@link #applyTTL} cycles. Visible for tests. */
  public long appliedCycles() {
    return numAppliedCycles;
  }

  /**
   * Prunes the tracking map: any file in {@code attempted} that does NOT appear in {@code
   * stillRetained} is removed. Retained files stay so the next cycle can retry them.
   */
  private void cleanUp(Set<Long> attempted, Set<Long> stillRetained) {
    for (Long fn : attempted) {
      if (!stillRetained.contains(fn)) {
        openTimes.remove(fn);
      }
    }
  }
}
