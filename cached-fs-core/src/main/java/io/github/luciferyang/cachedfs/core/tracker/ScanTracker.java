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
package io.github.luciferyang.cachedfs.core.tracker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-scan tracker of file read density. Mirrors velox {@code ScanTracker}.
 *
 * <p>Lives at the Task / TableScan level. Streams are identified by their {@code fileNum} (the
 * monotonic id assigned by {@code StringIdMap}). Two events:
 *
 * <ul>
 *   <li>{@link #recordReference} — recorded when a region is planned for reading
 *   <li>{@link #recordRead} — recorded when bytes are actually consumed
 * </ul>
 *
 * <p>{@link #data(long)} returns an immutable snapshot the planner uses to decide prefetch
 * admission.
 *
 * <p><b>Key choice.</b> Keying by raw {@code long fileNum} (instead of a 29-bit hashed {@code
 * TrackingId} as in earlier revisions) eliminates birthday-paradox collisions around ~33k files per
 * scan. Each file's density is tracked independently; the memory cost is ~40 bytes per entry × the
 * distinct fileNums the scan touches. See {@link #ScanTracker(String, int, int) the cap-aware
 * constructor} for the bound.
 *
 * <p><b>Entry cap (R3 follow-up).</b> A non-zero {@code maxEntries} caps the inner map's distinct
 * fileNum count. Once the cap is hit, subsequent {@link #recordReference} / {@link #recordRead}
 * calls for NEW fileNums silently no-op and bump {@link #entriesRejected()}; existing entries
 * continue to update normally. Operators observe the gauge to decide whether to enlarge the cap,
 * split the scan, or accept reduced density visibility on the tail of a huge fan-out. The check is
 * a soft cap (race window of size O(active concurrent puts) — acceptable for a memory bound).
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls. Storage uses {@link
 * ConcurrentHashMap} keyed by {@code Long}, each value an {@link AtomicReference} to an immutable
 * {@link TrackingData} snapshot — record/read updates are CAS-loops via {@code updateAndGet}, and
 * {@link #data(long)} is a single volatile read of the current snapshot (coherent across all three
 * fields by construction).
 *
 * <p><b>Off-switch:</b> {@link #DISABLED} short-circuits {@code recordReference}/{@code recordRead}
 * to no-ops and returns {@link TrackingData#EMPTY} from {@code data}. The class is {@code final};
 * the off-switch is realized via an internal {@code disabled} flag rather than subclassing.
 */
public final class ScanTracker {

  /**
   * Off-switch sentinel — all mutating calls are no-ops, {@link #data(long)} returns {@link
   * TrackingData#EMPTY}. Returned by {@code CacheBootstrap.trackerFor(...)} when {@code
   * fs.cached.scan-tracker.enabled=false}.
   */
  public static final ScanTracker DISABLED = new ScanTracker("__disabled__", 0, 0, true);

  private final String scanId;

  @SuppressWarnings("unused")
  private final int loadQuantumIgnored;

  /**
   * Maximum number of distinct fileNums this tracker will admit before silently rejecting new
   * entries. {@code 0} (or negative) disables the cap.
   */
  private final int maxEntries;

  private final boolean disabled;

  private final ConcurrentMap<Long, AtomicReference<TrackingData>> data = new ConcurrentHashMap<>();

  /** Count of {@code recordReference}/{@code recordRead} calls rejected because the cap was hit. */
  private final AtomicLong entriesRejected = new AtomicLong();

  /** Unlimited-cap convenience constructor. Preserved for tests / call sites that don't bound. */
  public ScanTracker(String scanId, int loadQuantum) {
    this(scanId, loadQuantum, /* maxEntries= */ 0, /* disabled= */ false);
  }

  /**
   * Bounds the inner map to {@code maxEntries} distinct fileNums. {@code 0} = unlimited (legacy
   * behavior). Production wiring passes {@code fs.cached.scan-tracker.max-entries-per-tracker}.
   */
  public ScanTracker(String scanId, int loadQuantum, int maxEntries) {
    this(scanId, loadQuantum, maxEntries, /* disabled= */ false);
  }

  private ScanTracker(String scanId, int loadQuantum, int maxEntries, boolean disabled) {
    this.scanId = scanId;
    this.loadQuantumIgnored = loadQuantum;
    this.maxEntries = maxEntries;
    this.disabled = disabled;
  }

  public String scanId() {
    return scanId;
  }

  /**
   * Number of distinct fileNums observed by this tracker. {@link ConcurrentHashMap#size} is O(1)
   * approximate (lock-free but non-atomic vs concurrent puts) — acceptable for a gauge per
   * Hadoop/JMX/Prometheus gauge semantics.
   */
  public int size() {
    return data.size();
  }

  /** Cumulative count of recordReference/recordRead calls dropped because the cap was hit. */
  public long entriesRejected() {
    return entriesRejected.get();
  }

  /** Records that {@code bytes} have been planned for reading on {@code fileNum}. */
  public void recordReference(long fileNum, long bytes) {
    if (disabled) return;
    AtomicReference<TrackingData> ref = refFor(fileNum);
    if (ref == null) return; // capped
    ref.updateAndGet(
        prev -> new TrackingData(prev.referencedBytes() + bytes, bytes, prev.readBytes()));
  }

  /** Records that {@code bytes} have actually been consumed for {@code fileNum}. */
  public void recordRead(long fileNum, long bytes) {
    if (disabled) return;
    AtomicReference<TrackingData> ref = refFor(fileNum);
    if (ref == null) return; // capped
    ref.updateAndGet(
        prev ->
            new TrackingData(
                prev.referencedBytes(), prev.lastReferencedBytes(), prev.readBytes() + bytes));
  }

  /** Returns an immutable snapshot of the per-stream counters, or {@link TrackingData#EMPTY}. */
  public TrackingData data(long fileNum) {
    if (disabled) return TrackingData.EMPTY;
    AtomicReference<TrackingData> ref = data.get(fileNum);
    return ref == null ? TrackingData.EMPTY : ref.get();
  }

  /** Convenience wrapper for {@code data(fileNum).readPct()}. */
  public int readPct(long fileNum) {
    return data(fileNum).readPct();
  }

  /**
   * Returns the entry's AtomicReference, creating it lazily. Returns {@code null} when {@link
   * #maxEntries} is set and the cap has been reached for a previously-unseen fileNum — the caller
   * then no-ops the record. The size pre-check races with concurrent puts; the cap is soft and may
   * be exceeded by O(concurrent puts) entries. Acceptable for a memory bound.
   */
  private AtomicReference<TrackingData> refFor(long fileNum) {
    AtomicReference<TrackingData> existing = data.get(fileNum);
    if (existing != null) return existing;
    if (maxEntries > 0 && data.size() >= maxEntries) {
      entriesRejected.incrementAndGet();
      return null;
    }
    return data.computeIfAbsent(fileNum, k -> new AtomicReference<>(TrackingData.EMPTY));
  }
}
