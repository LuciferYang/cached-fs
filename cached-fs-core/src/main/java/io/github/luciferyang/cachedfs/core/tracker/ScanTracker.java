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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-scan tracker of column read density. Mirrors velox {@code ScanTracker}.
 *
 * <p>Lives at the Task / TableScan level. Streams are identified by {@link TrackingId}, packing a
 * {@code (node, streamKind)} pair. Two events:
 *
 * <ul>
 *   <li>{@link #recordReference} — recorded when a region is planned for reading
 *   <li>{@link #recordRead} — recorded when bytes are actually consumed
 * </ul>
 *
 * <p>{@link #data(TrackingId)} returns an immutable snapshot the planner uses to decide prefetch
 * admission. Note: the {@code loadQuantum} velox accepts in its constructor is silently dropped and
 * never stored; we keep parity by exposing it as a constructor parameter for compatibility but
 * never reading it.
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls. Storage uses {@link
 * ConcurrentHashMap} keyed by {@link TrackingId} with each value an {@link AtomicReference} to an
 * immutable {@link TrackingData} snapshot — record/read updates are CAS-loops via {@code
 * updateAndGet}, and {@link #data(TrackingId)} is a single volatile read of the current snapshot
 * (coherent across all three fields by construction).
 *
 * <p><b>Off-switch:</b> {@link #DISABLED} short-circuits {@code recordReference}/{@code recordRead}
 * to no-ops and returns {@link TrackingData#EMPTY} from {@code data}. The class is {@code final};
 * the off-switch is realized via an internal {@code disabled} flag rather than subclassing.
 */
public final class ScanTracker {

  /**
   * Off-switch sentinel — all mutating calls are no-ops, {@link #data(TrackingId)} returns {@link
   * TrackingData#EMPTY}. Returned by {@code CacheBootstrap.trackerFor(...)} when {@code
   * fs.cached.scan-tracker.enabled=false}.
   */
  public static final ScanTracker DISABLED = new ScanTracker("__disabled__", 0, true);

  private final String scanId;

  @SuppressWarnings("unused")
  private final int loadQuantumIgnored;

  private final boolean disabled;

  private final ConcurrentMap<TrackingId, AtomicReference<TrackingData>> data =
      new ConcurrentHashMap<>();

  public ScanTracker(String scanId, int loadQuantum) {
    this(scanId, loadQuantum, /* disabled= */ false);
  }

  private ScanTracker(String scanId, int loadQuantum, boolean disabled) {
    this.scanId = scanId;
    this.loadQuantumIgnored = loadQuantum;
    this.disabled = disabled;
  }

  public String scanId() {
    return scanId;
  }

  /**
   * Number of distinct {@link TrackingId}s observed by this tracker. {@link ConcurrentHashMap#size}
   * is O(1) approximate (lock-free but non-atomic vs concurrent puts) — acceptable for a gauge per
   * Hadoop/JMX/Prometheus gauge semantics.
   */
  public int size() {
    return data.size();
  }

  /** Records that {@code bytes} have been planned for reading on {@code id}. */
  public void recordReference(TrackingId id, long bytes) {
    if (disabled || id.isEmpty()) return;
    refFor(id)
        .updateAndGet(
            prev -> new TrackingData(prev.referencedBytes() + bytes, bytes, prev.readBytes()));
  }

  /** Records that {@code bytes} have actually been consumed for {@code id}. */
  public void recordRead(TrackingId id, long bytes) {
    if (disabled || id.isEmpty()) return;
    refFor(id)
        .updateAndGet(
            prev ->
                new TrackingData(
                    prev.referencedBytes(), prev.lastReferencedBytes(), prev.readBytes() + bytes));
  }

  /** Returns an immutable snapshot of the per-stream counters, or {@link TrackingData#EMPTY}. */
  public TrackingData data(TrackingId id) {
    if (disabled) return TrackingData.EMPTY;
    AtomicReference<TrackingData> ref = data.get(id);
    return ref == null ? TrackingData.EMPTY : ref.get();
  }

  /** Convenience wrapper for {@code data(id).readPct()}. */
  public int readPct(TrackingId id) {
    return data(id).readPct();
  }

  private AtomicReference<TrackingData> refFor(TrackingId id) {
    return data.computeIfAbsent(id, k -> new AtomicReference<>(TrackingData.EMPTY));
  }
}
