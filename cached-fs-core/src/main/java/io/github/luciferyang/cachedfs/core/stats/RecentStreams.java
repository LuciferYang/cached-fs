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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Fixed-size ring buffer of per-stream {@link IoStatisticsSnapshot}s. Each {@code
 * CachingInputStream.close()} pushes one snapshot; the oldest is overwritten when the ring wraps.
 *
 * <p><b>Lock-free.</b> An {@link AtomicLong} write index strides through the ring; concurrent
 * {@link #add} calls race only on the increment, and each writes to its own slot. Readers ({@link
 * #snapshot()}) take a coherent view by reading the index once and walking backward — the worst-
 * case race is observing a slot mid-write and seeing the previous occupant (a strictly older
 * snapshot, still valid). No reader ever blocks a writer.
 *
 * <p><b>Capacity {@code 0} is a no-op.</b> {@link #add} swallows the snapshot, {@link #snapshot}
 * returns an empty list. Lets operators disable the ring without changing call-site code.
 *
 * <p>Lives in {@code cached-fs-core} so the contract is observable without depending on the Hadoop
 * decorator. {@code CacheBootstrap} owns one instance and exposes it via {@code recentStreams()}.
 */
public final class RecentStreams {

  /** Sentinel for the disabled ring (capacity 0). Shared across all callers — stateless. */
  public static final RecentStreams DISABLED = new RecentStreams(0);

  private final AtomicReferenceArray<IoStatisticsSnapshot> ring;
  private final AtomicLong writeIndex = new AtomicLong();

  public RecentStreams(int capacity) {
    if (capacity < 0) {
      throw new IllegalArgumentException("capacity must be non-negative: " + capacity);
    }
    this.ring = new AtomicReferenceArray<>(capacity);
  }

  /** Capacity of the ring. {@code 0} means the ring is disabled. */
  public int capacity() {
    return ring.length();
  }

  /** Cumulative count of snapshots added since construction (NOT capped by capacity). */
  public long addedTotal() {
    return writeIndex.get();
  }

  /** Pushes a snapshot into the ring. No-op on {@link #DISABLED} (capacity 0). */
  public void add(IoStatisticsSnapshot snapshot) {
    int cap = ring.length();
    if (cap == 0) return;
    long idx = writeIndex.getAndIncrement();
    int slot = (int) (Math.floorMod(idx, cap));
    ring.set(slot, snapshot);
  }

  /**
   * Returns a most-recent-first list of up to {@link #capacity} snapshots. Each snapshot is the
   * value of a ring slot at read time; a snapshot may be {@code null} if its slot has not yet been
   * written — filtered out of the result. Order is determined by the write index at call time; if
   * concurrent writes happen during the read, the older end of the list may show just-overwritten
   * slots (a strictly older snapshot, still a valid sample).
   */
  public List<IoStatisticsSnapshot> snapshot() {
    int cap = ring.length();
    if (cap == 0) return List.of();
    long w = writeIndex.get();
    int count = (int) Math.min(w, cap);
    List<IoStatisticsSnapshot> out = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      long idx = w - 1 - i;
      int slot = (int) Math.floorMod(idx, cap);
      IoStatisticsSnapshot s = ring.get(slot);
      if (s != null) out.add(s);
    }
    return Collections.unmodifiableList(out);
  }
}
