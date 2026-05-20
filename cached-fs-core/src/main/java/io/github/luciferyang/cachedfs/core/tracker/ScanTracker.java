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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

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
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls.
 */
public final class ScanTracker {

  private static final class MutableData {
    long referencedBytes;
    long lastReferencedBytes;
    long readBytes;

    TrackingData snapshot() {
      return new TrackingData(referencedBytes, lastReferencedBytes, readBytes);
    }
  }

  private final String scanId;

  @SuppressWarnings("unused")
  private final int loadQuantumIgnored;

  private final ReentrantLock lock = new ReentrantLock();
  private final Map<TrackingId, MutableData> data = new HashMap<>();

  public ScanTracker(String scanId, int loadQuantum) {
    this.scanId = scanId;
    this.loadQuantumIgnored = loadQuantum;
  }

  public String scanId() {
    return scanId;
  }

  /** Records that {@code bytes} have been planned for reading on {@code id}. */
  public void recordReference(TrackingId id, long bytes) {
    if (id.isEmpty()) return;
    lock.lock();
    try {
      MutableData td = data.computeIfAbsent(id, k -> new MutableData());
      td.lastReferencedBytes = bytes;
      td.referencedBytes += bytes;
    } finally {
      lock.unlock();
    }
  }

  /** Records that {@code bytes} have actually been consumed for {@code id}. */
  public void recordRead(TrackingId id, long bytes) {
    if (id.isEmpty()) return;
    lock.lock();
    try {
      MutableData td = data.computeIfAbsent(id, k -> new MutableData());
      td.readBytes += bytes;
    } finally {
      lock.unlock();
    }
  }

  /** Returns an immutable snapshot of the per-stream counters, or {@link TrackingData#EMPTY}. */
  public TrackingData data(TrackingId id) {
    lock.lock();
    try {
      MutableData td = data.get(id);
      return td == null ? TrackingData.EMPTY : td.snapshot();
    } finally {
      lock.unlock();
    }
  }

  /** Convenience wrapper for {@code data(id).readPct()}. */
  public int readPct(TrackingId id) {
    return data(id).readPct();
  }
}
