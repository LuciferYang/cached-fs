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

/**
 * Per-stream identifier within a scan: a {@code (node, streamKind)} pair packed into an int32 with
 * node in the high 27 bits and stream kind in the low 5. Mirrors velox {@code TrackingId}
 * (ScanTracker.h:30-32, 33-56).
 *
 * <p>The value {@code -1} is the "empty" sentinel.
 */
public record TrackingId(int id) {

  /** Empty / "no-id" sentinel. */
  public static final TrackingId EMPTY = new TrackingId(-1);

  private static final int STREAM_KIND_BITS = 5;
  private static final int STREAM_KIND_MASK = (1 << STREAM_KIND_BITS) - 1;

  /** Exclusive upper bound on {@code node}: 2^(31 - 5) = 2^26 (signed int, top bit reserved). */
  private static final int NODE_MAX = 1 << (Integer.SIZE - STREAM_KIND_BITS - 1);

  public static TrackingId of(int node, int streamKind) {
    if (node < 0 || node >= NODE_MAX) {
      throw new IllegalArgumentException("node must be in [0, " + NODE_MAX + "): " + node);
    }
    if ((streamKind & ~STREAM_KIND_MASK) != 0) {
      throw new IllegalArgumentException(
          "streamKind must fit in " + STREAM_KIND_BITS + " bits: " + streamKind);
    }
    return new TrackingId((node << STREAM_KIND_BITS) | streamKind);
  }

  public boolean isEmpty() {
    return id == -1;
  }

  /**
   * @throws IllegalStateException if this is the {@link #EMPTY} sentinel
   */
  public int node() {
    if (isEmpty()) {
      throw new IllegalStateException("node() on empty TrackingId");
    }
    return id >>> STREAM_KIND_BITS;
  }

  /**
   * @throws IllegalStateException if this is the {@link #EMPTY} sentinel
   */
  public int streamKind() {
    if (isEmpty()) {
      throw new IllegalStateException("streamKind() on empty TrackingId");
    }
    return id & STREAM_KIND_MASK;
  }
}
