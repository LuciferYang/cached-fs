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
 * Immutable snapshot of a stream's per-tracker counters. Mirrors velox {@code TrackingData}.
 *
 * <p>Returned by {@link ScanTracker#data(TrackingId)}. The internal mutable form is held privately
 * inside {@link ScanTracker}.
 */
public record TrackingData(long referencedBytes, long lastReferencedBytes, long readBytes) {

  public static final TrackingData EMPTY = new TrackingData(0L, 0L, 0L);

  /**
   * Read percentage (0-100) used to gate prefetch admission. Returns {@code 100} when no bytes have
   * been referenced yet — matches velox {@code ScanTracker::readPct}'s "unreferenced streams admit
   * prefetch" convention.
   *
   * <p>Java uses {@code long} for the underlying counters (atomic-friendly), whereas velox stores
   * them as {@code double}, so velox's divide-first form ({@code readBytes / referencedBytes *
   * 100}) is precise floating-point. cached-fs uses multiply-first ({@code 100 * readBytes /
   * referencedBytes}) to preserve precision in integer arithmetic; both implementations produce
   * equivalent percentages for prefetch admission decisions.
   */
  public int readPct() {
    if (referencedBytes == 0) {
      return 100;
    }
    return (int) (100L * readBytes / referencedBytes);
  }

  /**
   * Mirrors velox {@code BufferedInput::adjustedReadPct}. Excludes the most-recent reference batch
   * from the denominator so subsequent stripes report accurate steady-state density. The very first
   * stripe collapses the denominator to zero and returns 0 (suppressing prefetch on unproven
   * streams).
   */
  public int adjustedReadPct() {
    long denom = referencedBytes - lastReferencedBytes;
    if (denom == 0) {
      return 0;
    }
    return (int) (100L * readBytes / denom);
  }
}
