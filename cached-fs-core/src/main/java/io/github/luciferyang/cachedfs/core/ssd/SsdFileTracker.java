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
package io.github.luciferyang.cachedfs.core.ssd;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Per-region scoring for SSD eviction. Mirrors velox {@code SsdFileTracker}.
 *
 * <p>Each region holds a {@code double} score. Reads {@link #regionRead add to the score}; {@link
 * #fileTouched periodic decay} multiplies all scores by {@code 15/16} (≈ 6.25% reduction per pass)
 * once {@code numTouches} exceeds both {@code DECAY_INTERVAL = 1000} and {@code totalEntries / 2}.
 * {@link #regionFilled} boosts a freshly-written region to {@code max(currentScore, 1.1 × best)} so
 * it survives at least one decay pass.
 *
 * <p><b>Thread safety:</b> NOT thread-safe. Caller (the owning {@link SsdFile}) must serialize
 * access through its own mutex.
 */
final class SsdFileTracker {

  /** Velox: {@code kDecayInterval = 1000}. */
  private static final int DECAY_INTERVAL = 1000;

  /** Velox: {@code kNumEvictionCandidates = 3}. */
  private static final int MAX_EVICTION_CANDIDATES = 3;

  /** Velox: 15/16 decay factor (bit-shift-friendly geometric decay). */
  private static final double DECAY_FACTOR = 15.0 / 16.0;

  /** Velox: 1.1 boost for freshly-filled regions. */
  private static final double FILL_BOOST = 1.1;

  private final double[] regionScores;
  private long numTouches;

  SsdFileTracker(int maxRegions) {
    this.regionScores = new double[maxRegions];
  }

  /** Per-find bookkeeping: increments touch counter and possibly triggers decay. */
  void fileTouched(int totalEntries) {
    numTouches++;
    if (numTouches > DECAY_INTERVAL && numTouches > totalEntries / 2L) {
      for (int i = 0; i < regionScores.length; i++) {
        regionScores[i] *= DECAY_FACTOR;
      }
      numTouches = 0;
    }
  }

  /** Adds {@code bytes} to the region's score. */
  void regionRead(int region, long bytes) {
    regionScores[region] += bytes;
  }

  /**
   * Boosts the score of a freshly-filled region to {@code max(currentScore, 1.1 × best)} where
   * {@code best} is the maximum across all regions (including this one). Just enough above the
   * current top to survive one decay pass without becoming permanently top-of-mind.
   */
  void regionFilled(int region) {
    double best = 0;
    for (double s : regionScores) {
      if (s > best) best = s;
    }
    regionScores[region] = Math.max(regionScores[region], best * FILL_BOOST);
  }

  /**
   * Returns up to {@link #MAX_EVICTION_CANDIDATES} unpinned region indices with the lowest scores
   * (≤ avg). Sorted ascending by score. Returns empty if no unpinned regions exist.
   */
  List<Integer> findEvictionCandidates(int numRegions, int[] regionPins) {
    long unpinnedCount = 0;
    double scoreSum = 0;
    for (int i = 0; i < numRegions; i++) {
      if (regionPins[i] == 0) {
        unpinnedCount++;
        scoreSum += regionScores[i];
      }
    }
    if (unpinnedCount == 0) {
      return List.of();
    }
    double avg = scoreSum / unpinnedCount;
    List<Integer> candidates = new ArrayList<>();
    for (int i = 0; i < numRegions; i++) {
      if (regionPins[i] == 0 && regionScores[i] <= avg) {
        candidates.add(i);
      }
    }
    candidates.sort(Comparator.comparingDouble(i -> regionScores[i]));
    if (candidates.size() > MAX_EVICTION_CANDIDATES) {
      return new ArrayList<>(candidates.subList(0, MAX_EVICTION_CANDIDATES));
    }
    return candidates;
  }

  /** Returns a defensive copy of the per-region scores (for checkpointing). */
  double[] snapshotScores() {
    return regionScores.clone();
  }

  /** Restores per-region scores from a checkpoint snapshot. Copies into the live array. */
  void restoreScores(double[] src, int len) {
    if (len > regionScores.length) {
      throw new IllegalArgumentException("len=" + len + " > maxRegions=" + regionScores.length);
    }
    System.arraycopy(src, 0, regionScores, 0, len);
  }

  /** Visible for tests. */
  double[] testingScores() {
    return regionScores.clone();
  }

  /** Visible for tests. */
  long testingNumTouches() {
    return numTouches;
  }

  /** Resets a region's score (called when the region is fully evicted). */
  void resetRegion(int region) {
    regionScores[region] = 0;
  }
}
