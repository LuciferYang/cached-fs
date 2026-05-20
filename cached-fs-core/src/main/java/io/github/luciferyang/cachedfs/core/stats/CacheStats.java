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

/**
 * Cache-wide entry-level counters returned by {@code AsyncDataCache.refreshStats()}. Mirrors the
 * fields documented in velox §5.5 (CacheStats).
 *
 * <p>Aggregated process-wide across all shards. Distinct from per-reader {@link IoStatistics}.
 *
 * <p>{@code numHit} counts only confirmed reuse; the first reuse of a prefetched entry bumps the
 * entry's {@code isFirstUse_} flag instead. {@code numAgedOut} counts entries removed by {@code
 * removeFileEntries} (TTL or explicit drop), distinct from score-based {@code numEvict}.
 */
public record CacheStats(
    // ---- snapshot stats ----
    long tinySize,
    long largeSize,
    long tinyPadding,
    long largePadding,
    int numEntries,
    int numTinyEntries,
    int numLargeEntries,
    int numEmptyEntries,
    int numShared,
    int numExclusive,
    int numPrefetch,
    long prefetchBytes,
    long sharedPinnedBytes,
    long exclusivePinnedBytes,
    // ---- cumulative stats ----
    long numHit,
    long hitBytes,
    long numNew,
    long numEvict,
    long numSavableEvict,
    long numEvictChecks,
    long numWaitExclusive,
    long numAgedOut,
    long numStales,
    long allocClocks,
    long sumEvictScore,
    long numSkippedSaves) {

  /** Empty zero-valued stats for tests / initial state. */
  public static CacheStats empty() {
    return new CacheStats(
        0L, 0L, 0L, 0L, // tinySize, largeSize, tinyPadding, largePadding
        0, 0, 0, 0, // numEntries, numTinyEntries, numLargeEntries, numEmptyEntries
        0, 0, 0, // numShared, numExclusive, numPrefetch
        0L, 0L, 0L, // prefetchBytes, sharedPinnedBytes, exclusivePinnedBytes
        0L, 0L, 0L, // numHit, hitBytes, numNew
        0L, 0L, 0L, 0L, // numEvict, numSavableEvict, numEvictChecks, numWaitExclusive
        0L, 0L, 0L, 0L, 0L); // numAgedOut, numStales, allocClocks, sumEvictScore, numSkippedSaves
  }
}
