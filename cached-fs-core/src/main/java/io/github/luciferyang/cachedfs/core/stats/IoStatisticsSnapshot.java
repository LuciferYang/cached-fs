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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Immutable point-in-time snapshot of an {@link IoStatistics}. Pushed into the bootstrap's recent-
 * streams ring buffer by {@code CachingInputStream.close()} so post-hoc debugging can see what one
 * stream observed without needing to instrument every reader.
 *
 * <p>Per-event reason buckets ({@code prefetchSkipped*}) are captured separately because they're
 * the most useful debugging signal — they tell you WHY a chunk wasn't prefetched.
 *
 * @param closedAtNanos {@link System#nanoTime()} reading at snapshot time. Use for ordering only;
 *     do not subtract across snapshots to derive wall-clock durations.
 */
public record IoStatisticsSnapshot(
    long closedAtNanos,
    long ramHit,
    long ramHitBytes,
    long read,
    long readBytes,
    long prefetch,
    long prefetchBytes,
    long ssdRead,
    long ssdReadBytes,
    long rawOverreadBytes,
    long queryThreadIoLatencyUs,
    long storageReadLatencyUs,
    long ssdCacheReadLatencyUs,
    long cacheWaitLatencyUs,
    long coalescedSsdLoadLatencyUs,
    long coalescedStorageLoadLatencyUs,
    long prefetchSkippedQueueFullBytes,
    long prefetchSkippedBudgetBytes,
    long prefetchSkippedHeapPressureBytes,
    long prefetchSkippedOtherBytes,
    long prefetchEvictedBeforeUseBytes,
    long prefetchEligibleSuppressedBytes,
    long seqHwmRegimeResets) {

  /**
   * Reads every counter on the given {@link IoStatistics} into a new snapshot. Counters are read
   * one-by-one (no atomic group read), so a writer concurrent with this call may produce a snapshot
   * that mixes pre- and post-update values. For the debugging-ring use case this is acceptable: the
   * close path quiesces the stream before snapshotting.
   */
  public static IoStatisticsSnapshot of(IoStatistics s) {
    Map<String, AtomicLong> skipped = s.prefetchSkippedByReasonView();
    return new IoStatisticsSnapshot(
        System.nanoTime(),
        s.ramHit(),
        s.ramHitBytes(),
        s.read(),
        s.readBytes(),
        s.prefetch(),
        s.prefetchBytes(),
        s.ssdRead(),
        s.ssdReadBytes(),
        s.rawOverreadBytes(),
        s.queryThreadIoLatencyUs(),
        s.storageReadLatencyUs(),
        s.ssdCacheReadLatencyUs(),
        s.cacheWaitLatencyUs(),
        s.coalescedSsdLoadLatencyUs(),
        s.coalescedStorageLoadLatencyUs(),
        skipped.getOrDefault("queue_full", new AtomicLong()).get(),
        skipped.getOrDefault("budget", new AtomicLong()).get(),
        skipped.getOrDefault("heap_pressure", new AtomicLong()).get(),
        skipped.getOrDefault("other", new AtomicLong()).get(),
        s.prefetchEvictedBeforeUse(),
        s.prefetchEligibleSuppressedBytes(),
        s.seqHwmRegimeResets());
  }
}
