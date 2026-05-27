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
package io.github.luciferyang.cachedfs.spark.affinity;

/**
 * Atomic, read-only snapshot of {@link CachedFsSoftAffinityManager} state. Useful for metrics
 * dashboards + diagnostics + tests; the snapshot is taken under the manager's read lock so all
 * fields are consistent with each other (no half-applied knob change visible to one field but not
 * another).
 *
 * @param enabled master switch
 * @param replicationNum top-N executor candidates returned per lookup
 * @param minTargetHosts threshold below which soft-affinity defers to native locality
 * @param virtualNodes ring density: virtual partitions per physical executor
 * @param detectDuplicateReading feedback-mode toggle
 * @param duplicateReadingMaxCacheItems cap of the three bounded feedback-state maps
 * @param executorCount currently-registered executors
 * @param hostCount distinct hosts spanning the registered executors
 * @param duplicateReadingEntries entries currently held in the observed-placement cache
 * @param stageRddsCount entries in the {@code stageId → rddIds} feedback-state map
 * @param rddPartitionsCount entries in the {@code rddId → splits} feedback-state map
 */
public record AffinitySnapshot(
    boolean enabled,
    int replicationNum,
    int minTargetHosts,
    int virtualNodes,
    boolean detectDuplicateReading,
    int duplicateReadingMaxCacheItems,
    int executorCount,
    int hostCount,
    int duplicateReadingEntries,
    int stageRddsCount,
    int rddPartitionsCount) {}
