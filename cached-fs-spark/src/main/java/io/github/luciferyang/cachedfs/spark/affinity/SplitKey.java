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

import java.util.Objects;

/**
 * Fingerprint of one file-split inside a Spark {@code FilePartition}. Used by the feedback-mode
 * path of {@link CachedFsSoftAffinityManager} to key observed (executor, host) placements against
 * the EXACT file region the task read — Gluten's {@code "path_start_length"} convention, with the
 * null-byte separator chosen so URI paths containing any printable character compose unambiguously.
 */
public record SplitKey(int partitionId, String path, long start, long length) {

  public SplitKey {
    Objects.requireNonNull(path, "path");
  }
}
