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
package io.github.luciferyang.cachedfs.core;

/**
 * Non-owning {@code (fileNum, offset)} cache key — the value type used as the lookup key in every
 * cache map. Mirrors velox {@code RawFileCacheKey} (AsyncDataCache.h:115-122).
 *
 * <p>16 bytes regardless of path length. {@link #hashCode} is mixed so the high bits influence the
 * RAM shard (see velox §3.3 — RAM shards by {@code hash(fileNum, offset) & shardMask_}).
 */
public record RawFileCacheKey(long fileNum, long offset) {

  /** Mixing hash. Same algorithm family as folly::hash mix used by velox. */
  @Override
  public int hashCode() {
    long h = fileNum;
    h ^= offset + 0x9E3779B97F4A7C15L + (h << 6) + (h >>> 2);
    h ^= h >>> 33;
    h *= 0xFF51AFD7ED558CCDL;
    h ^= h >>> 33;
    h *= 0xC4CEB9FE1A85EC53L;
    h ^= h >>> 33;
    return (int) h ^ (int) (h >>> 32);
  }
}
