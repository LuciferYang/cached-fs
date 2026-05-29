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
package io.github.luciferyang.cachedfs.hadoop.mbean;

/**
 * JMX MBean exposing live read-only state and a few destructive operations on the cached-fs
 * bootstrap. The {@code MXBean} suffix means every method's types are JMX open types: the scalar
 * ops use String / long (rendered directly by JConsole / jmxterm) and {@link #counters} returns a
 * {@code Map<String,Long>} (rendered as a TabularData). The cached-fs CLI connects over standard
 * JMX RMI — via an {@code MXBean} proxy, so the Map round-trips back to a Map — to operate a
 * running JVM.
 *
 * <p>All methods are thread-safe; reads observe a coherent point-in-time view (counters are read
 * atomically), and mutating operations are guarded by the bootstrap's own internal locks.
 */
public interface CachedFsBootstrapMXBean {

  /**
   * Returns a multi-line human-readable report for {@code path}: whether an open handle exists for
   * that exact key, the total open-handle count, and a per-scan breakdown (scanId, tracked-entry
   * count, rejected-entry count) for every live tracker.
   *
   * <p>It does NOT resolve {@code path} to a fileNum or report per-path bytes: doing so would
   * require a side-effecting {@code StringIdMap.makeId}, and the cache keys trackers by fileNum
   * with no reverse path lookup. It also does NOT enumerate cached chunks per offset — the RAM
   * cache is shard-hashed by {@code CacheKey} for O(1) lookup, not enumeration. A future revision
   * MAY add per-chunk reporting if the {@code AsyncDataCache} grows an enumeration API.
   */
  String inspect(String path);

  /**
   * Returns a multi-line stats dump: AggregatedIoStatistics totals, RAM cache stats, SSD cache
   * stats (if present), tracker counts, recent-streams ring counters. Counters are point-in-time;
   * deltas over a window are the CLI's job (two snapshots {@code N} seconds apart).
   */
  String stats();

  /**
   * Returns the same numeric values as {@link #stats} but as a typed name → value map, so a caller
   * can compute deltas/rates over a window without parsing the formatted text. Keys are namespaced
   * ({@code io.*}, {@code ram.*}, {@code handles.*}, {@code trackers.*}, {@code recent-streams.*}).
   * Cumulative counters (e.g. {@code io.read-bytes}) yield meaningful per-second rates;
   * point-in-time gauges (e.g. {@code ram.num-entries}) yield a change-over-window that the caller
   * may present differently.
   *
   * <p>The map is built insertion-ordered ({@code LinkedHashMap}) to mirror the {@code stats()}
   * layout. That order is preserved by the standard platform MBean server's {@code TabularData}
   * round-trip but is NOT guaranteed by the JMX spec across exotic implementations; callers that
   * need a fixed order should sort. Row order is cosmetic only — each entry is self-keyed, so
   * values never desync from their names.
   *
   * <p>{@code Map<String, Long>} is a JMX open type — JConsole renders it as a TabularData and an
   * {@code MXBean} proxy reconstructs it back into a {@code Map}.
   */
  java.util.Map<String, Long> counters();

  /**
   * Returns a multi-line dump of the most-recent stream snapshots (oldest-discarded ring buffer)
   * with at most {@code limit} entries. Pass {@code 0} for "all retained". Pass a positive cap when
   * you want only the freshest. Each line summarizes one {@code IoStatisticsSnapshot}.
   */
  String recentStreams(int limit);

  /**
   * Closes every cached file handle. Returns the number of handles that were active. Reads
   * in-flight finish from already-pinned chunks; new opens after {@code drain} repopulate the
   * cache. RAM cache entries are NOT cleared by this call (they age out via the existing LRU /
   * pressure paths).
   */
  long drain();

  /**
   * Closes every cached file handle whose key matches the {@code regex} pattern. Returns the number
   * of keys that matched when the purge was issued — a best-effort count, not a guaranteed
   * closed-count: the underlying close primitive is {@code void}, and a concurrent open or eviction
   * between the match snapshot and the close can shift the true number. Use {@code .*} to mean
   * "everything" (equivalent to {@link #drain}). An invalid regex throws {@link
   * java.util.regex.PatternSyntaxException} which JMX wraps as {@code MBeanException}.
   */
  long purgeMatching(String regex);
}
