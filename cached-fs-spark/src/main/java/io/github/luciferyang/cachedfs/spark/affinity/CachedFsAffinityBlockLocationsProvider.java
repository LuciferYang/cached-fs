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

import io.github.luciferyang.cachedfs.hadoop.BlockLocationsProvider;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;

/**
 * {@link BlockLocationsProvider} that rewrites the inner FS's locality hint with executor-scoped
 * preferred locations from {@link CachedFsSoftAffinityManager}. Installed by the Spark extension at
 * session start.
 *
 * <p>Behavior per call:
 *
 * <ul>
 *   <li>Manager disabled or ring empty → return {@code underlying} unchanged.
 *   <li>Native locality already satisfies the {@code minTargetHosts} threshold → return {@code
 *       underlying} unchanged (don't stomp better HDFS locality).
 *   <li>Otherwise → return a SINGLE {@link BlockLocation} spanning {@code [start, start + len)}
 *       with the executor hostnames as its {@code hosts} array. Spark's {@code TaskLocation.apply}
 *       parses {@code "executor_<host>_<execId>"} into {@code ExecutorCacheTaskLocation} for
 *       PROCESS_LOCAL placement.
 * </ul>
 *
 * <p>The {@code hosts} string is the only field Spark consults from {@code BlockLocation}; the
 * {@code names} and {@code topologyPaths} fields are passed through empty to keep the
 * representation honest about its non-HDFS origin.
 */
public final class CachedFsAffinityBlockLocationsProvider implements BlockLocationsProvider {

  @Override
  public BlockLocation[] getBlockLocations(
      FileStatus status, long start, long len, BlockLocation[] underlying) {
    CachedFsSoftAffinityManager mgr = CachedFsSoftAffinityManager.getInstance();
    if (!mgr.isEnabled() || mgr.executorCount() == 0) {
      return underlying;
    }
    if (status == null || status.getPath() == null) {
      return underlying;
    }
    String[] nativeHosts = collectHosts(underlying);
    if (mgr.shouldDeferToNativeLocality(nativeHosts)) {
      return underlying;
    }
    String[] preferred =
        CachedFsAffinity.getPreferredLocations(status.getPath().toString(), nativeHosts);
    if (preferred.length == 0) {
      return underlying;
    }
    // Single span block — preferredLocations are advisory and Spark doesn't subdivide the file
    // via this hook. Per-split granularity is handled by the feedback-mode path on top of
    // CachedFsAffinity.getPreferredLocations(splits, nativeHosts).
    return new BlockLocation[] {
      new BlockLocation(/* names */ new String[0], preferred, start, len)
    };
  }

  /** Collects the union of inner-FS hosts. Deduped + null-safe; returns empty when unset. */
  private static String[] collectHosts(BlockLocation[] underlying) {
    if (underlying == null || underlying.length == 0) return new String[0];
    Set<String> hosts = new LinkedHashSet<>();
    for (BlockLocation b : underlying) {
      try {
        String[] hs = b.getHosts();
        if (hs == null) continue;
        for (String h : hs) {
          if (h != null && !h.isEmpty()) hosts.add(h);
        }
      } catch (java.io.IOException ignored) {
        // BlockLocation.getHosts() is declared with IOException, but the default impl never
        // throws. Defensive catch so a misbehaving Hadoop FS doesn't bubble through our planner.
      }
    }
    return hosts.toArray(new String[0]);
  }
}
