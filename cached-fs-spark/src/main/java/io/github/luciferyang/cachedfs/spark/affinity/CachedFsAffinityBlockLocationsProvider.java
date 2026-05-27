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
    if (nativeHosts.length > 0 && mgr.shouldDeferToNativeLocality(nativeHosts)) {
      return underlying;
    }
    String[] preferred =
        CachedFsAffinity.getPreferredLocations(status.getPath().toString(), nativeHosts);
    if (preferred.length == 0) {
      return underlying;
    }
    // Rewrite per underlying block, preserving (offset, length) so callers that subdivide a file
    // along block boundaries (HDFS FileInputFormat.getSplits, planners that consult per-block
    // ranges) keep the natural split points. Hosts are replaced uniformly because the affinity
    // hint is a whole-file decision; per-block heterogeneity would require a different API. When
    // the inner FS returns no blocks at all (object stores), synthesize one spanning [start, len).
    if (underlying == null || underlying.length == 0) {
      return new BlockLocation[] {hostsOnly(preferred, start, len)};
    }
    BlockLocation[] out = new BlockLocation[underlying.length];
    for (int i = 0; i < underlying.length; i++) {
      BlockLocation src = underlying[i];
      out[i] = hostsOnly(preferred, src.getOffset(), src.getLength());
    }
    return out;
  }

  /**
   * Builds a fresh {@link BlockLocation} whose {@code hosts} carries the executor-shaped task
   * locations, with {@code names} aligned to the same length so the informal {@code names.length ==
   * hosts.length} parallel-array invariant some Hadoop consumers rely on holds.
   */
  private static BlockLocation hostsOnly(String[] preferred, long offset, long length) {
    String[] names = new String[preferred.length];
    for (int i = 0; i < preferred.length; i++) {
      names[i] = preferred[i];
    }
    return new BlockLocation(names, preferred, offset, length);
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
      } catch (java.io.IOException ex) {
        // BlockLocation.getHosts() declares IOException for historical reasons; the default
        // impl never throws. If a custom inner FS does throw, log + treat as no native hint
        // for this block — better than swallowing silently while building a partial host set
        // that would mislead the minTargetHosts short-circuit.
        org.slf4j.LoggerFactory.getLogger(CachedFsAffinityBlockLocationsProvider.class)
            .warn("BlockLocation.getHosts threw — treating block as no-locality", ex);
      }
    }
    return hosts.toArray(new String[0]);
  }
}
