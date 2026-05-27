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
package io.github.luciferyang.cachedfs.hadoop;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;

/**
 * Optional hook that rewrites the {@link BlockLocation} array returned by {@link
 * org.apache.hadoop.fs.FileSystem#getFileBlockLocations(FileStatus, long, long)} on the cached-fs
 * decorator's read path. Use cases:
 *
 * <ul>
 *   <li>Soft-affinity scheduling for Spark — see the {@code cached-fs-spark} module's installer
 *       that points the {@code BlockLocation.hosts} at executor-shaped task locations.
 *   <li>Test fixtures that inject deterministic locality hints.
 * </ul>
 *
 * <p>Installed via {@link CacheBootstrap#setBlockLocationsProvider(BlockLocationsProvider)}; at
 * most one provider is active per JVM at a time. {@code null} (the default) disables the hook
 * entirely.
 *
 * <p>This interface lives in {@code cached-fs-hadoop} so the integration is Hadoop-only; downstream
 * modules (e.g. {@code cached-fs-spark}) wire concrete providers without making cached-fs-hadoop
 * depend on Spark.
 */
@FunctionalInterface
public interface BlockLocationsProvider {

  /**
   * Returns the {@link BlockLocation}s the decorator should hand back to the caller.
   *
   * @param status the file's {@link FileStatus} as seen by the decorator
   * @param start byte offset of the region the caller asked about
   * @param len byte length of the region the caller asked about
   * @param underlying the inner FS's {@link BlockLocation}s for {@code (status, start, len)} —
   *     never {@code null}; returned to callers unchanged when this provider chooses to defer
   * @return the rewritten array (may share elements with {@code underlying}); never {@code null}
   */
  BlockLocation[] getBlockLocations(
      FileStatus status, long start, long len, BlockLocation[] underlying);
}
