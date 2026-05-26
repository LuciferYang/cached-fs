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

import io.github.luciferyang.cachedfs.core.io.ReadFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Factory producing a {@link ReadFile} for a given Hadoop {@code (FileSystem, Path, key, size)}
 * tuple. Production uses {@code HadoopReadFile::new}; tests substitute a counting wrapper via
 * {@link CacheBootstrap#setReadFileFactoryForTesting(ReadFileFactory)} to observe IO patterns
 * without standing up a full file-system fixture.
 *
 * <p>The seam was introduced for Phase 5b coalescing tests (preadv-call counting), but it is a pure
 * refactor of the prior {@code new HadoopReadFile(fs, p, key, size)} call site in {@code
 * CachedFileSystem.openHandleForKey} — no behavior change in the default path.
 */
@FunctionalInterface
public interface ReadFileFactory {

  /**
   * Builds a {@link ReadFile} that serves byte ranges from the underlying file. Implementations
   * MUST be cheap to call: the bootstrap factory is invoked once per opened handle (per {@code
   * FileHandleFactory} miss), so any per-call cost is amortized but should not block.
   */
  ReadFile create(FileSystem fs, Path p, String key, long size);
}
