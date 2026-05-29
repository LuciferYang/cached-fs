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
package io.github.luciferyang.cachedfs.hadoop.spi;

import org.apache.hadoop.fs.PathHandle;

/**
 * Opt-in SPI for {@link PathHandle} implementations whose bytes are derived from the file content
 * itself (e.g. a SHA-256 of the bytes, or a connector-published immutable digest). When a {@code
 * PathHandle} implements this interface the cached-fs decorator will cache reads keyed by {@link
 * #contentHash}, so a second open of the same content hash hits the cache exactly like a repeated
 * {@code open(Path)} would.
 *
 * <p>Standard Hadoop {@code PathHandle} bytes are opaque and connector-defined — the decorator
 * cannot tell, in general, whether a given byte sequence uniquely identifies content (vs. a path
 * version, an inode, etc.). This marker interface is the contract a connector signs to say "yes,
 * these bytes are content-derived; cache by them".
 *
 * <p><b>Contract:</b>
 *
 * <ul>
 *   <li>Two {@code ContentAddressedPathHandle}s with byte-equal {@link #contentHash} arrays MUST
 *       refer to byte-equal file content. Different content MUST produce different hashes. The
 *       decorator relies on this for safe cache reuse — a collision returns stale content.
 *   <li>{@link #contentLength} MUST be the file's exact size in bytes. The decorator uses it as the
 *       cache entry's upper bound and to short-circuit reads past EOF; a wrong length leaks reads
 *       past the real file end into the cache.
 *   <li>Implementations SHOULD make {@link #contentHash} cheap (already-computed bytes, not a
 *       per-call hash); the decorator may read it multiple times per open.
 * </ul>
 *
 * <p>Connectors that DON'T implement this interface keep the previous behavior — {@code
 * openFile(PathHandle)} / {@code open(PathHandle, int)} delegate to the inner FS unchanged.
 */
public interface ContentAddressedPathHandle extends PathHandle {

  /**
   * Returns the content-derived hash that uniquely identifies this file's bytes. The decorator
   * caches reads keyed by these bytes; see the class-level contract for collision requirements.
   *
   * <p>Implementations SHOULD return a defensive copy if the underlying buffer is mutable — the
   * decorator does not copy the result before hashing it into a cache key.
   */
  byte[] contentHash();

  /** Returns the file's exact length in bytes. See the class-level contract for accuracy. */
  long contentLength();
}
