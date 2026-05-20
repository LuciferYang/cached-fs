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
package io.github.luciferyang.cachedfs.core.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Read-only file abstraction the cache uses to fetch bytes from the underlying storage. Mirrors
 * velox's {@code ReadFile} interface.
 *
 * <p>Implementations adapt:
 *
 * <ul>
 *   <li>{@code cached-fs-hadoop}'s {@code HadoopReadFile} — wraps a Hadoop {@code
 *       FSDataInputStream} and prefers {@code PositionedReadable.readVectored} when the delegate
 *       stream supports it
 *   <li>tests / non-Hadoop embedders may provide trivial {@code FileChannel}-based impls
 * </ul>
 */
public interface ReadFile {

  /** Total file size in bytes. */
  long size() throws IOException;

  /** Stable identity string used for the cache key. Typically a fully-qualified URI. */
  String identity();

  /** Reads exactly {@code length} bytes starting at {@code offset} into a fresh byte array. */
  byte[] pread(long offset, int length) throws IOException;

  /**
   * Reads exactly {@code length} bytes starting at {@code offset} into {@code dst} starting at
   * {@code dstOffset}.
   */
  void pread(long offset, byte[] dst, int dstOffset, int length) throws IOException;

  /**
   * Vectored read — fills the supplied {@code buffers} with bytes starting at {@code offset}. The
   * sum of the buffers' remaining capacities defines the total length read. Mirrors velox's {@code
   * preadv} usage.
   */
  void preadv(long offset, List<ByteBuffer> buffers) throws IOException;

  /** Closes the file. Does not affect the cache. */
  void close() throws IOException;
}
