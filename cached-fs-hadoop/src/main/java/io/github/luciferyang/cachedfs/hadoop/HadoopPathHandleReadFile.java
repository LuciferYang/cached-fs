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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathHandle;

/**
 * {@link ReadFile} that opens its inner {@link FSDataInputStream} via {@link
 * FileSystem#open(PathHandle, int)} instead of {@code FileSystem.open(Path)}. Used by the
 * decorator's {@code openFile(PathHandle)} / {@code open(PathHandle, int)} cached path when the
 * supplied handle implements {@link
 * io.github.luciferyang.cachedfs.hadoop.spi.ContentAddressedPathHandle}.
 *
 * <p>Behavior otherwise mirrors {@link HadoopReadFile}: positioned reads only, lazy stream open
 * under a lock, refusal to re-open after {@link #close}.
 */
public final class HadoopPathHandleReadFile implements ReadFile {

  private final FileSystem fs;
  private final PathHandle pathHandle;
  private final int bufferSize;
  private final String identity;
  private final long size;

  private volatile FSDataInputStream stream;
  private final Object streamLock = new Object();
  private boolean closed;

  /**
   * @param fs Hadoop filesystem the {@code pathHandle} originated from
   * @param pathHandle handle captured at open time; held by the cached entry so subsequent cache
   *     misses on the same content hash can re-open without help from the original caller
   * @param bufferSize forwarded to {@link FileSystem#open(PathHandle, int)} when the stream is
   *     materialized
   * @param identity stable cache key (typically {@code "cah://<dec-id>/<hex-contenthash>"})
   * @param size content length advertised by {@link
   *     io.github.luciferyang.cachedfs.hadoop.spi.ContentAddressedPathHandle#contentLength}
   */
  public HadoopPathHandleReadFile(
      FileSystem fs, PathHandle pathHandle, int bufferSize, String identity, long size) {
    this.fs = Objects.requireNonNull(fs, "fs");
    this.pathHandle = Objects.requireNonNull(pathHandle, "pathHandle");
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("bufferSize must be > 0: " + bufferSize);
    }
    this.bufferSize = bufferSize;
    this.identity = Objects.requireNonNull(identity, "identity");
    if (size < 0) {
      throw new IllegalArgumentException("size must be >= 0: " + size);
    }
    this.size = size;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public String identity() {
    return identity;
  }

  @Override
  public byte[] pread(long offset, int length) throws IOException {
    byte[] buf = new byte[length];
    pread(offset, buf, 0, length);
    return buf;
  }

  @Override
  public void pread(long offset, byte[] dst, int dstOffset, int length) throws IOException {
    if (length == 0) {
      return;
    }
    checkRange(offset, length);
    FSDataInputStream s = openIfNeeded();
    s.readFully(offset, dst, dstOffset, length);
  }

  @Override
  public void preadv(long offset, List<ByteBuffer> buffers) throws IOException {
    long cursor = offset;
    for (ByteBuffer dst : buffers) {
      int remaining = dst.remaining();
      if (remaining == 0) continue;
      if (dst.hasArray()) {
        int arrOff = dst.arrayOffset() + dst.position();
        pread(cursor, dst.array(), arrOff, remaining);
        dst.position(dst.position() + remaining);
      } else {
        byte[] tmp = pread(cursor, remaining);
        dst.put(tmp);
      }
      cursor += remaining;
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (streamLock) {
      closed = true;
      if (stream != null) {
        try {
          stream.close();
        } finally {
          stream = null;
        }
      }
    }
  }

  private FSDataInputStream openIfNeeded() throws IOException {
    FSDataInputStream s = stream;
    if (s != null) return s;
    synchronized (streamLock) {
      if (closed) {
        throw new ClosedChannelException();
      }
      if (stream == null) {
        stream = fs.open(pathHandle, bufferSize);
      }
      return stream;
    }
  }

  private void checkRange(long offset, int length) {
    if (offset < 0) {
      throw new IllegalArgumentException("offset must be >= 0: " + offset);
    }
    if ((long) offset + length > size) {
      throw new IllegalArgumentException(
          "read past EOF: offset=" + offset + " length=" + length + " size=" + size);
    }
  }
}
