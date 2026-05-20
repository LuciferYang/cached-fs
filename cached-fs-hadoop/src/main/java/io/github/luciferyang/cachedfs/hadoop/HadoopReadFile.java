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
import org.apache.hadoop.fs.Path;

/**
 * {@link ReadFile} backed by a Hadoop {@link FSDataInputStream}. All reads use the positioned-read
 * API so concurrent {@code pread} calls on different offsets do not race on the stream's internal
 * position (matches velox's preference for {@code pread} over seek+read).
 */
public final class HadoopReadFile implements ReadFile {

  private final FileSystem fs;
  private final Path path;
  private final String identity;
  private final long size;

  /**
   * Lazily opened on first read; {@code volatile} so the unlocked fast-path check in {@link
   * #openIfNeeded()} sees publication writes from the slow path. Plain visibility is not enough — a
   * reader observing a non-null reference could otherwise see a partially-constructed stream.
   */
  private volatile FSDataInputStream stream;

  private final Object streamLock = new Object();

  /**
   * Closed flag guarded by {@link #streamLock}. Once true, {@link #openIfNeeded()} refuses to
   * reopen the stream — otherwise a concurrent {@code preadv} arriving after the owning {@link
   * io.github.luciferyang.cachedfs.core.handle.FileHandle} eviction would resurrect a fresh {@link
   * FSDataInputStream} with no remaining owner, leaking the underlying connection.
   */
  private boolean closed;

  /**
   * @param fs Hadoop filesystem to delegate to
   * @param path absolute path of the file
   * @param identity stable URI string used as the cache key (typically {@code path.toUri()})
   * @param size pre-fetched file length so {@link #size()} does not require a stat call
   */
  public HadoopReadFile(FileSystem fs, Path path, String identity, long size) {
    this.fs = Objects.requireNonNull(fs, "fs");
    this.path = Objects.requireNonNull(path, "path");
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
        // Heap buffer — go through the byte[] path which uses readFully on PositionedReadable.
        int arrOff = dst.arrayOffset() + dst.position();
        pread(cursor, dst.array(), arrOff, remaining);
        dst.position(dst.position() + remaining);
      } else {
        // Direct buffer — readFully(byte[]) won't work. Stage via heap and copy.
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
        // A concurrent FileHandle eviction may have closed us between the caller picking up
        // the ReadFile reference and reaching openIfNeeded. Refusing to reopen here is what
        // prevents a leaked, never-closed FSDataInputStream from being created post-eviction.
        throw new ClosedChannelException();
      }
      if (stream == null) {
        stream = fs.open(path);
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
