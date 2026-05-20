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
package io.github.luciferyang.cachedfs.core.ssd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Test-only {@link FileChannel} wrapper that delegates to a real channel for reads + metadata but
 * throws {@link IOException} with the message {@code "No space left on device"} on writes. Used to
 * exercise the SSD shard's ENOSPC handling paths without filling the actual disk.
 */
final class FaultyFileChannel extends FileChannel {

  private final FileChannel delegate;
  private final boolean failGrow; // when true, only fail the lazy-grow sparse write
  private final long growThreshold; // grow detected when position >= this value

  /** Fails on every {@code write}. */
  static FaultyFileChannel failAllWrites(FileChannel delegate) {
    return new FaultyFileChannel(delegate, false, 0L);
  }

  /** Fails only when writing at position {@code >= threshold} (used for lazy-grow ENOSPC). */
  static FaultyFileChannel failGrowAt(FileChannel delegate, long threshold) {
    return new FaultyFileChannel(delegate, true, threshold);
  }

  private FaultyFileChannel(FileChannel delegate, boolean failGrow, long growThreshold) {
    this.delegate = delegate;
    this.failGrow = failGrow;
    this.growThreshold = growThreshold;
  }

  private void maybeFail(long position) throws IOException {
    if (!failGrow || position >= growThreshold) {
      throw new IOException("No space left on device");
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return delegate.read(dst);
  }

  @Override
  public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
    return delegate.read(dsts, offset, length);
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    return delegate.read(dst, position);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    maybeFail(delegate.position());
    return delegate.write(src);
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
    maybeFail(delegate.position());
    return delegate.write(srcs, offset, length);
  }

  @Override
  public int write(ByteBuffer src, long position) throws IOException {
    maybeFail(position);
    return delegate.write(src, position);
  }

  @Override
  public long position() throws IOException {
    return delegate.position();
  }

  @Override
  public FileChannel position(long newPosition) throws IOException {
    return delegate.position(newPosition);
  }

  @Override
  public long size() throws IOException {
    return delegate.size();
  }

  @Override
  public FileChannel truncate(long size) throws IOException {
    return delegate.truncate(size);
  }

  @Override
  public void force(boolean metaData) throws IOException {
    delegate.force(metaData);
  }

  @Override
  public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
    return delegate.transferTo(position, count, target);
  }

  @Override
  public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
    return delegate.transferFrom(src, position, count);
  }

  @Override
  public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
    return delegate.map(mode, position, size);
  }

  @Override
  public FileLock lock(long position, long size, boolean shared) throws IOException {
    return delegate.lock(position, size, shared);
  }

  @Override
  public FileLock tryLock(long position, long size, boolean shared) throws IOException {
    return delegate.tryLock(position, size, shared);
  }

  @Override
  protected void implCloseChannel() throws IOException {
    delegate.close();
  }
}
