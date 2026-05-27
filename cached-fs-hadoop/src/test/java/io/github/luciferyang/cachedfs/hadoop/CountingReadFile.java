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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only {@link ReadFile} that counts {@code preadv} invocations and total bytes served. Used by
 * coalesce tests to assert "exactly N preadv calls for M chunks" without standing up a remote
 * filesystem.
 */
final class CountingReadFile implements ReadFile {

  private final ReadFile delegate;
  private final AtomicInteger preadvCalls = new AtomicInteger();
  private final AtomicLong preadvBytes = new AtomicLong();

  CountingReadFile(ReadFile delegate) {
    this.delegate = delegate;
  }

  @Override
  public long size() throws IOException {
    return delegate.size();
  }

  @Override
  public String identity() {
    return delegate.identity();
  }

  @Override
  public byte[] pread(long offset, int length) throws IOException {
    return delegate.pread(offset, length);
  }

  @Override
  public void pread(long offset, byte[] dst, int dstOffset, int length) throws IOException {
    delegate.pread(offset, dst, dstOffset, length);
  }

  @Override
  public void preadv(long offset, List<ByteBuffer> buffers) throws IOException {
    preadvCalls.incrementAndGet();
    long total = 0L;
    for (ByteBuffer b : buffers) {
      total += b.remaining();
    }
    preadvBytes.addAndGet(total);
    delegate.preadv(offset, buffers);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  int preadvCalls() {
    return preadvCalls.get();
  }

  long preadvBytes() {
    return preadvBytes.get();
  }
}
