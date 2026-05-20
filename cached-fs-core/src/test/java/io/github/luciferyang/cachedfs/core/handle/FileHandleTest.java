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
package io.github.luciferyang.cachedfs.core.handle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.luciferyang.cachedfs.core.id.StringIdLease;
import io.github.luciferyang.cachedfs.core.id.StringIdMap;
import io.github.luciferyang.cachedfs.core.io.ReadFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FileHandleTest {

  static final class FakeReadFile implements ReadFile {
    final AtomicBoolean closed = new AtomicBoolean();

    @Override
    public long size() {
      return 0;
    }

    @Override
    public String identity() {
      return "fake://x";
    }

    @Override
    public byte[] pread(long offset, int length) {
      return new byte[length];
    }

    @Override
    public void pread(long offset, byte[] dst, int dstOffset, int length) {}

    @Override
    public void preadv(long offset, List<ByteBuffer> buffers) {}

    @Override
    public void close() {
      closed.set(true);
    }
  }

  static final class ThrowingReadFile implements ReadFile {
    @Override
    public long size() {
      return 0;
    }

    @Override
    public String identity() {
      return "throwing://x";
    }

    @Override
    public byte[] pread(long offset, int length) {
      return new byte[length];
    }

    @Override
    public void pread(long offset, byte[] dst, int dstOffset, int length) {}

    @Override
    public void preadv(long offset, List<ByteBuffer> buffers) {}

    @Override
    public void close() throws IOException {
      throw new IOException("boom");
    }
  }

  @Test
  @DisplayName("close() releases all three resources")
  void closeReleasesAll() throws Exception {
    StringIdMap map = new StringIdMap();
    FakeReadFile rf = new FakeReadFile();
    FileHandle h =
        new FileHandle(rf, new StringIdLease(map, "/path"), new StringIdLease(map, "/dir"));
    assertThat(map.size()).isEqualTo(2);
    h.close();
    assertThat(rf.closed.get()).isTrue();
    assertThat(map.size()).isZero();
  }

  @Test
  @DisplayName("close() still releases leases even when ReadFile.close() throws")
  void closeReleasesLeasesOnReadFileFailure() {
    StringIdMap map = new StringIdMap();
    FileHandle h =
        new FileHandle(
            new ThrowingReadFile(),
            new StringIdLease(map, "/path"),
            new StringIdLease(map, "/dir"));
    assertThatThrownBy(h::close).isInstanceOf(IOException.class).hasMessageContaining("boom");
    assertThat(map.size()).isZero();
  }
}
