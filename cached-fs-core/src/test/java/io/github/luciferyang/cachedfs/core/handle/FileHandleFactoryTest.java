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

import io.github.luciferyang.cachedfs.core.id.StringIdLease;
import io.github.luciferyang.cachedfs.core.id.StringIdMap;
import io.github.luciferyang.cachedfs.core.io.ReadFile;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FileHandleFactoryTest {

  static final class NoopReadFile implements ReadFile {
    final String id;

    NoopReadFile(String id) {
      this.id = id;
    }

    @Override
    public long size() {
      return 0;
    }

    @Override
    public String identity() {
      return id;
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
    public void close() {}
  }

  @Test
  @DisplayName("single-flight: concurrent opens of same key share one open call")
  void singleFlightOpens() throws Exception {
    StringIdMap map = new StringIdMap();
    AtomicInteger opens = new AtomicInteger();
    FileHandleFactory f =
        new FileHandleFactory(
            16,
            key -> {
              opens.incrementAndGet();
              return new FileHandle(
                  new NoopReadFile(key), new StringIdLease(map, key), new StringIdLease(map, "/"));
            });
    try (var p1 = f.open("file://a");
        var p2 = f.open("file://a")) {
      assertThat(p1.value()).isSameAs(p2.value());
    }
    assertThat(opens.get()).isEqualTo(1);
  }
}
