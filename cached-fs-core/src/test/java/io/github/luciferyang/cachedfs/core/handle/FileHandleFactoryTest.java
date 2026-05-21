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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FileHandleFactoryTest {

  static class NoopReadFile implements ReadFile {
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
    public void close() throws IOException {}
  }

  @Test
  @DisplayName("repeated sequential opens of the same key share one open call (LRU hit path)")
  void repeatedOpensShareOneOpenCall() throws Exception {
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

  @Test
  @DisplayName("single-flight: truly concurrent opens of the same key dedupe through pending/await")
  void singleFlightOpensConcurrent() throws Exception {
    StringIdMap map = new StringIdMap();
    AtomicInteger opens = new AtomicInteger();
    // Block the first opener inside generate() so the second thread is forced through
    // CachedFactory's pending/pendingCv path, not the plain LRU lookup.
    CountDownLatch openerReleased = new CountDownLatch(1);
    CountDownLatch openerEntered = new CountDownLatch(1);
    FileHandleFactory f =
        new FileHandleFactory(
            16,
            key -> {
              opens.incrementAndGet();
              openerEntered.countDown();
              try {
                openerReleased.await(5, TimeUnit.SECONDS);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
              }
              return new FileHandle(
                  new NoopReadFile(key), new StringIdLease(map, key), new StringIdLease(map, "/"));
            });

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<CachedFactory.CachedPtr<String, FileHandle>> first =
          pool.submit(() -> f.open("file://k"));
      // Wait until the first opener is in flight (in `pending`), then dispatch the racer so it
      // must wait on pendingCv rather than picking up a published LRU entry.
      assertThat(openerEntered.await(5, TimeUnit.SECONDS)).isTrue();
      Future<CachedFactory.CachedPtr<String, FileHandle>> second =
          pool.submit(() -> f.open("file://k"));

      openerReleased.countDown();

      try (var p1 = first.get(5, TimeUnit.SECONDS);
          var p2 = second.get(5, TimeUnit.SECONDS)) {
        assertThat(p1.value()).isSameAs(p2.value());
      }
    } finally {
      pool.shutdownNow();
    }
    assertThat(opens.get()).isEqualTo(1);
  }

  @Test
  @DisplayName("closeAll: drains every cached handle and closes each ReadFile")
  void closeAllDrainsAndCloses() throws Exception {
    StringIdMap map = new StringIdMap();
    AtomicInteger closes = new AtomicInteger();
    FileHandleFactory f =
        new FileHandleFactory(
            16,
            key ->
                new FileHandle(
                    new NoopReadFile(key) {
                      @Override
                      public void close() throws IOException {
                        closes.incrementAndGet();
                      }
                    },
                    new StringIdLease(map, key),
                    new StringIdLease(map, "/")));
    f.open("file://a").close();
    f.open("file://b").close();
    assertThat(f.size()).isEqualTo(2);

    f.closeAll();

    assertThat(f.size()).isZero();
    assertThat(closes.get()).isEqualTo(2);
  }

  @Test
  @DisplayName("closeAll: aggregates close failures via suppressed exceptions")
  void closeAllAggregatesFailures() {
    StringIdMap map = new StringIdMap();
    FileHandleFactory f =
        new FileHandleFactory(
            16,
            key ->
                new FileHandle(
                    new NoopReadFile(key) {
                      @Override
                      public void close() throws IOException {
                        throw new IOException("boom-" + identity());
                      }
                    },
                    new StringIdLease(map, key),
                    new StringIdLease(map, "/")));
    f.open("file://a").close();
    f.open("file://b").close();

    assertThatThrownBy(f::closeAll)
        .isInstanceOf(IOException.class)
        .matches(ex -> ex.getSuppressed().length >= 1);
    assertThat(f.size()).isZero();
  }
}
