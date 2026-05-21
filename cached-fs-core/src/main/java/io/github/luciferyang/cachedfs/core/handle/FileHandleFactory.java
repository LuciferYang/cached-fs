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

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Open-handle LRU keyed by a stable string identifier (typically a fully-qualified file URI). Built
 * on top of {@link CachedFactory}, so it inherits single-flight generation: concurrent requests for
 * the same key share one open call rather than racing to open the same file twice.
 *
 * <p>Mirrors velox {@code FileHandleFactory}.
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls.
 */
public final class FileHandleFactory {

  private final CachedFactory<String, FileHandle> delegate;

  /**
   * @param capacity LRU capacity (count of open handles, NOT bytes — matches velox's {@code
   *     FileHandleSizer = 1})
   * @param opener function that opens a handle for a given key. Called at most once per key per LRU
   *     lifetime.
   */
  public FileHandleFactory(int capacity, Function<String, FileHandle> opener) {
    this.delegate = new CachedFactory<>(capacity, Objects.requireNonNull(opener, "opener"));
  }

  /**
   * Returns a pinned handle for {@code key}. The returned {@link CachedFactory.CachedPtr} must be
   * closed (try-with-resources) to release the pin so the LRU can evict the entry later.
   */
  public CachedFactory.CachedPtr<String, FileHandle> open(String key) {
    return delegate.generate(Objects.requireNonNull(key, "key"));
  }

  /** Returns the number of currently cached handles. */
  public int size() {
    return delegate.size();
  }

  /**
   * Drains every cached handle and closes it. Called by the owning FileSystem decorator on shutdown
   * (and by test cleanup) so input streams pinned inside FileHandles are released while their inner
   * FS is still alive. Throws an aggregated {@link IOException} if any handle close throws, but
   * always attempts every close.
   */
  public void closeAll() throws IOException {
    closeDrained(delegate.drain());
  }

  /**
   * Drains and closes only the handles whose key matches {@code predicate}. Used by a single
   * decorator (e.g. one of several {@code CachedFileSystem} instances in a multi-scheme JVM) to
   * release just its own handles without disturbing peers' entries.
   *
   * <p>May block briefly while in-flight openers for matching keys settle — see {@link
   * CachedFactory#drainMatching}.
   *
   * <p><b>Pinned handles are drained too</b> — same contract as {@link #closeAll}. A caller still
   * mid-read on a pinned handle will see an {@code IOException} on its next read once the handle is
   * closed under it; close-while-reading is a fundamental Hadoop FS race and this method makes no
   * attempt to wait out active readers.
   */
  public void closeMatching(Predicate<String> predicate) throws IOException {
    Objects.requireNonNull(predicate, "predicate");
    closeDrained(delegate.drainMatching(predicate));
  }

  private void closeDrained(java.util.List<FileHandle> handles) throws IOException {
    IOException primary = null;
    for (FileHandle h : handles) {
      try {
        h.close();
      } catch (IOException ex) {
        if (primary == null) primary = ex;
        else primary.addSuppressed(ex);
      } catch (RuntimeException ex) {
        IOException wrapped = new IOException(ex);
        if (primary == null) primary = wrapped;
        else primary.addSuppressed(wrapped);
      }
    }
    if (primary != null) {
      throw primary;
    }
  }
}
