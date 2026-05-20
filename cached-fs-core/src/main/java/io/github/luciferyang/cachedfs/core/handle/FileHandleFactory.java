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

import java.util.Objects;
import java.util.function.Function;

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
}
