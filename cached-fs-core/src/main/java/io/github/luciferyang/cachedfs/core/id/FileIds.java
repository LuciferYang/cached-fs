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
package io.github.luciferyang.cachedfs.core.id;

/**
 * Process-wide singleton holder for the {@link StringIdMap} used as the cache's identity layer.
 *
 * <p>Mirrors velox's {@code fileIds()} accessor (FileIds.h:22). The singleton is exposed as a
 * static accessor; tests may swap it via {@link #setInstance} but must restore it afterwards.
 */
public final class FileIds {

  private static volatile StringIdMap instance = new StringIdMap();

  private FileIds() {}

  /** Returns the process-wide {@link StringIdMap}. */
  public static StringIdMap fileIds() {
    return instance;
  }

  /**
   * Replaces the process-wide instance. Test-only — production code must never call this. The
   * caller is responsible for ensuring no live cache entries reference the old instance.
   */
  public static void setInstance(StringIdMap newInstance) {
    if (newInstance == null) {
      throw new IllegalArgumentException("newInstance must not be null");
    }
    instance = newInstance;
  }
}
