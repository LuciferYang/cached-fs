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
package io.github.luciferyang.cachedfs.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CachePinTest {

  @Test
  @DisplayName("close() on empty pin is no-op")
  void closeEmptyPinIsNoOp() {
    CachePin.empty().close();
    CachePin.empty().close(); // again
    assertThat(CachePin.empty().isEmpty()).isTrue();
  }

  @Test
  @DisplayName("close() is idempotent — pin count drops by exactly 1")
  void closeIdempotent() {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey key = new RawFileCacheKey(1L, 0L);
      FindResult miss = cache.findOrCreate(key, 4096, false);
      CachePin pin = miss.pin().exclusiveToShared(false);
      assertThat(pin.entry().numPins()).isEqualTo(1);
      pin.close();
      pin.close(); // second close — no-op, no IllegalState
      assertThat(cache.find(key)).isPresent(); // entry still cached
    }
  }

  @Test
  @DisplayName("exclusiveToShared returns shared pin and detaches the original")
  void exclusiveToSharedDetaches() {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey key = new RawFileCacheKey(2L, 0L);
      FindResult miss = cache.findOrCreate(key, 4096, false);
      CachePin original = miss.pin();
      CachePin shared = original.exclusiveToShared(false);
      assertThat(shared.isShared()).isTrue();
      assertThat(original.isEmpty()).isTrue(); // detached
      original.close(); // safe (no-op)
      shared.close();
    }
  }

  @Test
  @DisplayName("exclusiveToShared on shared pin throws")
  void exclusiveToSharedOnSharedThrows() {
    try (AsyncDataCache cache = new AsyncDataCache(AsyncDataCache.Options.defaults())) {
      RawFileCacheKey key = new RawFileCacheKey(3L, 0L);
      CachePin shared = cache.findOrCreate(key, 4096, false).pin().exclusiveToShared(false);
      assertThatThrownBy(() -> shared.exclusiveToShared(false))
          .isInstanceOf(IllegalStateException.class);
      shared.close();
    }
  }
}
