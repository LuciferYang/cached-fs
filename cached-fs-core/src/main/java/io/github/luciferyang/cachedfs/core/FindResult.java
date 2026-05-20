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

import java.util.concurrent.CompletableFuture;

/**
 * Result of {@link CacheShard#findOrCreate} or {@link CacheShard#find}. Sealed — callers must use
 * {@code switch} pattern matching to handle all three shapes:
 *
 * <pre>{@code
 * switch (result) {
 *   case FindResult.Hit h -> consume(h.pin());
 *   case FindResult.Exclusive ex -> {
 *     try (CachePin p = ex.pin()) {
 *       fill(p.entry());
 *       p.exclusiveToShared(false).close();
 *     }
 *   }
 *   case FindResult.Waiting w -> w.future().thenRun(() -> retry(...));
 * }
 * }</pre>
 *
 * <p><b>Thread safety:</b> records are immutable and safe for unrestricted sharing; the {@link
 * CachePin}s they wrap are owned by one logical caller (see {@link CachePin}).
 */
public sealed interface FindResult {

  /** Cache hit. {@code pin} is shared and must be closed by the caller. */
  record Hit(CachePin pin) implements FindResult {}

  /** Cache miss. Caller fills data, then promotes via {@code pin.exclusiveToShared(...)}. */
  record Exclusive(CachePin pin) implements FindResult {}

  /** Another thread is filling. Caller awaits {@code future}, then retries {@code findOrCreate}. */
  record Waiting(CompletableFuture<Void> future) implements FindResult {}

  // Internal factories for the cache. Not part of the public API contract — callers should
  // never construct FindResult directly.
  static FindResult hit(CachePin pin) {
    return new Hit(pin);
  }

  static FindResult exclusive(CachePin pin) {
    return new Exclusive(pin);
  }

  static FindResult waiting(CompletableFuture<Void> future) {
    return new Waiting(future);
  }

  /**
   * Returns the pin held by this result. <strong>Throws</strong> if this is a {@link Waiting}
   * result — callers must handle Waiting before calling this.
   */
  default CachePin pin() {
    return switch (this) {
      case Hit h -> h.pin();
      case Exclusive e -> e.pin();
      case Waiting w ->
          throw new IllegalStateException("pin() on Waiting; await waitFuture() first");
    };
  }

  /** Returns the wait future. <strong>Throws</strong> if this is not a {@link Waiting} result. */
  default CompletableFuture<Void> waitFuture() {
    if (this instanceof Waiting w) return w.future();
    throw new IllegalStateException("waitFuture() on " + getClass().getSimpleName());
  }
}
