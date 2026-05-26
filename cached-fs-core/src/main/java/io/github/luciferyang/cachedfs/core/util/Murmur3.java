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
package io.github.luciferyang.cachedfs.core.util;

/**
 * MurmurHash3 finalizer.
 *
 * <p>Used by {@code fileNumHash} to uniformly distribute sequential file ids (assigned from 0 by
 * {@code StringIdMap}) across the {@link io.github.luciferyang.cachedfs.core.tracker.TrackingId}
 * 29-bit bucket space. A raw xor-fold of a sequential int yields the input unchanged, producing
 * zero collisions until ids exceed 2^29 — the birthday-paradox analysis in the reader-glue plan
 * assumes uniform distribution, which {@code fmix32} provides.
 */
public final class Murmur3 {

  private Murmur3() {}

  /** MurmurHash3 32-bit finalizer; distributes ints uniformly across the 32-bit space. */
  public static int fmix32(int h) {
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }
}
