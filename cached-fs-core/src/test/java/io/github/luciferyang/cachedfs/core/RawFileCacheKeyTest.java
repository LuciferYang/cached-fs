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

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RawFileCacheKeyTest {

  @Test
  void hashSpreadsAcross16Buckets() {
    // Verifies the (fileNum, offset) hash distributes adjacent quanta of one file across shards
    // — this is what differentiates the RAM tier from the SSD tier (velox §3.3 vs §4.1).
    Set<Integer> buckets = new HashSet<>();
    for (long off = 0; off < 64L * 8 * 1024 * 1024; off += 8L * 1024 * 1024) {
      buckets.add(new RawFileCacheKey(42L, off).hashCode() & 15);
    }
    // With 8 quanta we expect at least 4 distinct bucket indices in any reasonable hash.
    assertThat(buckets).hasSizeGreaterThanOrEqualTo(4);
  }

  @Test
  void equalKeysHashEqual() {
    var a = new RawFileCacheKey(7L, 1024L);
    var b = new RawFileCacheKey(7L, 1024L);
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }
}
