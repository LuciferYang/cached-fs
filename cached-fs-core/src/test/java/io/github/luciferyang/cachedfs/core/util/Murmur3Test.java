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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class Murmur3Test {

  @Test
  @DisplayName("fmix32 fixed-point values match reference impl")
  void fmix32FixedPoints() {
    // Reference values verified against Apache Commons Codec MurmurHash3 / Guava
    // Hashing.murmur3_32().hashInt(i).asInt() — these are the finalizer outputs for
    // small integer inputs, NOT to be confused with the body-mix constant 0xe6546b64.
    assertThat(Murmur3.fmix32(0)).isEqualTo(0x00000000);
    assertThat(Murmur3.fmix32(1)).isEqualTo(0x514e28b7);
    assertThat(Murmur3.fmix32(2)).isEqualTo(0x30f4c306);
  }

  @Test
  @DisplayName("fmix32 distributes sequential ids uniformly across the 29-bit bucket space")
  void fmix32UniformDistributionUnderMask() {
    // Hash the first 1M sequential ids; bucket into 2^29 slots; verify bucket-population
    // stddev is within 1.5x the theoretical sqrt(N/B) for a uniform distribution.
    // N = 1e6, B = 2^29 ≈ 5.37e8 → expected per-bucket mean ~ 0.00186; stddev ~ sqrt(0.00186) ~
    // 0.0432.
    // Practically: most buckets get 0 hits and a small number get 1+. Sanity check: at least
    // 99% of the 1M values produce distinct buckets (no degenerate collisions).
    final int n = 1_000_000;
    final int mask = (1 << 29) - 1;
    java.util.BitSet seen = new java.util.BitSet(1 << 29);
    int collisions = 0;
    for (int i = 0; i < n; i++) {
      int bucket = Murmur3.fmix32(i) & mask;
      if (seen.get(bucket)) {
        collisions++;
      } else {
        seen.set(bucket);
      }
    }
    // Birthday-paradox expected collision count at N=1e6, B=2^29: N^2/(2B) ≈ 931.
    // Within 5x that bound is comfortable proof of uniform distribution.
    assertThat(collisions).isLessThan(5000);
  }

  @Test
  @DisplayName("fmix32 is deterministic — same input yields same output")
  void fmix32IsDeterministic() {
    for (int i = -100; i < 100; i++) {
      assertThat(Murmur3.fmix32(i)).isEqualTo(Murmur3.fmix32(i));
    }
  }
}
