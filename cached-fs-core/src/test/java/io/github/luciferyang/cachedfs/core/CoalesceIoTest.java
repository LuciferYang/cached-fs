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

import io.github.luciferyang.cachedfs.core.CoalesceIo.CoalesceIoStats;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class CoalesceIoTest {

  @Test
  void mergesAdjacentRangesIntoOneIo() throws Exception {
    long[] offsets = {0, 1024, 2048};
    int[] sizes = {1024, 1024, 1024};

    CoalesceIoStats stats =
        CoalesceIo.coalesce(
            3,
            i -> offsets[i],
            i -> sizes[i],
            i -> List.of(ByteBuffer.allocate(sizes[i])),
            /* maxGap= */ 4096,
            /* maxBatch= */ 16,
            (offset, buffers) -> {
              // Single coalesced read covers offsets [0, 3072).
              assertThat(offset).isZero();
            });
    assertThat(stats.numIos()).isEqualTo(1);
    assertThat(stats.payloadBytes()).isEqualTo(3072L);
    assertThat(stats.extraBytes()).isZero();
  }

  @Test
  void splitsWhenGapExceedsBudget() throws Exception {
    long[] offsets = {0, 100_000};
    int[] sizes = {1024, 1024};
    var ranges = new ArrayList<Long>();

    CoalesceIo.coalesce(
        2,
        i -> offsets[i],
        i -> sizes[i],
        i -> List.of(ByteBuffer.allocate(sizes[i])),
        /* maxGap= */ 25_000,
        /* maxBatch= */ 16,
        (offset, buffers) -> ranges.add(offset));

    assertThat(ranges).containsExactly(0L, 100_000L);
  }

  @Test
  void smallGapsCountAsExtraBytes() throws Exception {
    long[] offsets = {0, 2048};
    int[] sizes = {1024, 1024};

    CoalesceIoStats stats =
        CoalesceIo.coalesce(
            2,
            i -> offsets[i],
            i -> sizes[i],
            i -> List.of(ByteBuffer.allocate(sizes[i])),
            /* maxGap= */ 4096,
            /* maxBatch= */ 16,
            (offset, buffers) -> {});
    // Gap = 2048 - 1024 = 1024 bytes wasted.
    assertThat(stats.numIos()).isEqualTo(1);
    assertThat(stats.extraBytes()).isEqualTo(1024L);
    assertThat(stats.payloadBytes()).isEqualTo(2048L);
  }
}
