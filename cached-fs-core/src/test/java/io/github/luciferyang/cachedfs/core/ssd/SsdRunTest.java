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
package io.github.luciferyang.cachedfs.core.ssd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SsdRunTest {

  @Test
  @DisplayName("encode/decode round-trip preserves offset and size")
  void roundTrip() {
    SsdRun r = SsdRun.of(64L * 1024 * 1024, 4096, 0xDEADBEEF);
    assertThat(r.offset()).isEqualTo(64L * 1024 * 1024);
    assertThat(r.size()).isEqualTo(4096);
    assertThat(r.checksum()).isEqualTo(0xDEADBEEF);
  }

  @Test
  @DisplayName("size 1 → stored as 0; size MAX → stored as 0x7FFFFF")
  void sizeBoundaries() {
    SsdRun r1 = SsdRun.of(0, 1);
    assertThat(r1.size()).isEqualTo(1);
    assertThat(r1.fileBits() & SsdRun.SIZE_MASK).isZero();

    SsdRun rMax = SsdRun.of(0, SsdRun.MAX_SIZE);
    assertThat(rMax.size()).isEqualTo(SsdRun.MAX_SIZE);
    assertThat(rMax.fileBits() & SsdRun.SIZE_MASK).isEqualTo(SsdRun.SIZE_MASK);
  }

  @Test
  @DisplayName("size 0 or > MAX_SIZE rejected")
  void sizeValidation() {
    assertThatThrownBy(() -> SsdRun.of(0, 0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SsdRun.of(0, SsdRun.MAX_SIZE + 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SsdRun.of(-1, 1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("max representable offset packs without collision")
  void maxOffset() {
    // 41 bits of offset — values up to (1<<41)-1 are representable when shifted into the
    // upper 41 bits of the 64-bit fileBits.
    long maxOffset = (1L << 41) - 1;
    SsdRun r = SsdRun.of(maxOffset, 1);
    assertThat(r.offset()).isEqualTo(maxOffset);
    assertThat(r.size()).isEqualTo(1);
  }

  @Test
  @DisplayName("two-arg of(offset,size) overload sets checksum to 0")
  void twoArgOfDefaultsChecksumZero() {
    SsdRun r = SsdRun.of(0, 1);
    assertThat(r.checksum()).isZero();
  }

  @Test
  @DisplayName("offset > MAX_OFFSET (= 2^41 - 1) is rejected to avoid silent shift overflow")
  void offsetOverflowRejected() {
    assertThatThrownBy(() -> SsdRun.of(1L << 41, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("offset must be in");
    // Boundary: MAX_OFFSET itself is accepted.
    SsdRun max = SsdRun.of(SsdRun.MAX_OFFSET, 1);
    assertThat(max.offset()).isEqualTo(SsdRun.MAX_OFFSET);
  }
}
