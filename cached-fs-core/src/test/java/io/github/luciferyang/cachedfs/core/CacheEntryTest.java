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

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CacheEntryTest {

  @Test
  @DisplayName("size strictly < TINY_DATA_SIZE → tinyData path")
  void tinyDataPath() {
    CacheEntry e = new CacheEntry();
    e.markExclusiveLocked();
    e.initialize(2047, false);
    assertThat(e.size()).isEqualTo(2047);
    List<ByteBuffer> ranges = e.dataRanges(2047);
    assertThat(ranges).hasSize(1);
    assertThat(ranges.get(0).isDirect()).isFalse(); // heap byte[] wrapped
    assertThat(ranges.get(0).remaining()).isEqualTo(2047);
  }

  @Test
  @DisplayName("size == TINY_DATA_SIZE picks contiguous or pages, not tiny")
  void thresholdBoundaryNotTiny() {
    CacheEntry e = new CacheEntry();
    e.markExclusiveLocked();
    e.initialize(CacheEntry.TINY_DATA_SIZE, true);
    List<ByteBuffer> ranges = e.dataRanges(CacheEntry.TINY_DATA_SIZE);
    assertThat(ranges).hasSize(1);
    assertThat(ranges.get(0).isDirect()).isTrue();
  }

  @Test
  @DisplayName("contiguous=true → single direct buffer")
  void contiguousPath() {
    CacheEntry e = new CacheEntry();
    e.markExclusiveLocked();
    e.initialize(8192, true);
    List<ByteBuffer> ranges = e.dataRanges(8192);
    assertThat(ranges).hasSize(1);
    assertThat(ranges.get(0).isDirect()).isTrue();
    assertThat(ranges.get(0).capacity()).isGreaterThanOrEqualTo(8192);
  }

  @Test
  @DisplayName("non-contiguous → page run summing to size, last page exact remainder")
  void nonContiguousPath() {
    CacheEntry e = new CacheEntry();
    e.markExclusiveLocked();
    e.initialize(10000, false);
    List<ByteBuffer> ranges = e.dataRanges(10000);
    assertThat(ranges).hasSize(3); // ceil(10000/4096) = 3
    int total = ranges.stream().mapToInt(ByteBuffer::remaining).sum();
    assertThat(total).isEqualTo(10000);
    assertThat(ranges.get(2).capacity()).isEqualTo(10000 - 2 * CacheEntry.PAGE_SIZE);
  }

  @Test
  @DisplayName("initialize twice throws")
  void initializeTwiceThrows() {
    CacheEntry e = new CacheEntry();
    e.markExclusiveLocked();
    e.initialize(4096, false);
    assertThatThrownBy(() -> e.initialize(4096, false)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("initialize with size <= 0 throws")
  void initializeNonPositiveThrows() {
    CacheEntry e = new CacheEntry();
    e.markExclusiveLocked();
    assertThatThrownBy(() -> e.initialize(0, false)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> e.initialize(-1, false)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("dataRanges(0) returns empty list across all storage paths")
  void dataRangesZero() {
    for (boolean contiguous : new boolean[] {true, false}) {
      CacheEntry e = new CacheEntry();
      e.markExclusiveLocked();
      e.initialize(8192, contiguous);
      assertThat(e.dataRanges(0)).isEmpty();
    }
    CacheEntry tiny = new CacheEntry();
    tiny.markExclusiveLocked();
    tiny.initialize(100, false);
    assertThat(tiny.dataRanges(0)).isEmpty();
  }

  @Test
  @DisplayName("dataRanges(length>size) throws")
  void dataRangesOversizeThrows() {
    CacheEntry e = new CacheEntry();
    e.markExclusiveLocked();
    e.initialize(1024, false);
    assertThatThrownBy(() -> e.dataRanges(2048)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("freeData drops references and zeroes size")
  void freeDataDropsReferences() {
    CacheEntry e = new CacheEntry();
    e.markExclusiveLocked();
    e.initialize(8192, false);
    e.freeData();
    assertThat(e.size()).isZero();
    assertThat(e.dataRanges(0)).isEmpty();
  }

  @Test
  @DisplayName("setSsdFile clears ssdSaveable (velox AsyncDataCache.h:277-281 parity)")
  void setSsdFileClearsSaveable() {
    // The entry was marked saveable while RAM-only; once it becomes SSD-resident the SSD-save
    // path must not re-flush it. completeExclusive avoids re-marking, but setSsdFile is the
    // boundary that flips the flag when an entry transitions to SSD-backed.
    CacheEntry e = new CacheEntry();
    e.setSsdSaveable(true);
    e.setSsdFile(new Object(), 1024L);
    assertThat(e.ssdSaveable()).isFalse();
    assertThat(e.ssdOffset()).isEqualTo(1024L);
  }

  @Test
  @DisplayName("getAndClearFirstUseFlag returns true at most once")
  void firstUseAtomicClear() {
    CacheEntry e = new CacheEntry();
    e.setFirstUse();
    assertThat(e.getAndClearFirstUseFlag()).isTrue();
    assertThat(e.getAndClearFirstUseFlag()).isFalse();
  }
}
