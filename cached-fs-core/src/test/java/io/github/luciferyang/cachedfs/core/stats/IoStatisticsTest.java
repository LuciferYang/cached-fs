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
package io.github.luciferyang.cachedfs.core.stats;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class IoStatisticsTest {

  @Test
  @DisplayName("each inc method updates both count and sum")
  void incrementsCountAndSum() {
    IoStatistics s = new IoStatistics();
    s.incRamHit(100);
    s.incRamHit(50);
    assertThat(s.ramHit()).isEqualTo(2);
    assertThat(s.ramHitBytes()).isEqualTo(150);

    s.incRead(1024);
    s.incPrefetch(8192);
    s.incSsdRead(2048);
    s.incRawOverreadBytes(64);
    assertThat(s.read()).isEqualTo(1);
    assertThat(s.readBytes()).isEqualTo(1024);
    assertThat(s.prefetch()).isEqualTo(1);
    assertThat(s.prefetchBytes()).isEqualTo(8192);
    assertThat(s.ssdRead()).isEqualTo(1);
    assertThat(s.ssdReadBytes()).isEqualTo(2048);
    assertThat(s.rawOverreadBytes()).isEqualTo(64);

    s.incQueryThreadIoLatencyUs(10);
    s.incStorageReadLatencyUs(20);
    s.incSsdCacheReadLatencyUs(30);
    s.incCacheWaitLatencyUs(40);
    s.incCoalescedSsdLoadLatencyUs(50);
    s.incCoalescedStorageLoadLatencyUs(60);
    assertThat(s.queryThreadIoLatencyUs()).isEqualTo(10);
    assertThat(s.storageReadLatencyUs()).isEqualTo(20);
    assertThat(s.ssdCacheReadLatencyUs()).isEqualTo(30);
    assertThat(s.cacheWaitLatencyUs()).isEqualTo(40);
    assertThat(s.coalescedSsdLoadLatencyUs()).isEqualTo(50);
    assertThat(s.coalescedStorageLoadLatencyUs()).isEqualTo(60);
  }
}
