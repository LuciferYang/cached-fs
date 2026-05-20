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

class SsdFileTrackerTest {

  @Test
  @DisplayName("regionRead increases score; regionFilled boosts to >= 1.1*best")
  void readAndFill() {
    SsdFileTracker t = new SsdFileTracker(4);
    t.regionRead(0, 1000);
    t.regionRead(1, 500);
    double[] s = t.testingScores();
    assertThat(s[0]).isEqualTo(1000.0);
    assertThat(s[1]).isEqualTo(500.0);
    t.regionFilled(2);
    s = t.testingScores();
    assertThat(s[2]).isGreaterThanOrEqualTo(1100.0); // 1.1 * 1000
  }

  @Test
  @DisplayName("findEvictionCandidates returns up to 3 unpinned regions with score <= avg")
  void evictionCandidates() {
    SsdFileTracker t = new SsdFileTracker(5);
    t.regionRead(0, 100);
    t.regionRead(1, 200);
    t.regionRead(2, 300);
    t.regionRead(3, 400);
    t.regionRead(4, 500);
    // avg = 300; candidates: [0(100), 1(200), 2(300)]
    int[] pins = new int[5];
    var cands = t.findEvictionCandidates(5, pins);
    assertThat(cands).hasSize(3);
    assertThat(cands).containsExactly(0, 1, 2); // sorted ascending by score
  }

  @Test
  @DisplayName("pinned regions are excluded from eviction candidates")
  void pinnedExcluded() {
    SsdFileTracker t = new SsdFileTracker(3);
    t.regionRead(0, 10);
    t.regionRead(1, 20);
    t.regionRead(2, 30);
    int[] pins = {1, 0, 0};
    var cands = t.findEvictionCandidates(3, pins);
    assertThat(cands).doesNotContain(0);
  }

  @Test
  @DisplayName("decay fires when numTouches > max(1000, totalEntries/2); numTouches resets")
  void decay() {
    SsdFileTracker t = new SsdFileTracker(2);
    t.regionRead(0, 16000);
    // Fewer than 1000 touches with totalEntries=10 — no decay yet.
    for (int i = 0; i < 500; i++) t.fileTouched(10);
    assertThat(t.testingScores()[0]).isEqualTo(16000.0);
    // Drive exactly to the decay-firing touch (#1001) and stop, so we can observe the reset.
    for (int i = 0; i < 501; i++) t.fileTouched(10);
    // After decay: 16000 * 15/16 = 15000; numTouches resets to 0 on the firing touch.
    assertThat(t.testingScores()[0]).isEqualTo(15000.0);
    assertThat(t.testingNumTouches()).isZero();
  }

  @Test
  @DisplayName("findEvictionCandidates: all pinned returns empty list")
  void allPinnedReturnsEmpty() {
    SsdFileTracker t = new SsdFileTracker(3);
    t.regionRead(0, 10);
    t.regionRead(1, 20);
    t.regionRead(2, 30);
    int[] pins = {1, 1, 1};
    assertThat(t.findEvictionCandidates(3, pins)).isEmpty();
  }

  @Test
  @DisplayName("findEvictionCandidates: numRegions=0 returns empty list")
  void zeroRegionsReturnsEmpty() {
    SsdFileTracker t = new SsdFileTracker(4);
    assertThat(t.findEvictionCandidates(0, new int[4])).isEmpty();
  }

  @Test
  @DisplayName("findEvictionCandidates: 7 regions all unpinned -> caps at 3 lowest scores")
  void sevenRegionsCapsAtThree() {
    SsdFileTracker t = new SsdFileTracker(8);
    int n = 7;
    for (int i = 0; i < n; i++) {
      t.regionRead(i, (i + 1) * 100L);
    }
    var cands = t.findEvictionCandidates(n, new int[8]);
    assertThat(cands).hasSize(3);
    assertThat(cands).containsExactly(0, 1, 2);
  }

  @Test
  @DisplayName("restoreScores rejects len > maxRegions")
  void restoreScoresValidation() {
    SsdFileTracker t = new SsdFileTracker(2);
    assertThatThrownBy(() -> t.restoreScores(new double[10], 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("len=10 > maxRegions=2");
  }
}
