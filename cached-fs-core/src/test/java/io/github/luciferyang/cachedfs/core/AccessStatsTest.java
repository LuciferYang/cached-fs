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

import org.junit.jupiter.api.Test;

class AccessStatsTest {

  @Test
  void scoreReturnsIntMaxWhenLastUseZero() {
    AccessStats s = new AccessStats();
    assertThat(s.lastUse()).isZero();
    assertThat(s.score(AccessStats.now(), 1000L)).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void touchPopulatesLastUseAndIncrementsCount() {
    AccessStats s = new AccessStats();
    s.touch();
    int afterFirst = s.numUses();
    assertThat(afterFirst).isEqualTo(1);
    assertThat(s.lastUse()).isNotZero();
    s.touch();
    assertThat(s.numUses()).isEqualTo(2);
  }

  @Test
  void makeEvictableMakesScoreIntMax() {
    AccessStats s = new AccessStats();
    s.touch();
    s.touch();
    s.makeEvictable();
    assertThat(s.score(AccessStats.now(), 1000L)).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void scoreFormulaDecreasesWithMoreUses() {
    AccessStats s = new AccessStats();
    s.touch(); // numUses=1
    int now = AccessStats.now() + 100;
    int onceUsedScore = s.score(now, 1000L);
    s.touch(); // numUses=2
    s.touch(); // numUses=3
    int oftenUsedScore = s.score(now, 1000L);
    assertThat(oftenUsedScore).isLessThan(onceUsedScore);
  }
}
