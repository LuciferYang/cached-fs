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
package io.github.luciferyang.cachedfs.core.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class StringIdMapTest {

  @Test
  void mintsAndReusesId() {
    StringIdMap map = new StringIdMap();
    long id1 = map.makeId("a");
    long id2 = map.makeId("a");
    assertThat(id1).isEqualTo(id2);
    assertThat(map.size()).isEqualTo(1);
    assertThat(map.stringForId(id1)).isEqualTo("a");
  }

  @Test
  void leaseRefcountFreesOnClose() {
    StringIdMap map = new StringIdMap();
    StringIdLease a = new StringIdLease(map, "x");
    StringIdLease b = new StringIdLease(map, "x");
    assertThat(map.size()).isEqualTo(1);
    a.close();
    assertThat(map.size()).isEqualTo(1);
    b.close();
    assertThat(map.size()).isZero();
  }

  @Test
  void emptyLeaseIsNoOp() {
    StringIdMap map = new StringIdMap();
    try (StringIdLease empty = StringIdLease.empty(map)) {
      assertThat(empty.id()).isEqualTo(StringIdLease.EMPTY_ID);
      assertThat(empty.hasValue()).isFalse();
    }
    assertThat(map.size()).isZero();
  }

  @Test
  void recoverIdReusesAndConflictDetected() {
    StringIdMap map = new StringIdMap();
    long recovered = map.recoverId(42L, "path/A");
    assertThat(recovered).isEqualTo(42L);
    // recovering same (id, name) again just adds a reference
    assertThat(map.recoverId(42L, "path/A")).isEqualTo(42L);
    assertThatThrownBy(() -> map.recoverId(42L, "path/B"))
        .isInstanceOf(IllegalStateException.class);
    // recovering same name with a different id throws
    assertThatThrownBy(() -> map.recoverId(99L, "path/A"))
        .isInstanceOf(IllegalStateException.class);
    assertThat(map.highWaterMark()).isGreaterThanOrEqualTo(42L);
  }

  @Test
  void newMakeIdSkipsOverRecoveredHighWater() {
    StringIdMap map = new StringIdMap();
    map.recoverId(1000L, "old");
    long fresh = map.makeId("new");
    assertThat(fresh).isGreaterThan(1000L);
  }
}
