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
package io.github.luciferyang.cachedfs.core.tracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ScanTrackerTest {

  @Test
  @DisplayName("readPct reflects ratio")
  void readPctReflectsRatio() {
    ScanTracker t = new ScanTracker("scan-1", 8 << 20);
    TrackingId col = TrackingId.of(0, 1);
    t.recordReference(col, 1000);
    t.recordRead(col, 800);
    assertThat(t.readPct(col)).isEqualTo(80);
  }

  @Test
  @DisplayName("readPct on unreferenced stream returns 100 (admit prefetch — matches velox)")
  void readPctEmptyAdmitsPrefetch() {
    ScanTracker t = new ScanTracker("scan-1b", 8 << 20);
    TrackingId col = TrackingId.of(0, 2);
    // No recordReference yet; data() returns the EMPTY snapshot whose readPct must be 100.
    assertThat(t.data(col).readPct()).isEqualTo(100);
    assertThat(TrackingData.EMPTY.readPct()).isEqualTo(100);
  }

  @Test
  @DisplayName("first stripe adjustedReadPct is zero")
  void firstStripeAdjustedReadPctIsZero() {
    ScanTracker t = new ScanTracker("scan-2", 8 << 20);
    TrackingId col = TrackingId.of(0, 1);
    t.recordReference(col, 1000);
    assertThat(t.data(col).adjustedReadPct()).isZero();

    t.recordRead(col, 1000);
    t.recordReference(col, 500);
    // denominator = 1500 - 500 = 1000; readBytes = 1000; pct = 100
    assertThat(t.data(col).adjustedReadPct()).isEqualTo(100);
  }

  @Test
  @DisplayName("empty TrackingId is silently ignored")
  void emptyIdIsIgnored() {
    ScanTracker t = new ScanTracker("scan-3", 8 << 20);
    t.recordReference(TrackingId.EMPTY, 1000);
    t.recordRead(TrackingId.EMPTY, 1000);
    assertThat(t.data(TrackingId.EMPTY)).isEqualTo(TrackingData.EMPTY);
  }

  @Test
  @DisplayName("TrackingId bit packing roundtrips (29-bit node, 2-bit streamKind)")
  void trackingIdBitPacking() {
    TrackingId id = TrackingId.of(12345, 3);
    assertThat(id.node()).isEqualTo(12345);
    assertThat(id.streamKind()).isEqualTo(3);
    assertThat(TrackingId.EMPTY.isEmpty()).isTrue();
  }

  @Test
  @DisplayName("TrackingId max node value (2^29 - 1) packs and unpacks")
  void trackingIdMaxNode() {
    int maxNode = (1 << 29) - 1;
    TrackingId id = TrackingId.of(maxNode, 0);
    assertThat(id.id()).isEqualTo(maxNode << 2);
    assertThat(id.node()).isEqualTo(maxNode);
    assertThat(id.streamKind()).isZero();
  }

  @Test
  @DisplayName("TrackingId rejects out-of-range node and streamKind")
  void trackingIdRejectsOutOfRange() {
    assertThatThrownBy(() -> TrackingId.of(1 << 29, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> TrackingId.of(0, 4)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> TrackingId.of(-1, 0)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("node()/streamKind() throw on EMPTY sentinel")
  void emptyNodeAndStreamKindThrow() {
    assertThatThrownBy(TrackingId.EMPTY::node).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(TrackingId.EMPTY::streamKind).isInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("data() returns immutable snapshot")
  void dataIsImmutableSnapshot() {
    ScanTracker t = new ScanTracker("scan-4", 8 << 20);
    TrackingId col = TrackingId.of(1, 2);
    t.recordReference(col, 100);
    t.recordRead(col, 50);
    TrackingData snap1 = t.data(col);
    t.recordRead(col, 50);
    TrackingData snap2 = t.data(col);
    // snap1 is a record - cannot mutate. snap2 reflects the new total.
    assertThat(snap1.readBytes()).isEqualTo(50);
    assertThat(snap2.readBytes()).isEqualTo(100);
  }
}
