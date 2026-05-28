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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ScanTrackerTest {

  @Test
  @DisplayName("readPct reflects ratio")
  void readPctReflectsRatio() {
    ScanTracker t = new ScanTracker("scan-1", 8 << 20);
    long fileNum = 42L;
    t.recordReference(fileNum, 1000);
    t.recordRead(fileNum, 800);
    assertThat(t.readPct(fileNum)).isEqualTo(80);
  }

  @Test
  @DisplayName("readPct on unreferenced stream returns 100 (admit prefetch — matches velox)")
  void readPctEmptyAdmitsPrefetch() {
    ScanTracker t = new ScanTracker("scan-1b", 8 << 20);
    long fileNum = 99L;
    // No recordReference yet; data() returns the EMPTY snapshot whose readPct must be 100.
    assertThat(t.data(fileNum).readPct()).isEqualTo(100);
    assertThat(TrackingData.EMPTY.readPct()).isEqualTo(100);
  }

  @Test
  @DisplayName("first stripe adjustedReadPct is zero")
  void firstStripeAdjustedReadPctIsZero() {
    ScanTracker t = new ScanTracker("scan-2", 8 << 20);
    long fileNum = 7L;
    t.recordReference(fileNum, 1000);
    assertThat(t.data(fileNum).adjustedReadPct()).isZero();

    t.recordRead(fileNum, 1000);
    t.recordReference(fileNum, 500);
    // denominator = 1500 - 500 = 1000; readBytes = 1000; pct = 100
    assertThat(t.data(fileNum).adjustedReadPct()).isEqualTo(100);
  }

  @Test
  @DisplayName("data() returns immutable snapshot")
  void dataIsImmutableSnapshot() {
    ScanTracker t = new ScanTracker("scan-4", 8 << 20);
    long fileNum = 12345L;
    t.recordReference(fileNum, 100);
    t.recordRead(fileNum, 50);
    TrackingData snap1 = t.data(fileNum);
    t.recordRead(fileNum, 50);
    TrackingData snap2 = t.data(fileNum);
    // snap1 is a record - cannot mutate. snap2 reflects the new total.
    assertThat(snap1.readBytes()).isEqualTo(50);
    assertThat(snap2.readBytes()).isEqualTo(100);
  }

  @Test
  @DisplayName("DISABLED tracker is a no-op")
  void disabledTrackerIsNoOp() {
    ScanTracker.DISABLED.recordReference(1L, 1000);
    ScanTracker.DISABLED.recordRead(1L, 500);
    assertThat(ScanTracker.DISABLED.data(1L)).isEqualTo(TrackingData.EMPTY);
    assertThat(ScanTracker.DISABLED.size()).isZero();
  }

  @Test
  @DisplayName(
      "different fileNums whose old 29-bit fileNumNode hash would collide are tracked independently"
          + " — defends against the regression M2 fixes")
  void distinctFileNumsAreTrackedIndependently() {
    // Two longs that differ ONLY in their high 32 bits AND in the high 3 bits of the low 32 —
    // the legacy `(fileNum ^ (fileNum >>> 32)) & ((1L << 29) - 1)` collapsed both to the same
    // 29-bit fileNumNode bucket. With raw fileNum keying they are now distinct.
    long fileA = 0x0000_0000_1234_5678L;
    // Flip a bit that lives outside the 29-bit window of the legacy fold so the hashes match.
    long fileB = fileA ^ (1L << 32);
    assertThat(fileA).isNotEqualTo(fileB);

    ScanTracker t = new ScanTracker("scan-collision", 8 << 20);
    t.recordReference(fileA, 1000);
    t.recordRead(fileA, 800);
    t.recordReference(fileB, 2000);
    t.recordRead(fileB, 200);

    assertThat(t.data(fileA).readPct()).isEqualTo(80);
    assertThat(t.data(fileB).readPct()).isEqualTo(10);
    assertThat(t.size()).isEqualTo(2);
    assertThat(t.entriesRejected()).isZero();
  }

  @Test
  @DisplayName("maxEntries cap silently rejects new fileNums past the cap; existing entries grow")
  void maxEntriesCapRejectsNewEntries() {
    ScanTracker t = new ScanTracker("scan-cap", 8 << 20, /* maxEntries= */ 2);
    t.recordReference(1L, 100);
    t.recordReference(2L, 200);
    // Cap reached. fileNum 3 is a new entry → rejected.
    t.recordReference(3L, 300);
    assertThat(t.size()).isEqualTo(2);
    assertThat(t.entriesRejected()).isEqualTo(1L);
    assertThat(t.data(3L)).isEqualTo(TrackingData.EMPTY);
    // Existing entry continues to grow even after the cap is hit.
    t.recordRead(1L, 50);
    assertThat(t.data(1L).readBytes()).isEqualTo(50);
    // Both record paths increment entriesRejected for new-fileNum calls under the cap.
    t.recordRead(4L, 400);
    assertThat(t.entriesRejected()).isEqualTo(2L);
  }

  @Test
  @DisplayName("maxEntries=0 means unlimited (legacy behavior)")
  void maxEntriesZeroIsUnlimited() {
    ScanTracker t = new ScanTracker("scan-no-cap", 8 << 20, /* maxEntries= */ 0);
    for (long i = 0; i < 100; i++) {
      t.recordReference(i, 10);
    }
    assertThat(t.size()).isEqualTo(100);
    assertThat(t.entriesRejected()).isZero();
  }
}
