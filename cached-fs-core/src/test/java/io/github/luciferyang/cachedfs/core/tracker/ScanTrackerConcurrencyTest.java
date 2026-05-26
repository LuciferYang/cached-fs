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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ScanTrackerConcurrencyTest {

  @Test
  @DisplayName("8 threads × 1M recordReference: no lost updates under CAS-loop storage")
  void recordReferenceNoLostUpdates() throws InterruptedException {
    ScanTracker tracker = new ScanTracker("scan-concurrency", 8 << 20);
    TrackingId id = TrackingId.of(42, 0);
    int threads = 8;
    int iterations = 1_000_000;

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      for (int t = 0; t < threads; t++) {
        pool.submit(
            () -> {
              try {
                start.await();
                for (int i = 0; i < iterations; i++) {
                  tracker.recordReference(id, 1L);
                }
              } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }
      start.countDown();
      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    } finally {
      pool.shutdown();
    }

    assertThat(tracker.data(id).referencedBytes()).isEqualTo((long) threads * iterations);
  }

  @Test
  @DisplayName(
      "8 writer threads + 1 reader thread: adjustedReadPct never observes negative under concurrent updates")
  void snapshotMonotonicityUnderContention() throws InterruptedException {
    ScanTracker tracker = new ScanTracker("scan-snapshot", 8 << 20);
    TrackingId id = TrackingId.of(7, 1);
    int writers = 8;
    AtomicBoolean stop = new AtomicBoolean();
    AtomicLong negativeObservations = new AtomicLong();

    ExecutorService pool = Executors.newFixedThreadPool(writers + 1);
    try {
      // Reader polls adjustedReadPct every ~1ms looking for negative snapshots.
      pool.submit(
          () -> {
            while (!stop.get()) {
              if (tracker.data(id).adjustedReadPct() < 0) {
                negativeObservations.incrementAndGet();
              }
              try {
                Thread.sleep(0, 100_000); // ~100us
              } catch (InterruptedException ignored) {
                return;
              }
            }
          });

      // Writers interleave reference + read updates to exercise both record* paths.
      for (int t = 0; t < writers; t++) {
        pool.submit(
            () -> {
              while (!stop.get()) {
                tracker.recordReference(id, 1024L);
                tracker.recordRead(id, 800L);
              }
            });
      }

      Thread.sleep(2_000);
      stop.set(true);
    } finally {
      pool.shutdown();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(negativeObservations.get()).isZero();
  }

  @Test
  @DisplayName("DISABLED tracker no-ops on recordReference/recordRead and returns EMPTY snapshot")
  void disabledTrackerNoOps() {
    ScanTracker disabled = ScanTracker.DISABLED;
    TrackingId id = TrackingId.of(0, 0);

    disabled.recordReference(id, 1_000);
    disabled.recordRead(id, 500);

    assertThat(disabled.data(id)).isEqualTo(TrackingData.EMPTY);
    assertThat(disabled.readPct(id)).isEqualTo(100); // EMPTY.readPct() is 100
    assertThat(disabled.size()).isZero();
  }

  @Test
  @DisplayName("size() reflects distinct (TrackingId) entries; increments only on first record")
  void sizeReflectsDistinctEntries() {
    ScanTracker tracker = new ScanTracker("scan-size", 8 << 20);
    assertThat(tracker.size()).isZero();

    TrackingId a = TrackingId.of(1, 0);
    TrackingId b = TrackingId.of(2, 0);

    tracker.recordReference(a, 100);
    assertThat(tracker.size()).isEqualTo(1);

    tracker.recordRead(a, 50);
    assertThat(tracker.size()).isEqualTo(1); // same id, no new entry

    tracker.recordReference(b, 100);
    assertThat(tracker.size()).isEqualTo(2);

    // EMPTY ids must NOT create entries.
    tracker.recordReference(TrackingId.EMPTY, 100);
    assertThat(tracker.size()).isEqualTo(2);
  }
}
