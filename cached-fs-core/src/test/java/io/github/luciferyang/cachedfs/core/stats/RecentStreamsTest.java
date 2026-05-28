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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RecentStreamsTest {

  private static IoStatisticsSnapshot snap(long tag) {
    // Use the closedAtNanos field as a payload tag — easier than synthesizing 23 distinct values.
    return new IoStatisticsSnapshot(
        tag, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
  }

  @Test
  @DisplayName("snapshot returns most-recent-first up to capacity")
  void mostRecentFirst() {
    RecentStreams ring = new RecentStreams(3);
    ring.add(snap(1));
    ring.add(snap(2));
    ring.add(snap(3));

    List<IoStatisticsSnapshot> view = ring.snapshot();
    assertThat(view).hasSize(3);
    assertThat(view.get(0).closedAtNanos()).isEqualTo(3); // newest first
    assertThat(view.get(1).closedAtNanos()).isEqualTo(2);
    assertThat(view.get(2).closedAtNanos()).isEqualTo(1);
  }

  @Test
  @DisplayName("ring wraps: oldest overwritten when capacity is exceeded")
  void ringWraps() {
    RecentStreams ring = new RecentStreams(3);
    for (long i = 1; i <= 5; i++) {
      ring.add(snap(i));
    }
    List<IoStatisticsSnapshot> view = ring.snapshot();
    assertThat(view).hasSize(3);
    // Newest three (5, 4, 3) survive; 1 and 2 were overwritten by 4 and 5.
    assertThat(view.get(0).closedAtNanos()).isEqualTo(5);
    assertThat(view.get(1).closedAtNanos()).isEqualTo(4);
    assertThat(view.get(2).closedAtNanos()).isEqualTo(3);
    assertThat(ring.addedTotal()).isEqualTo(5L);
  }

  @Test
  @DisplayName("capacity 0 → DISABLED behavior: adds swallowed, snapshot empty")
  void disabled() {
    RecentStreams ring = new RecentStreams(0);
    ring.add(snap(1));
    ring.add(snap(2));
    assertThat(ring.capacity()).isZero();
    assertThat(ring.snapshot()).isEmpty();
    // addedTotal stays at 0 — short-circuit means writeIndex isn't bumped.
    assertThat(ring.addedTotal()).isZero();
  }

  @Test
  @DisplayName("RecentStreams.DISABLED sentinel matches the capacity-0 contract")
  void disabledSentinel() {
    assertThat(RecentStreams.DISABLED.capacity()).isZero();
    RecentStreams.DISABLED.add(snap(1));
    assertThat(RecentStreams.DISABLED.snapshot()).isEmpty();
  }

  @Test
  @DisplayName("negative capacity rejected at construction")
  void negativeCapacityRejected() {
    assertThatThrownBy(() -> new RecentStreams(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("-1");
  }

  @Test
  @DisplayName("concurrent adds don't lose updates; addedTotal == cumulative writes")
  void concurrentAdds() throws InterruptedException {
    int writers = 8;
    int iterations = 10_000;
    RecentStreams ring = new RecentStreams(32);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(writers);
      for (int w = 0; w < writers; w++) {
        final int tag = w;
        pool.submit(
            () -> {
              try {
                start.await();
                for (int i = 0; i < iterations; i++) {
                  ring.add(snap((long) tag * iterations + i));
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
    assertThat(ring.addedTotal()).isEqualTo((long) writers * iterations);
    assertThat(ring.snapshot()).hasSize(32);
  }
}
