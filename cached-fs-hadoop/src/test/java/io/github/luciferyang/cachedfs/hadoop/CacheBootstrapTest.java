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
package io.github.luciferyang.cachedfs.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Covers the scheme+authority opener registry on {@link CacheBootstrap}: who can register, who gets
 * dispatched, and how removal affects siblings. Real-FS dispatch (open() calls flowing through the
 * registry to the right inner FS) is covered by {@code CachedFileSystemTest} and the HDFS / S3A
 * integration tests.
 */
class CacheBootstrapTest {

  @AfterEach
  void teardown() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  @Test
  @DisplayName("installOpener throws when bootstrap has not been installed yet")
  void installOpenerRequiresBootstrap() {
    assertThatThrownBy(() -> CacheBootstrap.installOpener("hdfs://nn-a", key -> null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not installed");
  }

  @Test
  @DisplayName("removeOpener is a no-op (returns false) when bootstrap is uninstalled")
  void removeOpenerNoopWithoutBootstrap() {
    assertThat(CacheBootstrap.removeOpener("hdfs://nn-a")).isFalse();
  }

  @Test
  @DisplayName(
      "multiple endpoints register independently — removing one leaves the others untouched")
  void registerAndRemoveMultipleEndpoints() throws IOException {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();

    CacheBootstrap.installOpener("hdfs://nn-a", key -> null);
    CacheBootstrap.installOpener("s3a://bucket-x", key -> null);
    CacheBootstrap.installOpener("bos://bucket.bj.bcebos.com", key -> null);

    assertThat(b.hasOpener("hdfs://nn-a")).isTrue();
    assertThat(b.hasOpener("s3a://bucket-x")).isTrue();
    assertThat(b.hasOpener("bos://bucket.bj.bcebos.com")).isTrue();
    assertThat(b.hasOpener("hdfs://nn-b")).isFalse();

    assertThat(CacheBootstrap.removeOpener("hdfs://nn-a")).isTrue();
    assertThat(b.hasOpener("hdfs://nn-a")).isFalse();
    assertThat(b.hasOpener("s3a://bucket-x")).isTrue();
    assertThat(b.hasOpener("bos://bucket.bj.bcebos.com")).isTrue();

    // removeOpener is idempotent — second removal of the same endpoint returns false.
    assertThat(CacheBootstrap.removeOpener("hdfs://nn-a")).isFalse();
  }

  @Test
  @DisplayName("installOpener refuses to overwrite an existing registration for the same endpoint")
  void installOpenerRefusesOverwrite() throws IOException {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();

    CacheBootstrap.HandleOpener first = key -> null;
    CacheBootstrap.HandleOpener second = key -> null;
    CacheBootstrap.installOpener("s3a://bucket-x", first);
    // Two decorators sharing scheme://authority would silently nuke each other's handles via the
    // shared close predicate. The registry enforces "one live decorator per endpoint" — the
    // second install throws with a clear remediation message.
    assertThatThrownBy(() -> CacheBootstrap.installOpener("s3a://bucket-x", second))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("already registered")
        .hasMessageContaining("s3a://bucket-x");
    // First registration is intact; second must close the prior decorator before re-registering.
    assertThat(b.hasOpener("s3a://bucket-x")).isTrue();
    assertThat(CacheBootstrap.removeOpener("s3a://bucket-x", first)).isTrue();
    assertThat(b.hasOpener("s3a://bucket-x")).isFalse();
    // After clean removal a fresh decorator can register again.
    CacheBootstrap.installOpener("s3a://bucket-x", second);
    assertThat(b.hasOpener("s3a://bucket-x")).isTrue();
  }

  @Test
  @DisplayName("installOpener is idempotent when called with the exact same opener reference")
  void installOpenerIdempotentSameReference() throws IOException {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();
    CacheBootstrap.HandleOpener opener = key -> null;
    CacheBootstrap.installOpener("hdfs://nn-a", opener);
    // Re-installing the SAME opener instance (e.g. a defensive retry in initialize) must be safe;
    // only a DIFFERENT opener for an already-claimed endpoint is rejected.
    CacheBootstrap.installOpener("hdfs://nn-a", opener);
    assertThat(b.hasOpener("hdfs://nn-a")).isTrue();
  }

  @Test
  @DisplayName("endpointKey formats authority variants consistently")
  void endpointKeyFormatting() {
    assertThat(CacheBootstrap.endpointKey(URI.create("hdfs://nn:9000/foo")))
        .isEqualTo("hdfs://nn:9000");
    assertThat(CacheBootstrap.endpointKey(URI.create("s3a://bucket-x/")))
        .isEqualTo("s3a://bucket-x");
    assertThat(CacheBootstrap.endpointKey(URI.create("bos://bucket.bj.bcebos.com/")))
        .isEqualTo("bos://bucket.bj.bcebos.com");
    // file:/// has no authority — endpoint is just the scheme prefix.
    assertThat(CacheBootstrap.endpointKey(URI.create("file:///tmp/"))).isEqualTo("file://");
    assertThatThrownBy(() -> CacheBootstrap.endpointKey(URI.create("/no/scheme")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("no scheme");
  }

  @Test
  @DisplayName("uninstallForTesting clears all registered openers")
  void uninstallClearsRegistry() throws IOException {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap.installOpener("hdfs://nn-a", key -> null);
    CacheBootstrap.installOpener("s3a://bucket-x", key -> null);
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();
    assertThat(b.hasOpener("hdfs://nn-a")).isTrue();

    CacheBootstrap.uninstallForTesting();

    // Re-installing should yield an empty registry — the old map didn't leak across uninstall.
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap fresh = CacheBootstrap.get().orElseThrow();
    assertThat(fresh.hasOpener("hdfs://nn-a")).isFalse();
    assertThat(fresh.hasOpener("s3a://bucket-x")).isFalse();
  }

  // --- Phase 5a: scanId / ScanTracker / aggregateIoStats ----------------

  @Test
  @DisplayName("trackerFor normalizes null/empty/whitespace to 'default' and caches by scanId")
  void trackerForNormalizesAndCaches() throws IOException {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();

    var t1 = b.trackerFor(null);
    var t2 = b.trackerFor("");
    var t3 = b.trackerFor("  ");
    var t4 = b.trackerFor("default");
    assertThat(t1).isSameAs(t2).isSameAs(t3).isSameAs(t4);
    assertThat(t1.scanId()).isEqualTo("default");

    var distinct = b.trackerFor("scan-A");
    assertThat(distinct).isNotSameAs(t1);
    assertThat(b.scanTrackerCount()).isEqualTo(2);
  }

  @Test
  @DisplayName("withScanId close path: ThreadLocal cleared AND tracker evicted")
  void withScanIdCloseEvictsTracker() throws Exception {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();
    try (var ignored = b.withScanId("scan-evict")) {
      b.trackerFor("scan-evict"); // materialize the entry
      assertThat(b.currentScanId()).isEqualTo("scan-evict");
      assertThat(b.scanTrackerCount()).isEqualTo(1);
    }
    assertThat(b.currentScanId()).isNull();
    assertThat(b.scanTrackerCount()).isZero();
    assertThat(b.aggregateIoStats().staleScanIdRecoveries()).isZero();
  }

  @Test
  @DisplayName(
      "stale slot triggers WARN-recover: prior tracker evicted, counter ticks, new slot set")
  void staleSlotRecovery() throws Exception {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();
    // Simulate a prior task that crashed mid-scope (slot set, never closed).
    AutoCloseable orphan = b.withScanId("scan-A");
    b.trackerFor("scan-A"); // materialize so we can observe eviction
    assertThat(b.scanTrackerCount()).isEqualTo(1);

    // Next withScanId on the same thread auto-recovers.
    try (var ignored = b.withScanId("scan-B")) {
      assertThat(b.currentScanId()).isEqualTo("scan-B");
      // "scan-A" was evicted by the recovery path.
      assertThat(b.scanTrackerCount()).isZero();
      assertThat(b.aggregateIoStats().staleScanIdRecoveries()).isEqualTo(1);
    }
    // Drop the orphan AutoCloseable. Its close() removes "scan-A" (no-op now) and clears the slot.
    orphan.close();
    assertThat(b.currentScanId()).isNull();
  }

  @Test
  @DisplayName("releaseCurrentScanId clears the ThreadLocal slot without evicting the tracker")
  void releaseCurrentScanIdPreservesTracker() throws Exception {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();
    try (var ignored = b.withScanId("outer")) {
      b.trackerFor("outer");
      assertThat(b.currentScanId()).isEqualTo("outer");
      b.releaseCurrentScanId();
      assertThat(b.currentScanId()).isNull();
      // Tracker entry MUST still be present — releaseCurrentScanId is slot-only.
      assertThat(b.scanTrackerCount()).isEqualTo(1);
      try (var nested = b.withScanId("inner")) {
        assertThat(b.currentScanId()).isEqualTo("inner");
        // No stale-slot recovery should fire because the slot was already cleared.
        assertThat(b.aggregateIoStats().staleScanIdRecoveries()).isZero();
      }
    }
    // Outer close evicts "outer" via removeScanTracker; inner already evicted itself.
    assertThat(b.scanTrackerCount()).isZero();
  }

  @Test
  @DisplayName("releaseCurrentScanId is idempotent — safe on missing slot, double-call no-ops")
  void releaseCurrentScanIdIdempotent() throws IOException {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();
    b.releaseCurrentScanId();
    b.releaseCurrentScanId();
    assertThat(b.currentScanId()).isNull();
  }

  @Test
  @DisplayName("removeScanTracker is idempotent and thread-safe (callable off the owning thread)")
  void removeScanTrackerIdempotent() throws Exception {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();
    b.removeScanTracker("ghost"); // never created
    b.trackerFor("scan-tcl");
    assertThat(b.scanTrackerCount()).isEqualTo(1);
    // Simulate a TCL on a different thread.
    Thread other = new Thread(() -> b.removeScanTracker("scan-tcl"));
    other.start();
    other.join();
    assertThat(b.scanTrackerCount()).isZero();
    b.removeScanTracker("scan-tcl"); // second call is a no-op
  }

  @Test
  @DisplayName(
      "scanTrackerEntries / scanTrackerMaxEntries aggregate across trackers; respond to record*")
  void scanTrackerEntriesGauges() throws IOException {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();
    assertThat(b.scanTrackerEntries()).isZero();
    assertThat(b.scanTrackerMaxEntries()).isZero();

    var a = b.trackerFor("scan-A");
    var c = b.trackerFor("scan-C");
    a.recordReference(io.github.luciferyang.cachedfs.core.tracker.TrackingId.of(1, 0), 100);
    a.recordReference(io.github.luciferyang.cachedfs.core.tracker.TrackingId.of(2, 0), 100);
    c.recordReference(io.github.luciferyang.cachedfs.core.tracker.TrackingId.of(3, 0), 100);

    assertThat(b.scanTrackerEntries()).isEqualTo(3); // 2 + 1
    assertThat(b.scanTrackerMaxEntries()).isEqualTo(2); // max(2, 1)
  }

  // --- Phase 5c: prefetch executor lifecycle ---------------------------

  @Test
  @DisplayName("prefetchExecutor() returns a live executor when fs.cached.prefetch.enabled=true")
  void prefetchExecutorPresentByDefault() throws IOException {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();
    java.util.concurrent.ThreadPoolExecutor exec = b.prefetchExecutor();
    assertThat(exec).isNotNull();
    assertThat(exec.isShutdown()).isFalse();
    assertThat(exec.getMaximumPoolSize()).isPositive();
  }

  @Test
  @DisplayName("prefetchExecutor() returns null when fs.cached.prefetch.enabled=false")
  void prefetchExecutorNullWhenDisabled() throws IOException {
    Configuration conf = minimalConf();
    conf.setBoolean(CachedFsConfig.PREFETCH_ENABLED, false);
    CacheBootstrap.installIfNeeded(conf);
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();
    assertThat(b.prefetchExecutor()).isNull();
  }

  @Test
  @DisplayName("uninstallForTesting shuts the prefetch executor down")
  void uninstallShutsExecutor() throws IOException {
    CacheBootstrap.installIfNeeded(minimalConf());
    java.util.concurrent.ThreadPoolExecutor exec =
        CacheBootstrap.get().orElseThrow().prefetchExecutor();
    assertThat(exec.isShutdown()).isFalse();

    CacheBootstrap.uninstallForTesting();

    assertThat(exec.isShutdown()).isTrue();
    assertThat(exec.isTerminated()).isTrue();
  }

  private static Configuration minimalConf() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, LocalFileSystem.class.getName());
    return conf;
  }
}
