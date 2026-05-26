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
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CachedFileSystemTest {

  @AfterEach
  void teardown() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  @Test
  @DisplayName("decorator reads bytes equal to direct LocalFileSystem read")
  void readMatchesUnderlying(@TempDir java.nio.file.Path dir) throws IOException {
    byte[] payload = bytes(64 * 1024); // smaller than default 8 MiB loadQuantum → 1 chunk
    java.nio.file.Path file = dir.resolve("data.bin");
    Files.write(file, payload);

    try (CachedFileSystem cfs = newCfs(file.toUri(), defaultConf())) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        byte[] got = in.readAllBytes();
        assertThat(got).isEqualTo(payload);
      }
    }
  }

  @Test
  @DisplayName("decorator reads a multi-chunk file correctly (spans loadQuantum boundary)")
  void readMultiChunk(@TempDir java.nio.file.Path dir) throws IOException {
    // loadQuantum = 4096, file = 10 * 4096 + 17 bytes → spans 11 chunks
    byte[] payload = bytes(10 * 4096 + 17);
    java.nio.file.Path file = dir.resolve("big.bin");
    Files.write(file, payload);

    Configuration conf = defaultConf();
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, 4096);
    try (CachedFileSystem cfs = newCfs(file.toUri(), conf)) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 1024)) {
        byte[] got = in.readAllBytes();
        assertThat(got).isEqualTo(payload);
      }
    }
  }

  @Test
  @DisplayName("positioned reads (PositionedReadable) return correct slices")
  void positionedRead(@TempDir java.nio.file.Path dir) throws IOException {
    byte[] payload = bytes(8 * 1024);
    java.nio.file.Path file = dir.resolve("pos.bin");
    Files.write(file, payload);

    Configuration conf = defaultConf();
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, 1024);
    try (CachedFileSystem cfs = newCfs(file.toUri(), conf)) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 1024)) {
        byte[] slice = new byte[500];
        in.readFully(2500, slice);
        for (int i = 0; i < 500; i++) {
          assertThat(slice[i]).isEqualTo(payload[2500 + i]);
        }
      }
    }
  }

  @Test
  @DisplayName("second read for the same offset is served from the RAM cache")
  void cacheHitOnSecondRead(@TempDir java.nio.file.Path dir) throws IOException {
    byte[] payload = bytes(2048);
    java.nio.file.Path file = dir.resolve("hit.bin");
    Files.write(file, payload);

    Configuration conf = defaultConf();
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, 1024);
    try (CachedFileSystem cfs = newCfs(file.toUri(), conf)) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 1024)) {
        in.readFully(0, new byte[1024]);
      }
      var statsBefore = CacheBootstrap.get().orElseThrow().ramCache().refreshStats();
      try (FSDataInputStream in = cfs.open(p, 1024)) {
        byte[] dst = new byte[1024];
        in.readFully(0, dst);
        // Bytes still match — cache is transparent.
        for (int i = 0; i < 1024; i++) {
          assertThat(dst[i]).isEqualTo(payload[i]);
        }
      }
      var statsAfter = CacheBootstrap.get().orElseThrow().ramCache().refreshStats();
      // numHit grew on the second read; numNew did NOT (cache was already populated).
      assertThat(statsAfter.numHit()).isGreaterThan(statsBefore.numHit());
      assertThat(statsAfter.numNew()).isEqualTo(statsBefore.numNew());
    }
  }

  @Test
  @DisplayName("fs.cached.enabled=false makes open() a pass-through (no bootstrap install)")
  void disabledIsPassthrough(@TempDir java.nio.file.Path dir) throws IOException {
    byte[] payload = bytes(1024);
    java.nio.file.Path file = dir.resolve("pt.bin");
    Files.write(file, payload);

    Configuration conf = defaultConf();
    conf.setBoolean(CachedFsConfig.ENABLED, false);
    try (CachedFileSystem cfs = newCfs(file.toUri(), conf)) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 1024)) {
        assertThat(in.readAllBytes()).isEqualTo(payload);
      }
      // Bootstrap not installed when disabled.
      assertThat(CacheBootstrap.get()).isEmpty();
    }
  }

  @Test
  @DisplayName("missing fs.cached.inner.impl fails initialize with a clear error")
  void missingInnerImpl(@TempDir java.nio.file.Path dir) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    // Deliberately omit INNER_IMPL.
    CachedFileSystem cfs = new CachedFileSystem();
    assertThatThrownBy(() -> cfs.initialize(URI.create("file:///"), conf))
        .isInstanceOf(IOException.class)
        .hasMessageContaining(CachedFsConfig.INNER_IMPL);
  }

  @Test
  @DisplayName("getScheme/getUri reflect the inner FS (transparent decoration)")
  void schemeIsTransparent(@TempDir java.nio.file.Path dir) throws IOException {
    java.nio.file.Path file = dir.resolve("scheme.bin");
    Files.write(file, bytes(32));
    try (CachedFileSystem cfs = newCfs(file.toUri(), defaultConf())) {
      // LocalFileSystem's scheme is "file"
      assertThat(cfs.getScheme()).isEqualTo("file");
      assertThat(cfs.innerFileSystem()).isInstanceOf(LocalFileSystem.class);
    }
  }

  @Test
  @DisplayName(
      "two decorators on different scheme+authority endpoints both serve cached reads; closing one"
          + " preserves the other's opener and handles")
  void multipleEndpointsCoexist(@TempDir java.nio.file.Path dir) throws IOException {
    // Two CachedFileSystem instances, each backed by its own inner FS that reports a distinct
    // scheme+authority. They share the JVM-wide bootstrap (one RAM tier, one handle factory) but
    // register independent openers in the registry. Closing one must drain ONLY its own handles
    // and unregister ONLY its own endpoint; the peer must continue to serve reads.
    byte[] payloadA = bytes(64);
    byte[] payloadB = bytes(128);
    java.nio.file.Path fileA = dir.resolve("a.bin");
    java.nio.file.Path fileB = dir.resolve("b.bin");
    Files.write(fileA, payloadA);
    Files.write(fileB, payloadB);

    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, TestSchemeFs.class.getName());

    CachedFileSystem alpha = new CachedFileSystem();
    CachedFileSystem beta = new CachedFileSystem();
    try {
      alpha.initialize(URI.create("alpha://host-a/"), conf);
      beta.initialize(URI.create("beta://host-b/"), conf);
      CacheBootstrap b = CacheBootstrap.get().orElseThrow();

      assertThat(b.hasOpener("alpha://host-a")).isTrue();
      assertThat(b.hasOpener("beta://host-b")).isTrue();

      Path pathA = new Path("alpha://host-a" + fileA.toUri().getRawPath());
      Path pathB = new Path("beta://host-b" + fileB.toUri().getRawPath());

      try (FSDataInputStream in = alpha.open(pathA, 4096)) {
        assertThat(in.readAllBytes()).isEqualTo(payloadA);
      }
      try (FSDataInputStream in = beta.open(pathB, 4096)) {
        assertThat(in.readAllBytes()).isEqualTo(payloadB);
      }
      // One handle per endpoint sits in the shared factory.
      assertThat(b.handleFactory().size()).isEqualTo(2);

      alpha.close();

      // alpha's opener + handle are gone; beta's are untouched.
      assertThat(b.hasOpener("alpha://host-a")).isFalse();
      assertThat(b.hasOpener("beta://host-b")).isTrue();
      assertThat(b.handleFactory().size()).isEqualTo(1);

      // Beta still serves cached reads after alpha's close — proves the partial drain didn't
      // also touch beta's handle and proves the opener registry is still live for beta.
      try (FSDataInputStream in = beta.open(pathB, 4096)) {
        assertThat(in.readAllBytes()).isEqualTo(payloadB);
      }
    } finally {
      try {
        beta.close();
      } catch (IOException ignored) {
        // best-effort cleanup
      }
    }
  }

  /**
   * Test-only Hadoop FS that pretends to be a custom scheme+authority while delegating actual I/O
   * to {@link RawLocalFileSystem}. Lets tests exercise the multi-endpoint registry path without a
   * real remote filesystem.
   */
  public static final class TestSchemeFs extends RawLocalFileSystem {
    private URI customUri;

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
      this.customUri = uri;
      super.initialize(uri, conf);
    }

    @Override
    public URI getUri() {
      return customUri != null ? customUri : super.getUri();
    }

    @Override
    public String getScheme() {
      return customUri != null ? customUri.getScheme() : super.getScheme();
    }
  }

  // --- Phase 5a: tracker + IoStatistics + IOStatisticsSource wiring -----

  @Test
  @DisplayName("reads bump ScanTracker (referencedBytes + readBytes) and IoStatistics (read+bytes)")
  void readsUpdateTrackerAndStats(@TempDir java.nio.file.Path dir) throws IOException {
    byte[] payload = bytes(16 * 1024);
    java.nio.file.Path file = dir.resolve("trk.bin");
    Files.write(file, payload);

    Configuration conf = defaultConf();
    conf.set(CachedFsConfig.SCAN_ID, "scan-track");
    try (CachedFileSystem cfs = newCfs(file.toUri(), conf)) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        byte[] got = new byte[8192];
        in.readFully(got);
        // Aggregate is merged on close, so query the per-stream surface via IOStatistics.
        org.apache.hadoop.fs.statistics.IOStatistics stats = in.getIOStatistics();
        assertThat(stats).isNotNull();
        assertThat(stats.counters().get("stream_read_bytes")).isEqualTo(8192L);
        assertThat(stats.counters().get("stream_read_operations")).isEqualTo(1L);
      }
      // After close, the bootstrap aggregate has the merged values.
      CacheBootstrap b = CacheBootstrap.get().orElseThrow();
      assertThat(b.aggregateIoStats().readBytes()).isEqualTo(8192L);
      assertThat(b.aggregateIoStats().read()).isEqualTo(1L);
      // The tracker entry exists and reflects the read.
      assertThat(b.scanTrackerEntries()).isPositive();
    }
  }

  @Test
  @DisplayName("second read of the same chunk records a RAM hit on the per-stream IoStatistics")
  void cacheHitBumpsRamHitCounter(@TempDir java.nio.file.Path dir) throws IOException {
    byte[] payload = bytes(4096);
    java.nio.file.Path file = dir.resolve("hit.bin");
    Files.write(file, payload);

    try (CachedFileSystem cfs = newCfs(file.toUri(), defaultConf())) {
      Path p = new Path(file.toUri());
      // First read populates the cache via Exclusive.
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        in.readAllBytes();
      }
      // Second read is served by Hit → incRamHit fires per chunk.
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        in.readAllBytes();
        org.apache.hadoop.fs.statistics.IOStatistics stats = in.getIOStatistics();
        assertThat(stats.counters().get("cachedfs_stream_cache_hit")).isEqualTo(1L);
        assertThat(stats.counters().get("cachedfs_stream_cache_hit_bytes")).isEqualTo(4096L);
      }
    }
  }

  @Test
  @DisplayName("fs.cached.metrics.enabled=false routes streams to IoStatistics.NO_OP")
  void metricsOffSwitchUsesNoOp(@TempDir java.nio.file.Path dir) throws IOException {
    byte[] payload = bytes(4096);
    java.nio.file.Path file = dir.resolve("noop.bin");
    Files.write(file, payload);

    Configuration conf = defaultConf();
    conf.setBoolean(CachedFsConfig.METRICS_ENABLED, false);
    try (CachedFileSystem cfs = newCfs(file.toUri(), conf)) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        in.readAllBytes();
        // NO_OP getters return 0 even after a successful read.
        org.apache.hadoop.fs.statistics.IOStatistics stats = in.getIOStatistics();
        assertThat(stats.counters().get("stream_read_bytes")).isZero();
      }
    }
  }

  @Test
  @DisplayName("fs.cached.scan-tracker.enabled=false routes streams to ScanTracker.DISABLED")
  void scanTrackerOffSwitch(@TempDir java.nio.file.Path dir) throws IOException {
    byte[] payload = bytes(4096);
    java.nio.file.Path file = dir.resolve("disabled.bin");
    Files.write(file, payload);

    Configuration conf = defaultConf();
    conf.setBoolean(CachedFsConfig.SCAN_TRACKER_ENABLED, false);
    try (CachedFileSystem cfs = newCfs(file.toUri(), conf)) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        in.readAllBytes();
      }
      // DISABLED tracker never enters scanTrackers; aggregate gauge stays at 0.
      CacheBootstrap b = CacheBootstrap.get().orElseThrow();
      assertThat(b.scanTrackerEntries()).isZero();
      // IoStatistics still records (only the tracker is off-switch'd).
      assertThat(b.aggregateIoStats().read()).isPositive();
    }
  }

  // --- helpers -------------------------------------------------------------

  private static CachedFileSystem newCfs(URI fileUri, Configuration conf) throws IOException {
    CachedFileSystem cfs = new CachedFileSystem();
    cfs.initialize(URI.create(fileUri.getScheme() + ":///"), conf);
    return cfs;
  }

  private static Configuration defaultConf() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, LocalFileSystem.class.getName());
    return conf;
  }

  private static byte[] bytes(int n) {
    byte[] out = new byte[n];
    for (int i = 0; i < n; i++) {
      out[i] = (byte) ((i * 31 + 7) & 0xff);
    }
    return out;
  }
}
