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
      "two decorators for different endpoints coexist; closing one preserves the other's opener")
  void multipleEndpointsCoexist(@TempDir java.nio.file.Path dir) throws IOException {
    // Two CachedFileSystem instances share the bootstrap but register independent openers under
    // their own scheme://authority keys. Closing one must NOT unregister the other.
    Files.write(dir.resolve("a.bin"), bytes(64));

    Configuration confA = defaultConf();
    Configuration confB = defaultConf();
    CachedFileSystem alpha = new CachedFileSystem();
    CachedFileSystem beta = new CachedFileSystem();
    try {
      alpha.initialize(URI.create("file:///"), confA);
      // Manually register a second opener simulating a peer decorator on a different endpoint —
      // this mirrors what a real BosFileSystem-backed CachedFileSystem would do at initialize().
      CacheBootstrap.installOpener("bos://bucket.bj.bcebos.com", key -> null);
      beta.initialize(URI.create("file:///"), confB); // no-op for endpoint (same as alpha)
      CacheBootstrap b = CacheBootstrap.get().orElseThrow();
      assertThat(b.hasOpener("file://")).isTrue();
      assertThat(b.hasOpener("bos://bucket.bj.bcebos.com")).isTrue();

      alpha.close();
      // Closing alpha unregisters file:// (its own endpoint) but the bos endpoint must remain
      // since no decorator owning it has closed.
      assertThat(b.hasOpener("file://")).isFalse();
      assertThat(b.hasOpener("bos://bucket.bj.bcebos.com")).isTrue();
    } finally {
      beta.close();
      // bos opener was registered without a real decorator owning it — clean it up so the
      // @AfterEach uninstallForTesting() doesn't try to drain a fake handle key.
      CacheBootstrap.removeOpener("bos://bucket.bj.bcebos.com");
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
