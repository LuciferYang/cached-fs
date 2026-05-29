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
package io.github.luciferyang.cachedfs.hadoop.mbean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.luciferyang.cachedfs.hadoop.CacheBootstrap;
import io.github.luciferyang.cachedfs.hadoop.CachedFileSystem;
import io.github.luciferyang.cachedfs.hadoop.CachedFsConfig;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.file.Files;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CachedFsBootstrapMBeanTest {

  @AfterEach
  void teardown() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  @Test
  @DisplayName("installIfNeeded registers the MBean on the platform server")
  void installRegistersMBean() throws Exception {
    CacheBootstrap.installIfNeeded(minimalConf());
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName name = new ObjectName(CachedFsConfig.DEFAULT_JMX_OBJECT_NAME);
    assertThat(server.isRegistered(name)).isTrue();
  }

  @Test
  @DisplayName("uninstallForTesting unregisters the MBean from the platform server")
  void uninstallUnregistersMBean() throws Exception {
    CacheBootstrap.installIfNeeded(minimalConf());
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName name = new ObjectName(CachedFsConfig.DEFAULT_JMX_OBJECT_NAME);
    assertThat(server.isRegistered(name)).isTrue();
    CacheBootstrap.uninstallForTesting();
    assertThat(server.isRegistered(name)).isFalse();
  }

  @Test
  @DisplayName("fs.cached.jmx.enabled=false skips MBean registration")
  void jmxDisabledSkipsRegistration() throws Exception {
    Configuration conf = minimalConf();
    conf.setBoolean(CachedFsConfig.JMX_ENABLED, false);
    CacheBootstrap.installIfNeeded(conf);
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName name = new ObjectName(CachedFsConfig.DEFAULT_JMX_OBJECT_NAME);
    assertThat(server.isRegistered(name)).isFalse();
  }

  @Test
  @DisplayName("stats() returns a multi-line report with all expected sections")
  void statsReportContainsSections() throws Exception {
    CacheBootstrap b = CacheBootstrap.installIfNeeded(minimalConf());
    CachedFsBootstrapMBean mbean = new CachedFsBootstrapMBean(b);
    String report = mbean.stats();
    assertThat(report)
        .contains("=== cached-fs stats ===")
        .contains("--- aggregate-io ---")
        .contains("--- ram-cache ---")
        .contains("--- handles ---")
        .contains("--- trackers ---")
        .contains("--- recent-streams ---")
        .contains("ram-hit: ")
        .contains("scan-tracker-count: ");
  }

  @Test
  @DisplayName("counters() returns the namespaced numeric map for windowed-delta computation")
  void countersReturnsNamespacedMap() throws Exception {
    CacheBootstrap b = CacheBootstrap.installIfNeeded(minimalConf());
    CachedFsBootstrapMBean mbean = new CachedFsBootstrapMBean(b);
    java.util.Map<String, Long> c = mbean.counters();
    // Spot-check one key per namespace and that every value is non-negative on a fresh bootstrap.
    assertThat(c)
        .containsKeys(
            "io.read-bytes",
            "ram.num-entries",
            "handles.size",
            "trackers.scan-tracker-count",
            "recent-streams.added-total");
    assertThat(c.values()).allMatch(v -> v >= 0L);
    // Insertion order: io.* group comes before recent-streams.* (matches the stats() layout).
    var keys = new java.util.ArrayList<>(c.keySet());
    assertThat(keys.indexOf("io.read-bytes"))
        .isLessThan(keys.indexOf("recent-streams.added-total"));
  }

  @Test
  @DisplayName("counters() reflects real IO: io.read-bytes moves after a read through the cache")
  void countersReflectRealIo(@TempDir java.nio.file.Path dir) throws Exception {
    byte[] payload = new byte[8192];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) i;
    }
    java.nio.file.Path file = dir.resolve("io.bin");
    Files.write(file, payload);

    Configuration conf = minimalConf();
    try (CachedFileSystem cfs = new CachedFileSystem()) {
      cfs.initialize(URI.create("file:///"), conf);
      CacheBootstrap b = CacheBootstrap.get().orElseThrow();
      CachedFsBootstrapMBean mbean = new CachedFsBootstrapMBean(b);

      assertThat(mbean.counters().get("io.read-bytes")).isZero();

      try (FSDataInputStream in = cfs.open(new Path(file.toUri()), 4096)) {
        in.readFully(0, new byte[4096]);
      }
      // aggregate merges on stream close — read-bytes must now reflect the 4096 consumed.
      assertThat(mbean.counters().get("io.read-bytes")).isEqualTo(4096L);
    }
  }

  @Test
  @DisplayName("inspect(key) reports open-handle false when nothing is cached for that key")
  void inspectReportsMiss() throws Exception {
    CacheBootstrap b = CacheBootstrap.installIfNeeded(minimalConf());
    CachedFsBootstrapMBean mbean = new CachedFsBootstrapMBean(b);
    String report = mbean.inspect("hdfs://nn-a/no-such-file");
    assertThat(report)
        .contains("=== cached-fs inspect ===")
        .contains("key: hdfs://nn-a/no-such-file")
        .contains("open-handle: false")
        .contains("handles-total: 0");
  }

  @Test
  @DisplayName("recentStreams(0) emits the full retained ring")
  void recentStreamsFullDump() throws Exception {
    CacheBootstrap b = CacheBootstrap.installIfNeeded(minimalConf());
    CachedFsBootstrapMBean mbean = new CachedFsBootstrapMBean(b);
    String report = mbean.recentStreams(0);
    // Empty cache → count: 0; the dump still includes the section header.
    assertThat(report)
        .contains("=== cached-fs recent-streams ===")
        .contains("count: 0")
        .contains("returned: 0");
  }

  @Test
  @DisplayName("recentStreams rejects a negative limit")
  void recentStreamsRejectsNegativeLimit() throws Exception {
    CacheBootstrap b = CacheBootstrap.installIfNeeded(minimalConf());
    CachedFsBootstrapMBean mbean = new CachedFsBootstrapMBean(b);
    assertThatThrownBy(() -> mbean.recentStreams(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("-1");
  }

  @Test
  @DisplayName("drain() on an empty bootstrap returns 0 and is safe")
  void drainEmpty() throws Exception {
    CacheBootstrap b = CacheBootstrap.installIfNeeded(minimalConf());
    CachedFsBootstrapMBean mbean = new CachedFsBootstrapMBean(b);
    assertThat(mbean.drain()).isZero();
  }

  @Test
  @DisplayName("purgeMatching rejects an invalid regex via PatternSyntaxException")
  void purgeRejectsInvalidRegex() throws Exception {
    CacheBootstrap b = CacheBootstrap.installIfNeeded(minimalConf());
    CachedFsBootstrapMBean mbean = new CachedFsBootstrapMBean(b);
    assertThatThrownBy(() -> mbean.purgeMatching("[unclosed"))
        .isInstanceOf(java.util.regex.PatternSyntaxException.class);
  }

  @Test
  @DisplayName("purgeMatching with .* matches nothing on an empty bootstrap")
  void purgeMatchingEmpty() throws Exception {
    CacheBootstrap b = CacheBootstrap.installIfNeeded(minimalConf());
    CachedFsBootstrapMBean mbean = new CachedFsBootstrapMBean(b);
    assertThat(mbean.purgeMatching(".*")).isZero();
  }

  @Test
  @DisplayName("drain() reports the number of open handles, not a constant 0")
  void drainClosesOpenHandles(@TempDir java.nio.file.Path dir) throws Exception {
    Configuration conf = minimalConf();
    try (CachedFileSystem cfs = new CachedFileSystem()) {
      cfs.initialize(URI.create("file:///"), conf);
      CacheBootstrap b = CacheBootstrap.get().orElseThrow();
      CachedFsBootstrapMBean mbean = new CachedFsBootstrapMBean(b);

      openAndRead(cfs, dir, "drain-a.bin");
      openAndRead(cfs, dir, "drain-b.bin");
      // Streams closed (handles unpinned) but still cached in the LRU — two distinct keys.
      assertThat(b.handleFactory().size()).isEqualTo(2);
      assertThat(mbean.drain()).isEqualTo(2);
      assertThat(b.handleFactory().size()).isZero();
    }
  }

  @Test
  @DisplayName("purgeMatching closes only the keys matching the regex")
  void purgeMatchingByRegex(@TempDir java.nio.file.Path dir) throws Exception {
    Configuration conf = minimalConf();
    try (CachedFileSystem cfs = new CachedFileSystem()) {
      cfs.initialize(URI.create("file:///"), conf);
      CacheBootstrap b = CacheBootstrap.get().orElseThrow();
      CachedFsBootstrapMBean mbean = new CachedFsBootstrapMBean(b);

      openAndRead(cfs, dir, "keep.bin");
      openAndRead(cfs, dir, "purge-me.bin");
      assertThat(b.handleFactory().size()).isEqualTo(2);

      // Match only the 'purge-me' key → closes 1, leaves 'keep'.
      assertThat(mbean.purgeMatching(".*purge-me.*")).isEqualTo(1);
      assertThat(b.handleFactory().size()).isEqualTo(1);
      // A non-matching regex closes nothing.
      assertThat(mbean.purgeMatching(".*no-such-key.*")).isZero();
      assertThat(b.handleFactory().size()).isEqualTo(1);
    }
  }

  /**
   * Opens {@code name} under {@code dir} through {@code cfs}, reads a few bytes, closes the stream.
   */
  private static void openAndRead(CachedFileSystem cfs, java.nio.file.Path dir, String name)
      throws IOException {
    java.nio.file.Path file = dir.resolve(name);
    Files.write(file, new byte[1024]);
    try (FSDataInputStream in = cfs.open(new Path(file.toUri()), 4096)) {
      in.readFully(0, new byte[16]);
    }
  }

  private static Configuration minimalConf() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, LocalFileSystem.class.getName());
    return conf;
  }
}
