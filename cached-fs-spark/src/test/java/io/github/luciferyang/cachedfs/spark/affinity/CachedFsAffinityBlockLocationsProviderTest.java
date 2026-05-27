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
package io.github.luciferyang.cachedfs.spark.affinity;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.luciferyang.cachedfs.hadoop.CacheBootstrap;
import io.github.luciferyang.cachedfs.hadoop.CachedFileSystem;
import io.github.luciferyang.cachedfs.hadoop.CachedFsConfig;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies {@link CachedFsAffinityBlockLocationsProvider} rewrites {@code BlockLocation.hosts} with
 * affinity-aware executor strings AND that the cached-fs decorator's {@code getFileBlockLocations}
 * override picks up the provider via {@code CacheBootstrap}.
 */
class CachedFsAffinityBlockLocationsProviderTest {

  @BeforeEach
  void setUp() {
    CachedFsSoftAffinityManager.resetForTesting();
  }

  @AfterEach
  void teardown() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  @Test
  @DisplayName("disabled manager → provider returns underlying BlockLocations unchanged")
  void disabledPassesThrough() {
    CachedFsAffinitySoftMgr().setEnabled(false);

    CachedFsAffinityBlockLocationsProvider provider = new CachedFsAffinityBlockLocationsProvider();
    FileStatus status =
        new FileStatus(0, false, 1, 0, 0, new org.apache.hadoop.fs.Path("file://x"));
    BlockLocation[] underlying = new BlockLocation[] {block(new String[] {"h-native"}, 0, 100)};
    BlockLocation[] rewritten = provider.getBlockLocations(status, 0, 100, underlying);
    assertThat(rewritten).isSameAs(underlying);
  }

  @Test
  @DisplayName("enabled + executors present → provider returns a single executor-shaped block")
  void rewritesToExecutorLocations() {
    CachedFsSoftAffinityManager mgr = CachedFsAffinitySoftMgr();
    mgr.setEnabled(true);
    mgr.setReplicationNum(2);
    mgr.handleExecutorAdded("exec-1", "host-a");
    mgr.handleExecutorAdded("exec-2", "host-b");

    CachedFsAffinityBlockLocationsProvider provider = new CachedFsAffinityBlockLocationsProvider();
    FileStatus status =
        new FileStatus(0, false, 1, 0, 0, new org.apache.hadoop.fs.Path("file:///big.parquet"));
    BlockLocation[] underlying = new BlockLocation[] {block(new String[] {"h-native"}, 0, 200)};

    BlockLocation[] rewritten = provider.getBlockLocations(status, 0, 200, underlying);
    assertThat(rewritten).hasSize(1);
    String[] hosts = safeHosts(rewritten[0]);
    assertThat(hosts).hasSize(2);
    for (String h : hosts) {
      assertThat(h).matches("executor_host-[ab]_exec-[12]");
    }
  }

  @Test
  @DisplayName(
      "native locality already covers minTargetHosts → provider defers (returns underlying)")
  void defersToStrongNativeLocality() {
    CachedFsSoftAffinityManager mgr = CachedFsAffinitySoftMgr();
    mgr.setEnabled(true);
    mgr.setMinTargetHosts(1);
    mgr.handleExecutorAdded("exec-1", "host-a");

    CachedFsAffinityBlockLocationsProvider provider = new CachedFsAffinityBlockLocationsProvider();
    FileStatus status =
        new FileStatus(0, false, 1, 0, 0, new org.apache.hadoop.fs.Path("file://y"));
    BlockLocation[] underlying = new BlockLocation[] {block(new String[] {"host-a"}, 0, 50)};
    BlockLocation[] rewritten = provider.getBlockLocations(status, 0, 50, underlying);
    assertThat(rewritten).isSameAs(underlying);
  }

  @Test
  @DisplayName(
      "end-to-end: stageBlockLocationsProvider before bootstrap install picks up on first install")
  void stageThenInstallWiresProvider(@TempDir Path dir) throws IOException {
    byte[] payload = bytes(2048);
    Path file = dir.resolve("data.bin");
    Files.write(file, payload);

    // Stage the provider BEFORE bootstrap install (Spark extension start sequence).
    CacheBootstrap.stageBlockLocationsProvider(new CachedFsAffinityBlockLocationsProvider());

    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, LocalFileSystem.class.getName());

    CachedFsSoftAffinityManager mgr = CachedFsAffinitySoftMgr();
    mgr.setEnabled(true);
    mgr.setReplicationNum(1);
    mgr.handleExecutorAdded("exec-7", "host-x");

    try (CachedFileSystem cfs = new CachedFileSystem()) {
      cfs.initialize(URI.create("file:///"), conf);
      // Bootstrap is now installed → staged provider should be live.
      assertThat(CacheBootstrap.get().orElseThrow().blockLocationsProvider()).isNotNull();

      FileStatus status = cfs.getFileStatus(new org.apache.hadoop.fs.Path(file.toUri()));
      BlockLocation[] locs = cfs.getFileBlockLocations(status, 0, payload.length);
      assertThat(locs).hasSize(1);
      assertThat(safeHosts(locs[0])).containsExactly("executor_host-x_exec-7");
    }
  }

  @Test
  @DisplayName("install-then-stage: stage after bootstrap is already installed → live install")
  void installThenStageWiresProvider(@TempDir Path dir) throws IOException {
    byte[] payload = bytes(2048);
    Path file = dir.resolve("data.bin");
    Files.write(file, payload);

    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, LocalFileSystem.class.getName());

    CachedFsSoftAffinityManager mgr = CachedFsAffinitySoftMgr();
    mgr.setEnabled(true);
    mgr.setReplicationNum(1);
    mgr.handleExecutorAdded("exec-9", "host-z");

    try (CachedFileSystem cfs = new CachedFileSystem()) {
      cfs.initialize(URI.create("file:///"), conf);
      // Bootstrap is now installed WITHOUT a provider.
      assertThat(CacheBootstrap.get().orElseThrow().blockLocationsProvider()).isNull();

      // Stage AFTER install — the static method must transparently route to the live instance.
      CacheBootstrap.stageBlockLocationsProvider(new CachedFsAffinityBlockLocationsProvider());
      assertThat(CacheBootstrap.get().orElseThrow().blockLocationsProvider()).isNotNull();

      FileStatus status = cfs.getFileStatus(new org.apache.hadoop.fs.Path(file.toUri()));
      BlockLocation[] locs = cfs.getFileBlockLocations(status, 0, payload.length);
      assertThat(safeHosts(locs[0])).containsExactly("executor_host-z_exec-9");
    }
  }

  // --- helpers -----------------------------------------------------------

  private static CachedFsSoftAffinityManager CachedFsAffinitySoftMgr() {
    return CachedFsSoftAffinityManager.getInstance();
  }

  private static BlockLocation block(String[] hosts, long start, long len) {
    return new BlockLocation(new String[0], hosts, start, len);
  }

  private static String[] safeHosts(BlockLocation b) {
    try {
      return b.getHosts();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static byte[] bytes(int n) {
    byte[] out = new byte[n];
    for (int i = 0; i < n; i++) out[i] = (byte) i;
    return out;
  }
}
