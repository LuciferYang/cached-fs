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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the {@link BlockLocationsProvider} rewrite path on {@link CachedFileSystem} — {@code
 * getFileBlockLocations}, {@code listLocatedStatus}, {@code listFiles}, and the per-entry {@code
 * rewriteOne} — which the soft-affinity feature relies on. The affinity end-to-end ITs live in the
 * {@code cached-fs-spark} module, so without these the rewrite plumbing is unexercised in
 * cached-fs-hadoop. Uses a {@link LocalFileSystem} inner so the inner FS returns real block
 * locations for the decorator to rewrite.
 */
class CachedFileSystemBlockLocationsTest {

  private static final String EXEC_HOST = "executor_test";

  @AfterEach
  void teardown() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  /** Rewrites every block to a single synthetic executor host so the rewrite is observable. */
  private static BlockLocation[] rewriteToExecutor(FileStatus status) {
    return new BlockLocation[] {
      new BlockLocation(
          new String[] {EXEC_HOST + ":9866"}, new String[] {EXEC_HOST}, 0, status.getLen())
    };
  }

  @Test
  @DisplayName("getFileBlockLocations rewrites through an installed provider")
  void getFileBlockLocationsRewrites(@TempDir java.nio.file.Path dir) throws IOException {
    java.nio.file.Path file = dir.resolve("loc.bin");
    Files.write(file, new byte[4096]);
    try (CachedFileSystem cfs = newCfs(dir, false)) {
      CacheBootstrap.get()
          .orElseThrow()
          .setBlockLocationsProvider((status, start, len, underlying) -> rewriteToExecutor(status));
      Path p = new Path(file.toUri());
      FileStatus st = cfs.getFileStatus(p);

      BlockLocation[] rewritten = cfs.getFileBlockLocations(st, 0, st.getLen());
      assertThat(rewritten).hasSize(1);
      assertThat(rewritten[0].getHosts()).containsExactly(EXEC_HOST);
    }
  }

  @Test
  @DisplayName("getFileBlockLocations returns inner-FS locations unchanged when no provider is set")
  void getFileBlockLocationsPassthroughWithoutProvider(@TempDir java.nio.file.Path dir)
      throws IOException {
    java.nio.file.Path file = dir.resolve("plain.bin");
    Files.write(file, new byte[4096]);
    try (CachedFileSystem cfs = newCfs(dir, false)) {
      // No provider installed → activeProvider() returns null → underlying passed through.
      Path p = new Path(file.toUri());
      FileStatus st = cfs.getFileStatus(p);
      BlockLocation[] locs = cfs.getFileBlockLocations(st, 0, st.getLen());
      assertThat(locs).isNotEmpty();
      assertThat(locs[0].getHosts()).doesNotContain(EXEC_HOST);
    }
  }

  @Test
  @DisplayName("fs.cached.enabled=false disables the provider (activeProvider returns null)")
  void disabledDecoratorSkipsProvider(@TempDir java.nio.file.Path dir) throws IOException {
    java.nio.file.Path file = dir.resolve("disabled.bin");
    Files.write(file, new byte[4096]);
    try (CachedFileSystem cfs = newCfs(dir, true)) {
      // Even if a provider were installed, a disabled decorator must not rewrite. With the master
      // switch off the bootstrap isn't installed, so this exercises the activeProvider !enabled
      // short-circuit.
      Path p = new Path(file.toUri());
      FileStatus st = cfs.getFileStatus(p);
      BlockLocation[] locs = cfs.getFileBlockLocations(st, 0, st.getLen());
      assertThat(locs[0].getHosts()).doesNotContain(EXEC_HOST);
    }
  }

  @Test
  @DisplayName("listLocatedStatus rewrites each emitted entry's block locations")
  void listLocatedStatusRewrites(@TempDir java.nio.file.Path dir) throws IOException {
    Files.write(dir.resolve("a.bin"), new byte[2048]);
    Files.write(dir.resolve("b.bin"), new byte[2048]);
    try (CachedFileSystem cfs = newCfs(dir, false)) {
      CacheBootstrap.get()
          .orElseThrow()
          .setBlockLocationsProvider((status, start, len, underlying) -> rewriteToExecutor(status));
      int seen = 0;
      RemoteIterator<LocatedFileStatus> it = cfs.listLocatedStatus(new Path(dir.toUri()));
      while (it.hasNext()) {
        LocatedFileStatus s = it.next();
        assertThat(s.getBlockLocations()[0].getHosts()).containsExactly(EXEC_HOST);
        seen++;
      }
      assertThat(seen).isEqualTo(2);
    }
  }

  @Test
  @DisplayName("listFiles rewrites each emitted entry's block locations")
  void listFilesRewrites(@TempDir java.nio.file.Path dir) throws IOException {
    Files.write(dir.resolve("c.bin"), new byte[2048]);
    try (CachedFileSystem cfs = newCfs(dir, false)) {
      CacheBootstrap.get()
          .orElseThrow()
          .setBlockLocationsProvider((status, start, len, underlying) -> rewriteToExecutor(status));
      RemoteIterator<LocatedFileStatus> it = cfs.listFiles(new Path(dir.toUri()), true);
      assertThat(it.hasNext()).isTrue();
      LocatedFileStatus s = it.next();
      assertThat(s.getBlockLocations()[0].getHosts()).containsExactly(EXEC_HOST);
    }
  }

  @Test
  @DisplayName("a provider returning null defers to the inner FS locations unchanged")
  void providerReturningNullDefers(@TempDir java.nio.file.Path dir) throws IOException {
    java.nio.file.Path file = dir.resolve("defer.bin");
    Files.write(file, new byte[2048]);
    try (CachedFileSystem cfs = newCfs(dir, false)) {
      // Provider opts out (returns null) → decorator falls back to underlying.
      CacheBootstrap.get().orElseThrow().setBlockLocationsProvider((status, start, len, u) -> null);
      Path p = new Path(file.toUri());
      FileStatus st = cfs.getFileStatus(p);
      BlockLocation[] locs = cfs.getFileBlockLocations(st, 0, st.getLen());
      assertThat(locs).isNotEmpty();
      assertThat(locs[0].getHosts()).doesNotContain(EXEC_HOST);
    }
  }

  private static CachedFileSystem newCfs(java.nio.file.Path dir, boolean disabled)
      throws IOException {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, !disabled);
    conf.set(CachedFsConfig.INNER_IMPL, LocalFileSystem.class.getName());
    conf.setBoolean(CachedFsConfig.JMX_ENABLED, false);
    CachedFileSystem cfs = new CachedFileSystem();
    cfs.initialize(URI.create("file:///"), conf);
    return cfs;
  }
}
