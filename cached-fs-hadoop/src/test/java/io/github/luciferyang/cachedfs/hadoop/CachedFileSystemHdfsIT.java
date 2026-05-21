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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test: {@link CachedFileSystem} wrapping {@link DistributedFileSystem} backed by a
 * {@link MiniDFSCluster}. Runs in the {@code verify} phase via maven-failsafe-plugin.
 *
 * <p>Validates that the decorator's cache-fill path correctly drives Hadoop's positioned-read API
 * over a real HDFS implementation — not just {@code LocalFileSystem}. The unit tests in {@link
 * CachedFileSystemTest} exercise the cache logic; this class exercises the {@code HadoopReadFile}
 * adapter against an FS whose positioned reads do real network I/O.
 */
class CachedFileSystemHdfsIT {

  private static MiniDFSCluster cluster;
  private static Configuration baseConf;

  @BeforeAll
  static void startCluster(@TempDir java.nio.file.Path baseDir) throws IOException {
    baseConf = new Configuration();
    // Pin the cluster's storage under the JUnit temp dir so it tears down with the test.
    baseConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toAbsolutePath().toString());
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(1).build();
    cluster.waitClusterUp();
  }

  @AfterAll
  static void stopCluster() {
    if (cluster != null) {
      cluster.shutdown(true);
      cluster = null;
    }
  }

  @AfterEach
  void uninstallBootstrap() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  @Test
  @DisplayName("hdfs: cached decorator reads bytes equal to direct DistributedFileSystem read")
  void readMatchesUnderlying() throws IOException {
    byte[] payload = bytes(4096);
    Path file = new Path("/cached-fs-it/read-matches.bin");
    writeToHdfs(file, payload);

    Configuration conf = cachedConf();
    try (CachedFileSystem cfs = newCfs(conf)) {
      try (FSDataInputStream in = cfs.open(file, 4096)) {
        byte[] got = in.readAllBytes();
        assertThat(got).isEqualTo(payload);
      }
    }
  }

  @Test
  @DisplayName("hdfs: multi-chunk read crosses loadQuantum boundary and cache fills correctly")
  void readMultiChunk() throws IOException {
    // loadQuantum = 4 KiB, file = 10 KiB + 17 → spans 3 chunks.
    byte[] payload = bytes(10 * 1024 + 17);
    Path file = new Path("/cached-fs-it/multi-chunk.bin");
    writeToHdfs(file, payload);

    Configuration conf = cachedConf();
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, 4096);
    try (CachedFileSystem cfs = newCfs(conf)) {
      try (FSDataInputStream in = cfs.open(file, 1024)) {
        byte[] got = in.readAllBytes();
        assertThat(got).isEqualTo(payload);
      }
    }
  }

  @Test
  @DisplayName("hdfs: second read for the same offset hits the RAM cache (numHit > 0)")
  void cacheHitOnSecondRead() throws IOException {
    byte[] payload = bytes(2048);
    Path file = new Path("/cached-fs-it/cache-hit.bin");
    writeToHdfs(file, payload);

    Configuration conf = cachedConf();
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, 1024);
    try (CachedFileSystem cfs = newCfs(conf)) {
      try (FSDataInputStream in = cfs.open(file, 1024)) {
        in.readFully(0, new byte[1024]);
      }
      var statsBefore =
          CacheBootstrap.get().orElseThrow().ramCache().refreshStats();
      try (FSDataInputStream in = cfs.open(file, 1024)) {
        byte[] dst = new byte[1024];
        in.readFully(0, dst);
        for (int i = 0; i < 1024; i++) {
          assertThat(dst[i]).isEqualTo(payload[i]);
        }
      }
      var statsAfter = CacheBootstrap.get().orElseThrow().ramCache().refreshStats();
      assertThat(statsAfter.numHit()).isGreaterThan(statsBefore.numHit());
      assertThat(statsAfter.numNew()).isEqualTo(statsBefore.numNew());
    }
  }

  @Test
  @DisplayName("hdfs: positioned reads (PositionedReadable) return correct slices")
  void positionedRead() throws IOException {
    byte[] payload = bytes(8 * 1024);
    Path file = new Path("/cached-fs-it/positioned.bin");
    writeToHdfs(file, payload);

    Configuration conf = cachedConf();
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, 1024);
    try (CachedFileSystem cfs = newCfs(conf)) {
      try (FSDataInputStream in = cfs.open(file, 1024)) {
        byte[] slice = new byte[500];
        in.readFully(2500, slice);
        for (int i = 0; i < 500; i++) {
          assertThat(slice[i]).isEqualTo(payload[2500 + i]);
        }
      }
    }
  }

  // --- helpers ------------------------------------------------------------

  private static Configuration cachedConf() {
    // Start from the cluster conf so fs.defaultFS / hdfs.client.* land here, then layer the
    // decorator wiring on top. fs.<scheme>.impl=CachedFileSystem replaces HDFS's own impl.
    Configuration conf = new Configuration(cluster.getConfiguration(0));
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, DistributedFileSystem.class.getName());
    return conf;
  }

  private static CachedFileSystem newCfs(Configuration conf) throws IOException {
    CachedFileSystem cfs = new CachedFileSystem();
    URI hdfsUri = cluster.getFileSystem().getUri();
    cfs.initialize(hdfsUri, conf);
    return cfs;
  }

  private static void writeToHdfs(Path file, byte[] payload) throws IOException {
    try (FSDataOutputStream out = cluster.getFileSystem().create(file, true)) {
      out.write(payload);
    }
  }

  private static byte[] bytes(int n) {
    byte[] out = new byte[n];
    for (int i = 0; i < n; i++) {
      out[i] = (byte) ((i * 31 + 7) & 0xff);
    }
    return out;
  }
}
