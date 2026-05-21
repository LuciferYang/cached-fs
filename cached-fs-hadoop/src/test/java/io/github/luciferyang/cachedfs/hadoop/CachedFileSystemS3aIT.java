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

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test: {@link CachedFileSystem} wrapping {@link S3AFileSystem} against an in-process
 * {@link S3MockExtension} S3 mock. Runs in the {@code verify} phase via maven-failsafe-plugin.
 *
 * <p>Validates the decorator's cache-fill path through {@code S3AFileSystem}'s positioned-read
 * implementation, which uses ranged HTTP GETs — exercising a different code path from {@code
 * DistributedFileSystem} (covered by {@link CachedFileSystemHdfsIT}) and {@code LocalFileSystem}
 * (covered by unit tests).
 */
class CachedFileSystemS3aIT {

  private static final String BUCKET = "cached-fs-it";

  @RegisterExtension
  static final S3MockExtension S3_MOCK =
      S3MockExtension.builder().silent().withInitialBuckets(BUCKET).build();

  // S3A stages uploads under hadoop.tmp.dir; @TempDir scopes the staging area to this test class
  // so it tears down with the test instead of accumulating across re-runs on a reused GHA runner.
  @TempDir static java.nio.file.Path tmpDir;

  @AfterEach
  void uninstallBootstrap() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  @Test
  @DisplayName("s3a: cached decorator reads bytes equal to direct S3A read")
  void readMatchesUnderlying() throws IOException {
    byte[] payload = bytes(4096);
    Path file = new Path("s3a://" + BUCKET + "/read-matches.bin");
    Configuration conf = cachedConf();
    writeViaInnerFs(file, payload, conf);

    try (CachedFileSystem cfs = newCfs(file.toUri(), conf)) {
      try (FSDataInputStream in = cfs.open(file, 4096)) {
        byte[] got = in.readAllBytes();
        assertThat(got).isEqualTo(payload);
      }
    }
  }

  @Test
  @DisplayName("s3a: multi-chunk read crosses loadQuantum boundary")
  void readMultiChunk() throws IOException {
    byte[] payload = bytes(10 * 1024 + 17);
    Path file = new Path("s3a://" + BUCKET + "/multi-chunk.bin");
    Configuration conf = cachedConf();
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, 4096);
    writeViaInnerFs(file, payload, conf);

    try (CachedFileSystem cfs = newCfs(file.toUri(), conf)) {
      try (FSDataInputStream in = cfs.open(file, 1024)) {
        byte[] got = in.readAllBytes();
        assertThat(got).isEqualTo(payload);
      }
      // Anchor the claim to the cache path — without this, a silent bypass via
      // CachedFileSystem.open's fallthrough branches (enabled=false, or bootstrap missing) would
      // serve correct bytes directly from S3A and the equality assertion would still pass,
      // contradicting the "loadQuantum boundary" claim which only exists on the cache path.
      var stats = CacheBootstrap.get().orElseThrow().ramCache().refreshStats();
      assertThat(stats.numNew()).isGreaterThanOrEqualTo(3);
    }
  }

  @Test
  @DisplayName("s3a: second read for the same offset hits the RAM cache")
  void cacheHitOnSecondRead() throws IOException {
    byte[] payload = bytes(2048);
    Path file = new Path("s3a://" + BUCKET + "/cache-hit.bin");
    Configuration conf = cachedConf();
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, 1024);
    writeViaInnerFs(file, payload, conf);

    try (CachedFileSystem cfs = newCfs(file.toUri(), conf)) {
      try (FSDataInputStream in = cfs.open(file, 1024)) {
        in.readFully(0, new byte[1024]);
      }
      var statsBefore = CacheBootstrap.get().orElseThrow().ramCache().refreshStats();
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
  @DisplayName("s3a: positioned reads return correct slices")
  void positionedRead() throws IOException {
    byte[] payload = bytes(8 * 1024);
    Path file = new Path("s3a://" + BUCKET + "/positioned.bin");
    Configuration conf = cachedConf();
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, 1024);
    writeViaInnerFs(file, payload, conf);

    try (CachedFileSystem cfs = newCfs(file.toUri(), conf)) {
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

  private Configuration cachedConf() {
    Configuration conf = new Configuration(false);
    // S3A stages uploads under hadoop.tmp.dir; without it create() blows up with NPE.
    conf.set("hadoop.tmp.dir", tmpDir.toAbsolutePath().toString());
    // Decorator wiring.
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, S3AFileSystem.class.getName());
    // S3A pointed at the s3mock endpoint. path-style access avoids virtual-host bucket DNS
    // resolution which the mock doesn't serve. Dummy credentials — s3mock accepts anything.
    conf.set(Constants.ENDPOINT, "http://localhost:" + S3_MOCK.getHttpPort());
    conf.setBoolean(Constants.PATH_STYLE_ACCESS, true);
    conf.setBoolean(Constants.SECURE_CONNECTIONS, false);
    conf.set(Constants.ACCESS_KEY, "test-access-key");
    conf.set(Constants.SECRET_KEY, "test-secret-key");
    conf.set(
        Constants.AWS_CREDENTIALS_PROVIDER,
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    conf.set("fs.s3a.endpoint.region", "us-east-1");
    return conf;
  }

  private static CachedFileSystem newCfs(URI fileUri, Configuration conf) throws IOException {
    CachedFileSystem cfs = new CachedFileSystem();
    cfs.initialize(URI.create("s3a://" + fileUri.getHost() + "/"), conf);
    return cfs;
  }

  /**
   * Write via a separate S3AFileSystem instance — the cached path uses {@link CachedFileSystem}'s
   * inherited FilterFileSystem.create, but going through the inner FS directly avoids any
   * decorator-side ambiguity about whether create() is supposed to route through the cache.
   */
  private static void writeViaInnerFs(Path file, byte[] payload, Configuration conf)
      throws IOException {
    Configuration plain = new Configuration(conf);
    plain.unset(CachedFsConfig.ENABLED);
    plain.unset(CachedFsConfig.INNER_IMPL);
    plain.set("fs.s3a.impl", S3AFileSystem.class.getName());
    try (S3AFileSystem fs = new S3AFileSystem()) {
      fs.initialize(URI.create("s3a://" + file.toUri().getHost() + "/"), plain);
      try (FSDataOutputStream out = fs.create(file, true)) {
        out.write(payload);
      }
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
