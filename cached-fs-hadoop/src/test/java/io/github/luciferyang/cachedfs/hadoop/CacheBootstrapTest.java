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
  @DisplayName("re-registering an endpoint replaces the previous opener")
  void reregistrationReplaces() throws IOException {
    CacheBootstrap.installIfNeeded(minimalConf());
    CacheBootstrap b = CacheBootstrap.get().orElseThrow();

    CacheBootstrap.HandleOpener first = key -> null;
    CacheBootstrap.HandleOpener second = key -> null;
    CacheBootstrap.installOpener("s3a://bucket-x", first);
    CacheBootstrap.installOpener("s3a://bucket-x", second);
    assertThat(b.hasOpener("s3a://bucket-x")).isTrue();
    // Single removeOpener clears the slot — proves there is at most one entry per endpoint.
    assertThat(CacheBootstrap.removeOpener("s3a://bucket-x")).isTrue();
    assertThat(b.hasOpener("s3a://bucket-x")).isFalse();
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

  private static Configuration minimalConf() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, LocalFileSystem.class.getName());
    return conf;
  }
}
