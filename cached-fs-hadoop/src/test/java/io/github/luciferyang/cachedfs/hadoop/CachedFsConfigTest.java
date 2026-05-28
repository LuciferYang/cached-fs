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

import io.github.luciferyang.cachedfs.core.ssd.SsdCache;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CachedFsConfigTest {

  @Test
  @DisplayName(
      "prefetch.max-pending-multiplier scales the auto-default budget; explicit"
          + " .max-pending-bytes overrides")
  void prefetchMaxPendingMultiplier() {
    Configuration conf = new Configuration(false);
    // Default multiplier: 4 → loadQuantum × threads × 4.
    assertThat(CachedFsConfig.defaultPrefetchMaxPendingBytes(1024, 2)).isEqualTo(8192L);
    assertThat(CachedFsConfig.prefetchMaxPendingMultiplier(conf)).isEqualTo(4);
    assertThat(CachedFsConfig.prefetchMaxPendingBytes(conf, 1024, 2)).isEqualTo(8192L);

    // Operator-set multiplier flows through to the auto-default.
    conf.setInt(CachedFsConfig.PREFETCH_MAX_PENDING_MULTIPLIER, 8);
    assertThat(CachedFsConfig.prefetchMaxPendingBytes(conf, 1024, 2)).isEqualTo(16384L);

    // Explicit max-pending-bytes wins over the multiplier (back-compat).
    conf.setLong(CachedFsConfig.PREFETCH_MAX_PENDING_BYTES, 99_999L);
    assertThat(CachedFsConfig.prefetchMaxPendingBytes(conf, 1024, 2)).isEqualTo(99_999L);

    // Non-positive multiplier rejected with the offending key in the message.
    Configuration bad = new Configuration(false);
    bad.setInt(CachedFsConfig.PREFETCH_MAX_PENDING_MULTIPLIER, 0);
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> CachedFsConfig.prefetchMaxPendingMultiplier(bad))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(CachedFsConfig.PREFETCH_MAX_PENDING_MULTIPLIER);
  }

  @Test
  @DisplayName("isEnabled defaults to false; toggled by fs.cached.enabled")
  void enabledToggle() {
    Configuration conf = new Configuration(false);
    assertThat(CachedFsConfig.isEnabled(conf)).isFalse();
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    assertThat(CachedFsConfig.isEnabled(conf)).isTrue();
  }

  @Test
  @DisplayName("ssdConfig returns null when fs.cached.ssd.paths is unset")
  void ssdConfigEmptyWhenPathsMissing() {
    Configuration conf = new Configuration(false);
    assertThat(CachedFsConfig.ssdConfig(conf)).isNull();
  }

  @Test
  @DisplayName("ssdConfig parses comma-separated paths and trims whitespace")
  void ssdConfigParsesPaths() {
    Configuration conf = new Configuration(false);
    conf.set(CachedFsConfig.SSD_PATHS, " /mnt/nvme0 , /mnt/nvme1 ,/mnt/nvme2");
    SsdCache.Config cfg = CachedFsConfig.ssdConfig(conf);
    assertThat(cfg).isNotNull();
    assertThat(cfg.directories())
        .containsExactly(Paths.get("/mnt/nvme0"), Paths.get("/mnt/nvme1"), Paths.get("/mnt/nvme2"));
  }

  @Test
  @DisplayName("ssdConfig applies sensible defaults when only paths set")
  void ssdConfigDefaults() {
    Configuration conf = new Configuration(false);
    conf.set(CachedFsConfig.SSD_PATHS, "/mnt/ssd");
    SsdCache.Config cfg = CachedFsConfig.ssdConfig(conf);
    assertThat(cfg.shardPrefix()).isEqualTo(CachedFsConfig.DEFAULT_SSD_SHARD_PREFIX);
    assertThat(cfg.numShards()).isEqualTo(CachedFsConfig.DEFAULT_SSD_SHARDS);
    assertThat(cfg.regionsPerShard()).isEqualTo(CachedFsConfig.DEFAULT_SSD_REGIONS_PER_SHARD);
    assertThat(cfg.checkpointIntervalBytes())
        .isEqualTo(CachedFsConfig.DEFAULT_SSD_CHECKPOINT_INTERVAL_BYTES);
    assertThat(cfg.checksumEnabled()).isFalse();
  }

  @Test
  @DisplayName("loadQuantumBytes rejects non-positive values")
  void loadQuantumRejectsZero() {
    Configuration conf = new Configuration(false);
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, 0);
    assertThatThrownBy(() -> CachedFsConfig.loadQuantumBytes(conf))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("handleCacheCapacity rejects non-positive values")
  void handleCacheCapacityRejectsZero() {
    Configuration conf = new Configuration(false);
    conf.setInt(CachedFsConfig.HANDLE_CACHE_CAPACITY, 0);
    assertThatThrownBy(() -> CachedFsConfig.handleCacheCapacity(conf))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("ramOptions honors overrides")
  void ramOptionsOverrides() {
    Configuration conf = new Configuration(false);
    conf.setInt(CachedFsConfig.RAM_SHARDS, 8);
    conf.setDouble(CachedFsConfig.RAM_MAX_WRITE_RATIO, 0.5);
    var opts = CachedFsConfig.ramOptions(conf);
    assertThat(opts.numShards()).isEqualTo(8);
    assertThat(opts.maxWriteRatio()).isEqualTo(0.5);
  }

  @Test
  @DisplayName("parsePaths handles empty / whitespace input")
  void parsePathsEmpty() {
    assertThat(CachedFsConfig.parsePaths(null)).isEmpty();
    assertThat(CachedFsConfig.parsePaths("")).isEmpty();
    assertThat(CachedFsConfig.parsePaths("   ")).isEmpty();
    assertThat(CachedFsConfig.parsePaths(",,,")).isEmpty();
    assertThat(CachedFsConfig.parsePaths(" / ")).containsExactly(Path.of("/"));
  }
}
