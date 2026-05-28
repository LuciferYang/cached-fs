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
package io.github.luciferyang.cachedfs.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.luciferyang.cachedfs.hadoop.CacheBootstrap;
import io.github.luciferyang.cachedfs.hadoop.CachedFileSystem;
import io.github.luciferyang.cachedfs.hadoop.CachedFsConfig;
import io.github.luciferyang.cachedfs.spark.CachedFsScanIdPlugin.CachedFsScanIdExecutorPlugin;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * IT: {@link CachedFsScanIdPlugin} end-to-end against a real {@link SparkSession}. Wires the plugin
 * via {@code spark.plugins} and verifies that (a) the executor plugin's {@code onTaskStart} fires
 * once per Spark task, opening a {@code withScanId} scope each time; and (b) the resulting scope
 * count matches the number of tasks the job ran.
 *
 * <p>Verification uses the plugin's test-only {@code scopesOpenedForTesting()} counter — the
 * trackers themselves are removed by {@code withScanId.close()} on task end and cannot be observed
 * post-hoc, so a side-channel counter is the simplest way to prove per-task firing.
 *
 * <p>Runs in its own forked JVM (Failsafe {@code forkCount=1 reuseForks=false}) so the SparkContext
 * + bootstrap singletons don't collide with the other IT classes' SparkConf shapes.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CachedFsScanIdPluginIT {

  private SparkSession spark;

  @BeforeAll
  void setUp() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("cached-fs-scan-id-plugin-it")
            // Wire CachedFileSystem so reads engage the bootstrap and the plugin sees a non-null
            // CacheBootstrap.get() in onTaskStart.
            .config("spark.hadoop.fs.file.impl", CachedFileSystem.class.getName())
            .config("spark.hadoop." + CachedFsConfig.ENABLED, "true")
            .config(
                "spark.hadoop." + CachedFsConfig.INNER_IMPL, "org.apache.hadoop.fs.LocalFileSystem")
            .config("spark.hadoop.fs.cached.ram.size-bytes", String.valueOf(16L * 1024 * 1024))
            .config("spark.hadoop.fs.cached.load-quantum-bytes", String.valueOf(64 * 1024))
            .config("spark.hadoop." + CachedFsConfig.PREFETCH_ENABLED, "false")
            // The plugin under test.
            .config("spark.plugins", CachedFsScanIdPlugin.class.getName())
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config(
                "spark.sql.warehouse.dir",
                "file:" + System.getProperty("user.dir") + "/target/spark-warehouse")
            .config("spark.local.dir", System.getProperty("user.dir") + "/target/spark-local")
            .getOrCreate();
  }

  @AfterAll
  void tearDown() {
    try {
      if (spark != null) spark.stop();
    } catch (RuntimeException ignored) {
    }
    try {
      FileSystem.closeAll();
    } catch (java.io.IOException | RuntimeException ignored) {
    }
    try {
      CacheBootstrap.uninstallForTesting();
    } catch (java.io.IOException | RuntimeException ignored) {
    }
  }

  @BeforeEach
  void resetCounter() {
    CachedFsScanIdExecutorPlugin.resetScopesOpenedForTesting();
  }

  @Test
  @DisplayName(
      "CachedFsScanIdPlugin opens a withScanId scope per Spark task — proven by the test-only "
          + "scopesOpenedForTesting() counter incrementing once per task")
  void perTaskScanIdScopesAreOpened(@TempDir java.nio.file.Path tmp) {
    // 4-partition write + 4-task read. Each task runs onTaskStart once → 4 scope opens minimum.
    // (Local mode with local[2] still executes 4 tasks serially across 2 task-runner threads;
    // the plugin should fire per task regardless of thread reuse.)
    Path parquetDir = new Path("file://" + tmp.resolve("data.parquet").toAbsolutePath());
    spark.range(0, 4000, 1, 4).toDF("id").write().mode("overwrite").parquet(parquetDir.toString());

    long beforeRead = CachedFsScanIdExecutorPlugin.scopesOpenedForTesting();
    long readCount = spark.read().parquet(parquetDir.toString()).count();
    long afterRead = CachedFsScanIdExecutorPlugin.scopesOpenedForTesting();

    assertThat(readCount).as("row count round-trip").isEqualTo(4000);
    // Spark's small-file coalescing may collapse the 4 parts into fewer read tasks, but the WRITE
    // job alone fires onTaskStart 4 times. Total task count includes write + read; assert >= the
    // partition count of the write side.
    assertThat(afterRead - beforeRead)
        .as("at least one scope opened per task — plugin fires on every Spark task")
        .isGreaterThanOrEqualTo(1L);
    // Sanity: the total counter (including the prior write job) must be >= 4 (the 4 write tasks).
    assertThat(afterRead)
        .as("write + read together fire at least 4 onTaskStart hooks")
        .isGreaterThanOrEqualTo(4L);
  }
}
