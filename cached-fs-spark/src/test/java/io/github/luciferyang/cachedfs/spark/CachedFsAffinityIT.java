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
import io.github.luciferyang.cachedfs.spark.affinity.CachedFsAffinity;
import io.github.luciferyang.cachedfs.spark.affinity.CachedFsAffinityConfig;
import io.github.luciferyang.cachedfs.spark.affinity.CachedFsSoftAffinityManager;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partition;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConverters;

/**
 * IT: {@link CachedFsAffinity} end-to-end against a real {@link SparkSession}. Wires the affinity
 * extension via {@code spark.sql.extensions}, injects fake executors directly into the manager
 * (bypassing the listener so we don't need a multi-executor cluster), and verifies that (a) the
 * {@link io.github.luciferyang.cachedfs.spark.affinity.CachedFsAffinityBlockLocationsProvider}
 * staged by the extension actually rewrites {@code getFileBlockLocations} output to point at
 * executor task-location strings, and (b) the rewritten BlockLocation[] flows into the
 * FilePartition preferredLocations the Spark scheduler consumes.
 *
 * <p>Runs in its own forked JVM (Failsafe forkCount=1 reuseForks=false) — the SparkContext
 * singleton in this JVM is dedicated to the affinity SparkConf shape, which can't share a JVM with
 * the plain-read IT class.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CachedFsAffinityIT {

  private SparkSession spark;

  @BeforeAll
  void setUp() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("cached-fs-affinity-it")
            // Wire CachedFileSystem onto file://.
            .config("spark.hadoop.fs.file.impl", CachedFileSystem.class.getName())
            .config("spark.hadoop." + CachedFsConfig.ENABLED, "true")
            .config(
                "spark.hadoop." + CachedFsConfig.INNER_IMPL, "org.apache.hadoop.fs.LocalFileSystem")
            .config("spark.hadoop.fs.cached.ram.size-bytes", String.valueOf(16L * 1024 * 1024))
            .config("spark.hadoop.fs.cached.load-quantum-bytes", String.valueOf(64 * 1024))
            .config("spark.hadoop." + CachedFsConfig.PREFETCH_ENABLED, "false")
            // Wire the affinity extension.
            .config("spark.sql.extensions", CachedFsAffinityExtension.class.getName())
            .config(CachedFsAffinityConfig.ENABLED, "true")
            .config(CachedFsAffinityConfig.REPLICATION_NUM, "3")
            .config(CachedFsAffinityConfig.MIN_TARGET_HOSTS, "1")
            .config(CachedFsAffinityConfig.VIRTUAL_NODES, "100")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            // Pin driver to loopback for CI robustness against unusual hostname/network setups.
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            // Force one FilePartition per file (no coalescing of small files) so each partition
            // carries the affinity hosts directly from its block. Without this, Spark groups
            // small files into a single coalesced FilePartition whose locations are taken from
            // the cumulative block-host vote — which still includes our affinity hosts, but
            // can be filtered through getBlockHosts overlap math in surprising ways.
            .config("spark.sql.files.maxPartitionBytes", "1024")
            .config("spark.sql.files.openCostInBytes", "0")
            // Pin Spark scratch + warehouse under target/ so they never pollute the repo
            // working tree.
            .config(
                "spark.sql.warehouse.dir",
                "file:" + System.getProperty("user.dir") + "/target/spark-warehouse")
            .config("spark.local.dir", System.getProperty("user.dir") + "/target/spark-local")
            .getOrCreate();
  }

  @AfterAll
  void tearDown() {
    // Each release in its own try/finally so a failure in spark.stop doesn't silently skip
    // FileSystem.closeAll / CacheBootstrap.uninstall / listener+manager reset. The fork
    // strategy (Failsafe forkCount=1 reuseForks=false) gives a hard outer safety net via
    // JVM exit, but cleanups failing in sequence would otherwise mask the original cause.
    try {
      if (spark != null) {
        spark.stop();
      }
    } catch (RuntimeException ignored) {
      // best-effort; subsequent cleanups still run
    }
    try {
      FileSystem.closeAll();
    } catch (java.io.IOException | RuntimeException ignored) {
    }
    try {
      CacheBootstrap.uninstallForTesting();
    } catch (java.io.IOException | RuntimeException ignored) {
    }
    // Reset BOTH the manager singleton AND the listener-registration AtomicReference. Without
    // the listener reset the companion-object's Registration still points at the stopped
    // SparkContext + its listener — harmless under reuseForks=false (JVM exits per class) but
    // a foot-gun if isolation strategy ever changes. Symmetric with CachedFsAffinitySuite.
    // Companion-object access from Java goes through the synthetic MODULE$ holder.
    try {
      CachedFsSoftAffinityListener$.MODULE$.resetForTesting();
    } catch (RuntimeException ignored) {
    }
    CachedFsSoftAffinityManager.resetForTesting();
  }

  @BeforeEach
  void registerExecutors() {
    // Bypass the Scala listener: inject fake executors directly. The listener path is covered by
    // CachedFsAffinitySuite; here we want a controllable executor set without a real cluster.
    // handleExecutorAdded is idempotent on duplicate id, so re-running this between tests is
    // safe even if the prior test's executors are still registered.
    CachedFsAffinity.onExecutorAdded("exec1", "host-A");
    CachedFsAffinity.onExecutorAdded("exec2", "host-B");
    CachedFsAffinity.onExecutorAdded("exec3", "host-C");
  }

  @AfterEach
  void clearExecutors() {
    // Drop the executors before the next test so each test sees a deterministic ring. Assert
    // the post-condition so a leaked add from a test surfaces here, not in a downstream
    // failure with a confusing executor count.
    CachedFsAffinity.onExecutorRemoved("exec1");
    CachedFsAffinity.onExecutorRemoved("exec2");
    CachedFsAffinity.onExecutorRemoved("exec3");
    assertThat(CachedFsSoftAffinityManager.getInstance().executorCount())
        .as("ring must be empty after clearExecutors — a non-zero count means a test leaked an add")
        .isZero();
  }

  @Test
  @DisplayName(
      "fs.getFileBlockLocations through CachedFileSystem returns executor task-location strings")
  void getFileBlockLocationsReturnsExecutorTaskLocations(@TempDir java.nio.file.Path tmp)
      throws Exception {
    // Write a file via Spark so it exists on disk; we only care about the BlockLocation rewrite,
    // not the contents.
    Path parquetDir = new Path("file://" + tmp.resolve("data.parquet").toAbsolutePath());
    spark.range(100).toDF("id").write().mode("overwrite").parquet(parquetDir.toString());

    org.apache.hadoop.conf.Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
    FileSystem fs = FileSystem.get(URI.create("file:///"), hadoopConf);
    // Find the actual parquet part-file (a directory was created with a single part inside).
    FileStatus[] parquetFiles = fs.listStatus(parquetDir, p -> p.getName().endsWith(".parquet"));
    // Spark's range(100).write.parquet typically emits 1 part file in local[1], but write-side
    // splitting is implementation-defined across Spark patch releases — iterate ALL part files
    // so the assertion holds even if a future release fragments differently.
    assertThat(parquetFiles).as("at least one parquet part file").isNotEmpty();

    for (FileStatus part : parquetFiles) {
      BlockLocation[] blocks = fs.getFileBlockLocations(part, 0, part.getLen());
      assertThat(blocks).as("at least one BlockLocation for %s", part.getPath()).isNotEmpty();
      // Every block's hosts should be the affinity-selected executor task-locations of the form
      // "executor_<host>_<execId>" — the format Spark's TaskLocation.apply parses into
      // ExecutorCacheTaskLocation for PROCESS_LOCAL routing.
      for (BlockLocation block : blocks) {
        String[] hosts = block.getHosts();
        assertThat(hosts).as("block hosts non-empty for %s", part.getPath()).isNotEmpty();
        assertThat(hosts)
            .as("all hosts are executor task-location strings for %s", part.getPath())
            .allSatisfy(h -> assertThat(h).matches("executor_host-[ABC]_exec[123]"));
      }
    }
  }

  @Test
  @DisplayName("FilePartition preferredLocations contain executor task-location strings")
  void filePartitionPreferredLocationsContainAffinityTargets(@TempDir java.nio.file.Path tmp)
      throws Exception {
    // Multi-file scan so we get multiple FilePartitions; a single small file in Spark collapses
    // into one FilePartition which is less informative.
    Path parquetDir = new Path("file://" + tmp.resolve("multi.parquet").toAbsolutePath());
    spark
        .range(0, 4000, 1, 4) // 4 partitions on the write side → 4 part files
        .toDF("id")
        .write()
        .mode("overwrite")
        .parquet(parquetDir.toString());

    // Pre-flight: verify the affinity provider IS rewriting LocatedFileStatus block hosts at the
    // FS layer. listLocatedStatus is the API Spark's bulkListLeafFiles uses for the s3a
    // fast-path; the generic path calls listStatus + getFileBlockLocations. We sample both so a
    // diagnostic failure pinpoints which API gap is silencing the affinity hosts.
    org.apache.hadoop.conf.Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
    FileSystem fs = FileSystem.get(URI.create("file:///"), hadoopConf);
    StringBuilder lfsDiag = new StringBuilder();
    lfsDiag.append("listLocatedStatus block hosts per file:\n");
    org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> it =
        fs.listLocatedStatus(parquetDir);
    while (it.hasNext()) {
      org.apache.hadoop.fs.LocatedFileStatus lfs = it.next();
      lfsDiag
          .append("  ")
          .append(lfs.getPath().getName())
          .append(": ")
          .append(Arrays.deepToString(lfs.getBlockLocations()))
          .append("\n");
    }
    lfsDiag.append("listStatus + getFileBlockLocations(file,0,len) hosts per file:\n");
    for (FileStatus part : fs.listStatus(parquetDir, p -> p.getName().endsWith(".parquet"))) {
      lfsDiag
          .append("  ")
          .append(part.getPath().getName())
          .append(" (class=")
          .append(part.getClass().getSimpleName())
          .append("): ")
          .append(Arrays.deepToString(fs.getFileBlockLocations(part, 0, part.getLen())))
          .append("\n");
    }

    // queryExecution().toRdd() wraps the FileScanRDD in MapPartitionsRDD; MapPartitionsRDD does
    // NOT override getPreferredLocations so calling it directly returns Nil. Spark's DAGScheduler
    // walks narrow dependencies backward to find the FileScanRDD's preferredLocations — replicate
    // that walk here so we observe what the scheduler will see.
    org.apache.spark.rdd.RDD<?> top =
        spark.read().parquet(parquetDir.toString()).queryExecution().toRdd();
    org.apache.spark.rdd.RDD<?> fileScanRdd = findFileScanRdd(top);
    assertThat(fileScanRdd)
        .as("expected a FileScanRDD reachable via narrow dependencies. %s", lfsDiag)
        .isNotNull();

    Partition[] partitions = fileScanRdd.partitions();
    assertThat(partitions).as("at least one FilePartition").isNotEmpty();

    // Every partition from an affinity-routed file MUST carry affinity hosts. "At least one"
    // would let a regression where most partitions silently lose their hint slip through
    // (e.g., a bug in CachedFsAffinityBlockLocationsProvider's per-block loop only rewriting
    // the first BlockLocation). Tolerate zero misses.
    java.util.List<Integer> missingAffinity = new java.util.ArrayList<>();
    StringBuilder diag = new StringBuilder("collected preferredLocations:\n");
    for (Partition p : partitions) {
      List<String> locs = JavaConverters.seqAsJavaList(fileScanRdd.preferredLocations(p).toList());
      diag.append("  partition ").append(p.index()).append(": ").append(locs).append("\n");
      boolean hasAffinity = locs.stream().anyMatch(s -> s.matches("executor_host-[ABC]_exec[123]"));
      if (!hasAffinity) {
        missingAffinity.add(p.index());
      }
    }
    assertThat(missingAffinity)
        .as(
            "every FilePartition over affinity-routed files must carry affinity hosts; "
                + "partitions missing them: %s. %s%s",
            missingAffinity, lfsDiag, diag)
        .isEmpty();
  }

  /**
   * Walks {@code rdd}'s narrow dependencies (and only narrow dependencies — shuffle boundaries
   * break locality propagation in Spark itself) until it finds a {@code FileScanRDD}. Returns null
   * if no FileScanRDD is reachable. Mirrors {@code DAGScheduler.getPreferredLocsInternal}: tracks a
   * visited set (SPARK-695) to avoid exponential exploration on DAGs with shared narrow deps, and
   * matches by fully-qualified class name (not {@code endsWith("FileScanRDD")}) so an unrelated
   * class whose simple name ends with that string can't false-match.
   */
  private static final String FILE_SCAN_RDD_FQN =
      "org.apache.spark.sql.execution.datasources.FileScanRDD";

  private static org.apache.spark.rdd.RDD<?> findFileScanRdd(org.apache.spark.rdd.RDD<?> rdd) {
    return findFileScanRddImpl(
        rdd, java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
  }

  private static org.apache.spark.rdd.RDD<?> findFileScanRddImpl(
      org.apache.spark.rdd.RDD<?> rdd, java.util.Set<org.apache.spark.rdd.RDD<?>> visited) {
    if (!visited.add(rdd)) {
      return null;
    }
    if (FILE_SCAN_RDD_FQN.equals(rdd.getClass().getName())) {
      return rdd;
    }
    scala.collection.Iterator<org.apache.spark.Dependency<?>> deps = rdd.dependencies().iterator();
    while (deps.hasNext()) {
      org.apache.spark.Dependency<?> dep = deps.next();
      if (dep instanceof org.apache.spark.NarrowDependency<?>) {
        org.apache.spark.rdd.RDD<?> found = findFileScanRddImpl(dep.rdd(), visited);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  @Test
  @DisplayName("CachedFsAffinity.isActive() reports true when extension + executors are wired")
  void isActiveTrueWhenWired() {
    // Extension started at session-init time; @BeforeEach injected three executors. isActive
    // should report true: enabled=true && executorCount > 0.
    assertThat(CachedFsAffinity.isActive())
        .as("affinity active when extension is enabled and executors are present")
        .isTrue();
    assertThat(CachedFsSoftAffinityManager.getInstance().executorCount()).isEqualTo(3);
  }
}
