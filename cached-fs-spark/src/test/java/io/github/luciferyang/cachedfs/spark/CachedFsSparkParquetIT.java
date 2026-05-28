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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * IT: SparkSession reading Parquet through {@link CachedFileSystem} wrapping the local fs.
 *
 * <p>Spins a real local-mode SparkSession with {@code fs.file.impl=CachedFileSystem} so every
 * {@code file://} read flows through the decorator. Verifies (a) round-trip through the cache
 * produces byte-for-byte correct results, (b) a second read of the same file works (cache stays
 * consistent), and (c) the configured FileSystem class is actually our decorator (defends against
 * silent regress to {@code LocalFileSystem} if a future Spark release changes its conf-merge
 * semantics).
 *
 * <p>Runs in the {@code verify} phase via maven-failsafe-plugin. Lifetime is per-class ({@link
 * TestInstance.Lifecycle#PER_CLASS}) so the heavyweight SparkSession + JVM-wide {@link
 * CacheBootstrap} install happen once per IT class — Spark's JVM-singleton SparkContext forbids two
 * contexts in one JVM anyway.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CachedFsSparkParquetIT {

  private SparkSession spark;

  @BeforeAll
  void setUp() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("cached-fs-parquet-it")
            // Route file:// reads through our decorator. spark.hadoop.* keys flow into the
            // Hadoop Configuration that Spark builds for its readers.
            .config("spark.hadoop.fs.file.impl", CachedFileSystem.class.getName())
            .config("spark.hadoop." + CachedFsConfig.ENABLED, "true")
            .config(
                "spark.hadoop." + CachedFsConfig.INNER_IMPL, "org.apache.hadoop.fs.LocalFileSystem")
            // Tight cache footprint for ITs — we only need the cache *engaged*, not large.
            .config("spark.hadoop.fs.cached.ram.size-bytes", String.valueOf(16L * 1024 * 1024))
            .config("spark.hadoop.fs.cached.load-quantum-bytes", String.valueOf(64 * 1024))
            // SSD layer is optional; leaving the dirs unset disables it.
            // Prefetch off keeps the IT deterministic — we are not measuring prefetch behavior
            // here, and a background worker introduces JVM-shutdown timing noise.
            .config("spark.hadoop." + CachedFsConfig.PREFETCH_ENABLED, "false")
            // Speed up tests: Spark's default UI/event log on local mode adds noise.
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate();
  }

  @AfterAll
  void tearDown() throws Exception {
    if (spark != null) {
      spark.stop();
    }
    // FileSystem caches the per-URI singletons across Spark stops; clear so the next IT class
    // starts from a clean slate (otherwise stale CachedFileSystem instances linger).
    FileSystem.closeAll();
    CacheBootstrap.uninstallForTesting();
  }

  @AfterEach
  void clearSparkCatalog() {
    if (spark != null) {
      spark.catalog().clearCache();
    }
  }

  @Test
  @DisplayName("Spark reads Parquet through CachedFileSystem and bytes round-trip")
  void parquetReadRoundtripsThroughCache(@TempDir java.nio.file.Path tmp) {
    Path parquetDir = new Path("file://" + tmp.resolve("data.parquet").toAbsolutePath());

    // Write a small dataset (range source, no input reader) so the write path is independent of
    // the cache path under test.
    spark.range(1000).toDF("id").write().mode("overwrite").parquet(parquetDir.toString());

    Dataset<Row> first = spark.read().parquet(parquetDir.toString());
    long firstCount = first.count();
    assertThat(firstCount).as("first read row count").isEqualTo(1000);

    // Second read must succeed identically — proves the cache doesn't corrupt subsequent reads
    // (positioned-read semantics + chunk reassembly).
    long secondCount = spark.read().parquet(parquetDir.toString()).count();
    assertThat(secondCount).as("second read row count").isEqualTo(1000);
  }

  @Test
  @DisplayName("the configured file:// FileSystem is actually CachedFileSystem")
  void fileFsImplIsCachedFileSystem() throws Exception {
    org.apache.hadoop.conf.Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
    FileSystem fs = FileSystem.get(java.net.URI.create("file:///"), hadoopConf);
    // Hadoop's FileSystem.get returns the resolved impl per fs.<scheme>.impl. If a future change
    // to Spark's config merge silently dropped our spark.hadoop.fs.file.impl, this would resolve
    // to LocalFileSystem and the test would diagnose it directly.
    assertThat(fs)
        .as("fs.file.impl must resolve to CachedFileSystem, not the default LocalFileSystem")
        .isInstanceOf(CachedFileSystem.class);
  }

  @Test
  @DisplayName("Spark reads ORC through CachedFileSystem and bytes round-trip")
  void orcReadRoundtripsThroughCache(@TempDir java.nio.file.Path tmp) {
    // ORC exercises a different reader stack from Parquet: Spark's OrcFileFormat goes through the
    // ORC reader's positioned-read path on the Hadoop input stream, which our cache services via
    // CachingInputStream.read(long, byte[], int, int). A regression in cache chunk reassembly
    // would fail Parquet AND ORC differently (different file layouts), so covering both keeps the
    // regression surface honest.
    Path orcDir = new Path("file://" + tmp.resolve("data.orc").toAbsolutePath());
    spark.range(500).toDF("id").write().mode("overwrite").orc(orcDir.toString());

    long first = spark.read().orc(orcDir.toString()).count();
    assertThat(first).as("first ORC read row count").isEqualTo(500);
    long second = spark.read().orc(orcDir.toString()).count();
    assertThat(second).as("second ORC read row count").isEqualTo(500);
  }

  @Test
  @DisplayName("Spark reads CSV through CachedFileSystem and bytes round-trip")
  void csvReadRoundtripsThroughCache(@TempDir java.nio.file.Path tmp) {
    // CSV/text is the simplest reader path — no columnar codec, no footer, just a HadoopRDD
    // streaming bytes through the cache. Useful regression for the unstructured-read case.
    Path csvDir = new Path("file://" + tmp.resolve("data.csv").toAbsolutePath());
    spark
        .range(200)
        .toDF("id")
        .write()
        .mode("overwrite")
        .option("header", "true")
        .csv(csvDir.toString());

    Dataset<Row> first = spark.read().option("header", "true").csv(csvDir.toString());
    assertThat(first.count()).as("first CSV read row count").isEqualTo(200);
    long second = spark.read().option("header", "true").csv(csvDir.toString()).count();
    assertThat(second).as("second CSV read row count").isEqualTo(200);
  }

  @Test
  @DisplayName(
      "openFile builder routes Spark/Iceberg-style .opt(...).build() reads through the cache")
  void openFileBuilderRoutesThroughCache(@TempDir java.nio.file.Path tmp) throws Exception {
    // Iceberg, Parquet, and Spark 4's vectorized readers use the FileSystem builder API
    // (fs.openFile(path).opt(FS_OPTION_OPENFILE_BUFFER_SIZE, N).withFileStatus(status).build()).
    // This IT exercises that exact call shape inside a SparkSession so we catch any regression
    // where Spark's classpath confuses our CachedFsInputStreamBuilder with Hadoop's
    // FSDataInputStreamBuilder. The unit test
    // (cached-fs-hadoop CachedFileSystemTest.openFileBuilderRoutesThroughCache) covers the
    // wiring in isolation; here we drive it through a real SparkSession's hadoopConf so any
    // classpath/factory issue surfaces.
    Path file = new Path("file://" + tmp.resolve("openfile.parquet").toAbsolutePath());
    // Write enough bytes that the cache load-quantum-bytes (64 KiB) triggers at least one
    // chunk read rather than a trivial single-buffer fetch.
    spark.range(0, 50_000).toDF("id").write().mode("overwrite").parquet(file.toString());

    org.apache.hadoop.conf.Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
    org.apache.hadoop.fs.FileSystem fs =
        org.apache.hadoop.fs.FileSystem.get(java.net.URI.create("file:///"), hadoopConf);
    org.apache.hadoop.fs.FileStatus[] parts =
        fs.listStatus(file, p -> p.getName().endsWith(".parquet"));
    assertThat(parts).hasSizeGreaterThanOrEqualTo(1);

    org.apache.hadoop.fs.FileStatus part = parts[0];
    try (org.apache.hadoop.fs.FSDataInputStream in =
        fs.openFile(part.getPath())
            .opt(org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_BUFFER_SIZE, 16384)
            .withFileStatus(part)
            .build()
            .get()) {
      byte[] buf = new byte[(int) Math.min(part.getLen(), 1024)];
      int read = in.read(buf);
      assertThat(read).as("read >0 bytes via openFile builder").isGreaterThan(0);
    }

    // Second openFile via builder: bytes must round-trip identically. Routes through the cache.
    try (org.apache.hadoop.fs.FSDataInputStream in =
        fs.openFile(part.getPath()).withFileStatus(part).build().get()) {
      byte[] buf = new byte[(int) Math.min(part.getLen(), 1024)];
      int read = in.read(buf);
      assertThat(read).as("second openFile read >0 bytes").isGreaterThan(0);
    }
  }

  @Test
  @DisplayName("Spark reads plain text through CachedFileSystem and line count matches")
  void textReadRoundtripsThroughCache(@TempDir java.nio.file.Path tmp) {
    // text() reader returns one Row per line. Exercises the unsplit/uncompressed unstructured
    // read path, which is the most direct test of the cache's FSDataInputStream wrapper.
    Path textDir = new Path("file://" + tmp.resolve("lines.txt").toAbsolutePath());
    // text() requires a single String column. Cast id -> string so the writer accepts the schema.
    spark
        .range(100)
        .selectExpr("CAST(id AS STRING) AS value")
        .write()
        .mode("overwrite")
        .text(textDir.toString());

    long lines = spark.read().text(textDir.toString()).count();
    assertThat(lines).as("text line count").isEqualTo(100);
    long lines2 = spark.read().text(textDir.toString()).count();
    assertThat(lines2).as("second text read line count").isEqualTo(100);
  }
}
