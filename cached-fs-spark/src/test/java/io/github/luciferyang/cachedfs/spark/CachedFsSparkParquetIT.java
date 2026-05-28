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
            // Pin driver to loopback so the build doesn't hang on CI hosts with unusual
            // network setups (containers without a usable hostname → InetAddress.getLocalHost
            // throws and SparkEnv setup stalls). Loopback is fine for local-mode ITs.
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            // Pin Spark scratch + warehouse under target/ so they never pollute the repo
            // working tree. Without this, spark-warehouse/, metastore_db/, and derby.log
            // can leak to the module root.
            .config(
                "spark.sql.warehouse.dir",
                "file:" + System.getProperty("user.dir") + "/target/spark-warehouse")
            .config("spark.local.dir", System.getProperty("user.dir") + "/target/spark-local")
            .getOrCreate();
  }

  @AfterAll
  void tearDown() {
    // Each release in its own try/catch so a failure in spark.stop doesn't silently skip
    // FileSystem.closeAll and CacheBootstrap.uninstallForTesting. Failsafe forkCount=1
    // reuseForks=false gives a hard JVM-exit safety net, but a cascading teardown exception
    // masks the original test failure with a noisier secondary cause.
    try {
      if (spark != null) {
        spark.stop();
      }
    } catch (RuntimeException ignored) {
    }
    try {
      // FileSystem caches the per-URI singletons across Spark stops; clear so the next IT
      // class starts from a clean slate (otherwise stale CachedFileSystem instances linger).
      FileSystem.closeAll();
    } catch (java.io.IOException | RuntimeException ignored) {
    }
    try {
      CacheBootstrap.uninstallForTesting();
    } catch (java.io.IOException | RuntimeException ignored) {
    }
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

    // Collect actual id values, not just the count: a cache that corrupted column data while
    // leaving the Parquet footer intact would still yield count=1000 because Parquet returns
    // row count from metadata, not from re-decoding the data column. Set equality on the IDs
    // catches that class of regression.
    java.util.List<Long> firstIds =
        spark
            .read()
            .parquet(parquetDir.toString())
            .as(org.apache.spark.sql.Encoders.LONG())
            .collectAsList();
    java.util.List<Long> expected =
        java.util.stream.LongStream.range(0, 1000)
            .boxed()
            .collect(java.util.stream.Collectors.toList());
    assertThat(firstIds)
        .as("first read id values match expected range — defends column-data corruption")
        .containsExactlyInAnyOrderElementsOf(expected);

    // Second read: identical content. Proves the cache returns consistent bytes across reads
    // (positioned-read semantics + chunk reassembly stable across cache hits vs misses).
    java.util.List<Long> secondIds =
        spark
            .read()
            .parquet(parquetDir.toString())
            .as(org.apache.spark.sql.Encoders.LONG())
            .collectAsList();
    assertThat(secondIds)
        .as("second read returns identical content")
        .containsExactlyInAnyOrderElementsOf(expected);
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

    java.util.List<Long> expected =
        java.util.stream.LongStream.range(0, 500)
            .boxed()
            .collect(java.util.stream.Collectors.toList());
    java.util.List<Long> firstIds =
        spark
            .read()
            .orc(orcDir.toString())
            .as(org.apache.spark.sql.Encoders.LONG())
            .collectAsList();
    assertThat(firstIds)
        .as("first ORC read id values match — defends column-data corruption")
        .containsExactlyInAnyOrderElementsOf(expected);
    java.util.List<Long> secondIds =
        spark
            .read()
            .orc(orcDir.toString())
            .as(org.apache.spark.sql.Encoders.LONG())
            .collectAsList();
    assertThat(secondIds)
        .as("second ORC read returns identical content")
        .containsExactlyInAnyOrderElementsOf(expected);
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

    java.util.Set<String> expected =
        java.util.stream.LongStream.range(0, 200)
            .mapToObj(String::valueOf)
            .collect(java.util.stream.Collectors.toSet());
    java.util.Set<String> firstIds =
        spark.read().option("header", "true").csv(csvDir.toString()).collectAsList().stream()
            .map(r -> r.getString(0))
            .collect(java.util.stream.Collectors.toSet());
    assertThat(firstIds)
        .as("first CSV read values match — defends row-data corruption")
        .isEqualTo(expected);
    java.util.Set<String> secondIds =
        spark.read().option("header", "true").csv(csvDir.toString()).collectAsList().stream()
            .map(r -> r.getString(0))
            .collect(java.util.stream.Collectors.toSet());
    assertThat(secondIds).as("second CSV read returns identical content").isEqualTo(expected);
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

    // Reference bytes via java.nio out-of-band (bypassing Hadoop entirely) so we have a
    // known-good baseline to compare against. A 'read > 0' alone would silently pass if the
    // builder fell through to the inner FS — the exact regression we're defending against.
    int probeLen = (int) Math.min(part.getLen(), 1024);
    byte[] referenceBytes = new byte[probeLen];
    try (java.io.InputStream raw =
        java.nio.file.Files.newInputStream(java.nio.file.Path.of(part.getPath().toUri()))) {
      int filled = raw.read(referenceBytes);
      assertThat(filled).as("reference read filled the probe buffer").isEqualTo(probeLen);
    }

    try (org.apache.hadoop.fs.FSDataInputStream in =
        fs.openFile(part.getPath())
            .opt(org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_BUFFER_SIZE, 16384)
            .withFileStatus(part)
            .build()
            .get()) {
      // Wrap-class assertion: the stream MUST come from cached-fs's CachingInputStream wrapper,
      // not a fall-through to the inner FS. This is the exact regression the test claims to
      // defend against — a 'read > 0' check would silently pass on a working inner-FS fallback.
      assertThat(in.getWrappedStream().getClass().getName())
          .as("openFile builder must route through CachedFileSystem's CachingInputStream")
          .contains("CachingInputStream");

      byte[] buf = new byte[probeLen];
      org.apache.hadoop.io.IOUtils.readFully(in, buf, 0, probeLen);
      assertThat(buf)
          .as("bytes via openFile builder match the reference out-of-band read")
          .isEqualTo(referenceBytes);
    }

    // Second openFile via builder: hot path (cache hit). Must still route through
    // CachingInputStream and yield identical bytes.
    try (org.apache.hadoop.fs.FSDataInputStream in =
        fs.openFile(part.getPath()).withFileStatus(part).build().get()) {
      assertThat(in.getWrappedStream().getClass().getName())
          .as("second openFile must also route through CachingInputStream")
          .contains("CachingInputStream");
      byte[] buf = new byte[probeLen];
      org.apache.hadoop.io.IOUtils.readFully(in, buf, 0, probeLen);
      assertThat(buf).as("second openFile bytes match reference").isEqualTo(referenceBytes);
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

    java.util.Set<String> expected =
        java.util.stream.LongStream.range(0, 100)
            .mapToObj(String::valueOf)
            .collect(java.util.stream.Collectors.toSet());
    java.util.Set<String> firstLines =
        spark.read().text(textDir.toString()).collectAsList().stream()
            .map(r -> r.getString(0))
            .collect(java.util.stream.Collectors.toSet());
    assertThat(firstLines)
        .as("first text read line values match — defends row-data corruption")
        .isEqualTo(expected);
    java.util.Set<String> secondLines =
        spark.read().text(textDir.toString()).collectAsList().stream()
            .map(r -> r.getString(0))
            .collect(java.util.stream.Collectors.toSet());
    assertThat(secondLines).as("second text read returns identical content").isEqualTo(expected);
  }
}
