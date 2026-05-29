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
package io.github.luciferyang.cachedfs.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.luciferyang.cachedfs.cli.jmx.JmxClient;
import io.github.luciferyang.cachedfs.hadoop.CacheBootstrap;
import io.github.luciferyang.cachedfs.hadoop.CachedFsConfig;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.util.Map;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Live-state CLI subcommands exercised against the local platform MBean server (the CLI's "no
 * --jmx-url" mode). Verifies command wiring, exit codes, and report shape; the underlying MBean
 * behavior is covered by {@code CachedFsBootstrapMBeanTest}.
 */
class LiveCommandsTest {

  @BeforeEach
  void bootstrap() throws IOException {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, LocalFileSystem.class.getName());
    CacheBootstrap.installIfNeeded(conf);
  }

  @AfterEach
  void teardown() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  @Test
  @DisplayName("stats subcommand prints a multi-section report and exits 0")
  void statsHappyPath() {
    StringWriter sw = new StringWriter();
    CommandLine cli = new CommandLine(new CachedFsCli()).setOut(new PrintWriter(sw));
    int code = cli.execute("stats");
    assertThat(code).isZero();
    String out = sw.toString();
    assertThat(out)
        .contains("=== cached-fs stats ===")
        .contains("--- aggregate-io ---")
        .contains("--- ram-cache ---");
  }

  @Test
  @DisplayName("stats --window samples twice and prints per-counter delta + per-second rate")
  void statsWindowPrintsRates() {
    StringWriter sw = new StringWriter();
    CommandLine cli = new CommandLine(new CachedFsCli()).setOut(new PrintWriter(sw));
    int code = cli.execute("stats", "--window", "1");
    assertThat(code).isZero();
    String out = sw.toString();
    assertThat(out)
        .contains("=== cached-fs stats --window 1s")
        .contains("(elapsed ")
        .contains("counter")
        .contains("current")
        .contains("delta")
        .contains("per-sec")
        .contains("io.read-bytes");
  }

  @Test
  @DisplayName("windowRow computes signed delta + per-second rate (pure, no sleep)")
  void windowRowMath() {
    // before=100, now=340 over 2s → delta +240, rate 120.0/s. Guards the delta/rate arithmetic
    // without a real window wait (the wiring is covered by statsWindowPrintsRates).
    String row = StatsCommand.windowRow("io.read-bytes", 100L, 340L, 2.0);
    assertThat(row).contains("io.read-bytes").contains("340").contains("+240").contains("120.0");
    // elapsed <= 0 must not divide-by-zero; rate falls back to 0.0.
    assertThat(StatsCommand.windowRow("k", 5L, 5L, 0.0)).contains("+0").contains("0.0");
    // Negative delta (a gauge that fell) renders with a leading minus.
    assertThat(StatsCommand.windowRow("g", 10L, 4L, 1.0)).contains("-6");
  }

  @Test
  @DisplayName("counters() round-trips through the MXBean proxy as a Map (guards newMXBeanProxy)")
  void countersThroughProxyIsAMap() throws Exception {
    // A regression to JMX.newMBeanProxy would hand back TabularData and ClassCastException here.
    // Sleep-free, unlike statsWindowPrintsRates, so this guard survives any CI speed-up that
    // trims sleeping tests.
    try (io.github.luciferyang.cachedfs.cli.jmx.JmxClient client =
        io.github.luciferyang.cachedfs.cli.jmx.JmxClient.local(
            CachedFsConfig.DEFAULT_JMX_OBJECT_NAME)) {
      java.util.Map<String, Long> c = client.proxy().counters();
      assertThat(c).isNotEmpty().containsKey("io.read-bytes");
    }
  }

  @Test
  @DisplayName("counters() round-trips over a real RMI connector (remote connect + close)")
  void countersOverRealRmiConnector() throws Exception {
    // Exercises the production remote path that the local() tests skip: JMXConnectorFactory.connect
    // → newMXBeanProxy(conn, …) → counters() serialized as TabularData over RMI and reconstructed
    // into a Map. The `rmi://localhost:0` form uses an ephemeral server port with no external
    // registry, so it stands up entirely in-process.
    JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://localhost:0");
    JMXConnectorServer server =
        JMXConnectorServerFactory.newJMXConnectorServer(
            url, null, ManagementFactory.getPlatformMBeanServer());
    server.start();
    try {
      String address = server.getAddress().toString();
      try (JmxClient client = JmxClient.connect(address, CachedFsConfig.DEFAULT_JMX_OBJECT_NAME)) {
        Map<String, Long> c = client.proxy().counters();
        assertThat(c).isNotEmpty().containsKey("io.read-bytes");
        // A scalar op over the same remote proxy must also work.
        assertThat(client.proxy().stats()).contains("=== cached-fs stats ===");
      }
    } finally {
      server.stop();
    }
  }

  @Test
  @DisplayName("recent-streams --limit -1 is rejected client-side with exit code 2")
  void recentStreamsRejectsNegativeLimit() {
    StringWriter errW = new StringWriter();
    CommandLine cli = new CommandLine(new CachedFsCli()).setErr(new PrintWriter(errW));
    int code = cli.execute("recent-streams", "--limit", "-1");
    assertThat(code).isEqualTo(2);
    assertThat(errW.toString()).contains("--limit must be >= 0");
  }

  @Test
  @DisplayName("a JMX connection failure prints a clean error + exit 1, not a stack trace")
  void jmxConnectionFailureIsHandledCleanly() {
    // A malformed service URL makes JmxClient.connect() throw immediately (no network wait). The
    // execution-exception handler must turn that into "error: ..." on stderr + exit 1, rather than
    // picocli's default rethrow (raw stack trace).
    StringWriter errW = new StringWriter();
    CommandLine cli = CachedFsCli.newCommandLine().setErr(new PrintWriter(errW));
    int code = cli.execute("stats", "--jmx-url", "not-a-valid-jmx-url");
    assertThat(code).isEqualTo(1);
    assertThat(errW.toString()).startsWith("error:");
  }

  @Test
  @DisplayName("stats --window 0 is rejected with exit code 2")
  void statsWindowRejectsNonPositive() {
    StringWriter errW = new StringWriter();
    CommandLine cli = new CommandLine(new CachedFsCli()).setErr(new PrintWriter(errW));
    int code = cli.execute("stats", "--window", "0");
    assertThat(code).isEqualTo(2);
    assertThat(errW.toString()).contains("--window must be > 0");
  }

  @Test
  @DisplayName("inspect subcommand reports open-handle false for an unknown key")
  void inspectUnknownKey() {
    StringWriter sw = new StringWriter();
    CommandLine cli = new CommandLine(new CachedFsCli()).setOut(new PrintWriter(sw));
    int code = cli.execute("inspect", "hdfs://nn-a/does-not-exist");
    assertThat(code).isZero();
    String out = sw.toString();
    assertThat(out).contains("key: hdfs://nn-a/does-not-exist").contains("open-handle: false");
  }

  @Test
  @DisplayName("recent-streams subcommand emits the section header even when the ring is empty")
  void recentStreamsEmpty() {
    StringWriter sw = new StringWriter();
    CommandLine cli = new CommandLine(new CachedFsCli()).setOut(new PrintWriter(sw));
    int code = cli.execute("recent-streams");
    assertThat(code).isZero();
    assertThat(sw.toString()).contains("=== cached-fs recent-streams ===").contains("count: 0");
  }

  @Test
  @DisplayName("drain without --yes exits with code 3 and prints a confirmation reminder to stderr")
  void drainRequiresYes() {
    StringWriter outW = new StringWriter();
    StringWriter errW = new StringWriter();
    CommandLine cli =
        new CommandLine(new CachedFsCli())
            .setOut(new PrintWriter(outW))
            .setErr(new PrintWriter(errW));
    int code = cli.execute("drain");
    assertThat(code).isEqualTo(3);
    assertThat(errW.toString()).contains("destructive").contains("--yes");
  }

  @Test
  @DisplayName("drain --yes on an empty bootstrap reports 0 handles drained")
  void drainHappyPath() {
    StringWriter sw = new StringWriter();
    CommandLine cli = new CommandLine(new CachedFsCli()).setOut(new PrintWriter(sw));
    int code = cli.execute("drain", "--yes");
    assertThat(code).isZero();
    assertThat(sw.toString()).contains("drained: 0 handles");
  }

  @Test
  @DisplayName("purge without --yes exits with code 3 (destructive op guard)")
  void purgeRequiresYes() {
    StringWriter errW = new StringWriter();
    CommandLine cli = new CommandLine(new CachedFsCli()).setErr(new PrintWriter(errW));
    int code = cli.execute("purge", ".*");
    assertThat(code).isEqualTo(3);
    assertThat(errW.toString()).contains("destructive").contains("--yes");
  }

  @Test
  @DisplayName("purge --yes with an invalid regex exits with code 2 (bad input)")
  void purgeInvalidRegex() {
    StringWriter errW = new StringWriter();
    CommandLine cli = new CommandLine(new CachedFsCli()).setErr(new PrintWriter(errW));
    int code = cli.execute("purge", "[unclosed", "--yes");
    assertThat(code).isEqualTo(2);
    assertThat(errW.toString()).contains("invalid regex");
  }

  @Test
  @DisplayName("purge --yes with a valid regex on an empty cache reports 0 purged")
  void purgeHappyPath() {
    StringWriter sw = new StringWriter();
    CommandLine cli = new CommandLine(new CachedFsCli()).setOut(new PrintWriter(sw));
    int code = cli.execute("purge", "hdfs://.*", "--yes");
    assertThat(code).isZero();
    assertThat(sw.toString()).contains("purged: 0 handles matching /hdfs://.*/");
  }
}
