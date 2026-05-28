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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class ConfigCommandTest {

  @Test
  @DisplayName("config dumps defaults when no --conf and no -D is given")
  void dumpsDefaults() {
    ConfigCommand cmd = new ConfigCommand();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int code = cmd.run(new PrintStream(out), new PrintStream(err));

    assertThat(code).isZero();
    String stdout = out.toString();
    // Defaults from CachedFsConfig: ENABLED=false, SCAN_TRACKER_ENABLED=true,
    // SCAN_TRACKER_MAX_ENTRIES_PER_TRACKER=10000, COALESCE_ENABLED=true, PREFETCH_ENABLED=true,
    // METRICS_ENABLED=true.
    assertThat(stdout).contains("fs.cached.enabled=false");
    assertThat(stdout).contains("fs.cached.scan-tracker.enabled=true");
    assertThat(stdout).contains("fs.cached.scan-tracker.max-entries-per-tracker=10000");
    assertThat(stdout).contains("fs.cached.metrics.enabled=true");
    assertThat(stdout).contains("fs.cached.prefetch.enabled=true");
    assertThat(err.toString()).isEmpty();
  }

  @Test
  @DisplayName(
      "inline -D overrides flip resolved values; output is alphabetized for grep-friendly diff")
  void inlineOverridesFlipValues() {
    ConfigCommand cmd = new ConfigCommand();
    cmd.overrides =
        java.util.Map.of(
            "fs.cached.enabled", "true",
            "fs.cached.scan-tracker.max-entries-per-tracker", "42",
            "fs.cached.prefetch.enabled", "false");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int code = cmd.run(new PrintStream(out), new PrintStream(new ByteArrayOutputStream()));

    assertThat(code).isZero();
    String stdout = out.toString();
    assertThat(stdout).contains("fs.cached.enabled=true");
    assertThat(stdout).contains("fs.cached.scan-tracker.max-entries-per-tracker=42");
    assertThat(stdout).contains("fs.cached.prefetch.enabled=false");
    // Alphabetized: enabled comes before metrics which comes before prefetch.
    int enabledIdx = stdout.indexOf("fs.cached.enabled=");
    int metricsIdx = stdout.indexOf("fs.cached.metrics.enabled=");
    int prefetchIdx = stdout.indexOf("fs.cached.prefetch.enabled=");
    assertThat(enabledIdx).isLessThan(metricsIdx).isLessThan(prefetchIdx);
  }

  @Test
  @DisplayName("invalid range (load-quantum=0) exits 2 with offending key in the error message")
  void invalidRangeExitsTwo() {
    ConfigCommand cmd = new ConfigCommand();
    cmd.overrides = java.util.Map.of("fs.cached.load-quantum-bytes", "0");
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int code = cmd.run(new PrintStream(new ByteArrayOutputStream()), new PrintStream(err));

    assertThat(code).isEqualTo(2);
    assertThat(err.toString())
        .as("error message must name the offending key so ops can grep it")
        .contains("fs.cached.load-quantum-bytes");
  }

  @Test
  @DisplayName("--conf reads a real Hadoop XML file from disk")
  void readsConfXml(@TempDir Path tmp) throws Exception {
    Path xml = tmp.resolve("core-site.xml");
    Files.writeString(
        xml,
        "<?xml version=\"1.0\"?>\n"
            + "<configuration>\n"
            + "  <property><name>fs.cached.enabled</name><value>true</value></property>\n"
            + "  <property>\n"
            + "    <name>fs.cached.scan-tracker.max-entries-per-tracker</name>\n"
            + "    <value>5000</value>\n"
            + "  </property>\n"
            + "</configuration>\n");

    ConfigCommand cmd = new ConfigCommand();
    cmd.confFiles = java.util.List.of(xml.toFile());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int code = cmd.run(new PrintStream(out), new PrintStream(new ByteArrayOutputStream()));

    assertThat(code).isZero();
    assertThat(out.toString())
        .contains("fs.cached.enabled=true")
        .contains("fs.cached.scan-tracker.max-entries-per-tracker=5000");
  }

  @Test
  @DisplayName("missing --conf file exits 2 with a clear error message")
  void missingConfFileExitsTwo(@TempDir Path tmp) {
    ConfigCommand cmd = new ConfigCommand();
    cmd.confFiles = java.util.List.of(tmp.resolve("does-not-exist.xml").toFile());
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int code = cmd.run(new PrintStream(new ByteArrayOutputStream()), new PrintStream(err));

    assertThat(code).isEqualTo(2);
    assertThat(err.toString()).contains("not found").contains("does-not-exist.xml");
  }

  @Test
  @DisplayName("picocli `cached-fs config --help` exits 0 (smoke check for the subcommand wiring)")
  void picocliHelpWiring() {
    int code = new CommandLine(new CachedFsCli()).execute("config", "--help");
    assertThat(code).isZero();
  }
}
