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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class VersionCommandTest {

  @Test
  @DisplayName("version prints `cached-fs <version|unknown>` and exits 0")
  void printsVersionLine() {
    VersionCommand cmd = new VersionCommand();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int code = cmd.run(new PrintStream(out));

    assertThat(code).isZero();
    // Under test (no packaged jar), Implementation-Version is null → "unknown" fallback.
    assertThat(out.toString().trim()).startsWith("cached-fs ");
  }

  @Test
  @DisplayName("picocli `cached-fs version` exits 0 (smoke check for the subcommand wiring)")
  void picocliWiring() {
    int code = new CommandLine(new CachedFsCli()).execute("version");
    assertThat(code).isZero();
  }

  @Test
  @DisplayName("bare `cached-fs` (no subcommand) exits 0 by printing usage")
  void bareInvocationPrintsUsage() {
    int code = new CommandLine(new CachedFsCli()).execute();
    assertThat(code).isZero();
  }
}
