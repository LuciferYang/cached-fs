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

import io.github.luciferyang.cachedfs.cli.jmx.JmxClient;
import io.github.luciferyang.cachedfs.cli.jmx.JmxOptions;
import java.io.PrintWriter;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * {@code cached-fs inspect <key>} — queries the running bootstrap for the state of one cache key
 * (full file URI string, e.g. {@code hdfs://nn-a/data/part-00001.parquet}).
 *
 * <p>Reports whether an open handle exists, the total handle count, and a per-tracker breakdown.
 * Read-only — does NOT mutate the cache or bump LRU order.
 */
@Command(
    name = "inspect",
    description = "Inspect cache state for a single key against a running cached-fs JVM.",
    mixinStandardHelpOptions = true)
public final class InspectCommand implements Callable<Integer> {

  @Parameters(
      index = "0",
      paramLabel = "<key>",
      description = "Full file URI to inspect (matches cache key exactly).")
  String key;

  @Mixin JmxOptions jmx;

  @Option(
      names = {"-o", "--output"},
      description = "Write report to file instead of stdout.")
  java.io.File outputFile;

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    try (JmxClient client = jmx.connect()) {
      String report = client.proxy().inspect(key);
      writeReport(report);
    }
    return 0;
  }

  private void writeReport(String report) throws java.io.IOException {
    if (outputFile != null) {
      java.nio.file.Files.writeString(outputFile.toPath(), report);
      return;
    }
    PrintWriter out = spec != null ? spec.commandLine().getOut() : new PrintWriter(System.out);
    out.print(report);
    out.flush();
  }
}
