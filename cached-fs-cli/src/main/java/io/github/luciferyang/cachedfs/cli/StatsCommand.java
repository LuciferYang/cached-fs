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
import picocli.CommandLine.Spec;

/**
 * {@code cached-fs stats} — dumps an aggregated stats snapshot from a running cached-fs JVM.
 * Includes AggregatedIoStatistics totals, RAM/SSD cache stats, tracker counts, and recent-streams
 * ring counters.
 *
 * <p>{@code --window <seconds>} reserves the future delta-over-window mode; today the flag is
 * accepted but a single snapshot is always returned. Will be wired up when the bootstrap exposes
 * rolling rate buckets.
 */
@Command(
    name = "stats",
    description = "Dump cached-fs stats from a running JVM.",
    mixinStandardHelpOptions = true)
public final class StatsCommand implements Callable<Integer> {

  @Mixin JmxOptions jmx;

  @Option(
      names = {"-w", "--window"},
      description =
          "Reserved for delta-over-window mode (seconds). Accepted but ignored today; emits a"
              + " single snapshot.")
  Integer windowSeconds;

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    try (JmxClient client = jmx.connect()) {
      PrintWriter out = spec != null ? spec.commandLine().getOut() : new PrintWriter(System.out);
      out.print(client.proxy().stats());
      out.flush();
    }
    return 0;
  }
}
