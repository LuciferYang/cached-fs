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
 * {@code cached-fs recent-streams} — emits the bootstrap's ring of recent {@code
 * IoStatisticsSnapshot}s for post-hoc debugging without instrumenting every reader.
 */
@Command(
    name = "recent-streams",
    description = "Dump the recent-streams ring (post-hoc per-stream debugging).",
    mixinStandardHelpOptions = true)
public final class RecentStreamsCommand implements Callable<Integer> {

  @Mixin JmxOptions jmx;

  @Option(
      names = {"-n", "--limit"},
      defaultValue = "0",
      description =
          "Max entries to emit (most-recent-first). 0 = all retained. Default: ${DEFAULT-VALUE}")
  int limit;

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    PrintWriter out = spec != null ? spec.commandLine().getOut() : new PrintWriter(System.out);
    PrintWriter err = spec != null ? spec.commandLine().getErr() : new PrintWriter(System.err);
    // Validate client-side so a bad value exits 2 (bad input), consistent with stats --window /
    // purge — rather than letting the MBean throw and surface as a generic exit-1 runtime error.
    if (limit < 0) {
      err.println("--limit must be >= 0 (0 = all): " + limit);
      err.flush();
      return 2;
    }
    try (JmxClient client = jmx.connect()) {
      out.print(client.proxy().recentStreams(limit));
      out.flush();
    }
    return 0;
  }
}
