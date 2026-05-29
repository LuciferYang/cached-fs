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
import java.util.Map;
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
 * <p>{@code --window <seconds>} switches to delta-over-window mode: the command snapshots the typed
 * counters, waits the window, snapshots again, and prints the change and per-second rate for each
 * counter (the way {@code iostat 5} / {@code vmstat 5} report). It is a client-side two-shot — no
 * background sampler runs in the target JVM — so the command blocks for the window duration.
 */
@Command(
    name = "stats",
    description = "Dump cached-fs stats (or per-second rates with --window) from a running JVM.",
    mixinStandardHelpOptions = true)
public final class StatsCommand implements Callable<Integer> {

  @Mixin JmxOptions jmx;

  @Option(
      names = {"-w", "--window"},
      description =
          "Sample twice this many seconds apart and print per-counter change + per-second rate"
              + " instead of a single snapshot.")
  Integer windowSeconds;

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    PrintWriter out = spec != null ? spec.commandLine().getOut() : new PrintWriter(System.out);
    PrintWriter err = spec != null ? spec.commandLine().getErr() : new PrintWriter(System.err);

    if (windowSeconds != null && windowSeconds <= 0) {
      err.println("--window must be > 0 (seconds): " + windowSeconds);
      err.flush();
      return 2;
    }

    try (JmxClient client = jmx.connect()) {
      if (windowSeconds == null) {
        out.print(client.proxy().stats());
        out.flush();
        return 0;
      }
      printWindow(out, client, windowSeconds);
    }
    return 0;
  }

  private static void printWindow(PrintWriter out, JmxClient client, int windowSeconds)
      throws InterruptedException {
    Map<String, Long> first = client.proxy().counters();
    long startNanos = System.nanoTime();
    Thread.sleep(windowSeconds * 1000L);
    Map<String, Long> second = client.proxy().counters();
    double elapsedSeconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;

    out.printf(
        "=== cached-fs stats --window %ds (elapsed %.3fs) ===%n", windowSeconds, elapsedSeconds);
    out.printf("%-42s %16s %16s %16s%n", "counter", "current", "delta", "per-sec");
    for (Map.Entry<String, Long> e : second.entrySet()) {
      String key = e.getKey();
      long now = e.getValue();
      // Counters never disappear between snapshots, but guard defensively rather than NPE.
      long before = first.getOrDefault(key, now);
      long delta = now - before;
      double perSec = elapsedSeconds > 0 ? delta / elapsedSeconds : 0.0;
      out.printf("%-42s %16d %+16d %16.1f%n", key, now, delta, perSec);
    }
    out.flush();
  }
}
