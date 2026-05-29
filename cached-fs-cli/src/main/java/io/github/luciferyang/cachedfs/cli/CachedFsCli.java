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

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * cached-fs CLI entry point.
 *
 * <p>Static (offline) subcommands:
 *
 * <ul>
 *   <li>{@code config} — validate a Hadoop config XML and dump the effective {@code fs.cached.*}
 *       keys with defaults filled in.
 *   <li>{@code version} — print the cached-fs build version.
 * </ul>
 *
 * <p>Live-state subcommands (connect to a running JVM via JMX):
 *
 * <ul>
 *   <li>{@code inspect <key>} — does this key have an open handle? Per-tracker breakdown.
 *   <li>{@code stats} — aggregate IO + RAM/SSD cache + tracker + recent-streams snapshot.
 *   <li>{@code recent-streams} — dump the recent {@code IoStatisticsSnapshot} ring.
 *   <li>{@code drain} — close every cached handle. Requires {@code --yes}.
 *   <li>{@code purge <regex>} — close handles whose key matches a Java regex. Requires {@code
 *       --yes}.
 * </ul>
 *
 * <p>Live commands default to the local platform MBean server (in-process). Pass {@code --jmx-url}
 * (e.g. {@code service:jmx:rmi:///jndi/rmi://host:9999/jmxrmi}) to target a remote JVM.
 *
 * <p>Built as an executable jar with picocli's annotation processor; run via:
 *
 * <pre>{@code
 * java -jar cached-fs-cli-0.1.0-SNAPSHOT.jar stats --jmx-url service:jmx:rmi:///jndi/rmi://host:9999/jmxrmi
 * }</pre>
 */
@Command(
    name = "cached-fs",
    description = "cached-fs ops + diagnostic CLI",
    mixinStandardHelpOptions = true,
    subcommands = {
      ConfigCommand.class,
      VersionCommand.class,
      InspectCommand.class,
      StatsCommand.class,
      RecentStreamsCommand.class,
      DrainCommand.class,
      PurgeCommand.class,
    })
public final class CachedFsCli implements Runnable {

  /** Exit code for an unhandled execution failure (e.g. a JMX connection error). */
  static final int EXIT_RUNTIME_ERROR = 1;

  @Override
  public void run() {
    // Bare invocation with no subcommand → print usage and exit 0.
    CommandLine.usage(this, System.out);
  }

  /**
   * Renders execution failures as a one-line message + defined exit code instead of picocli's
   * default rethrow (which dumps a raw stack trace and exits 1 via the uncaught-exception path).
   * The live commands talk to a possibly-remote JVM over JMX, so a connection failure / RMI error
   * is an expected operational outcome, not a bug to stack-trace.
   */
  static CommandLine newCommandLine() {
    return new CommandLine(new CachedFsCli())
        .setExecutionExceptionHandler(
            (ex, commandLine, parseResult) -> {
              commandLine.getErr().println("error: " + ex.getMessage());
              return EXIT_RUNTIME_ERROR;
            });
  }

  public static void main(String[] args) {
    int exit = newCommandLine().execute(args);
    System.exit(exit);
  }
}
