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
 * cached-fs CLI entry point. Today ships two subcommands:
 *
 * <ul>
 *   <li>{@code config} — validate a Hadoop config XML and dump the effective {@code fs.cached.*}
 *       keys with defaults filled in. Run at deploy time to catch range/type errors before the JVM
 *       installs the cache.
 *   <li>{@code version} — print the cached-fs build version.
 * </ul>
 *
 * <p>Production live-state inspection (cache contents, hit/miss for a specific path) is
 * intentionally NOT here today — it requires JMX/RPC infrastructure that doesn't exist in cached-fs
 * yet. The Micrometer/Prometheus surface from {@code cached-fs-metrics} covers the cumulative
 * observability use case; per-path live state would need a daemon to query. Flagged as a follow-up.
 *
 * <p>Built as an executable jar with picocli's annotation processor; run via:
 *
 * <pre>{@code
 * java -jar cached-fs-cli-0.1.0-SNAPSHOT.jar config --conf core-site.xml
 * }</pre>
 */
@Command(
    name = "cached-fs",
    description = "cached-fs ops + diagnostic CLI",
    mixinStandardHelpOptions = true,
    subcommands = {ConfigCommand.class, VersionCommand.class})
public final class CachedFsCli implements Runnable {

  @Override
  public void run() {
    // Bare invocation with no subcommand → print usage and exit 0.
    CommandLine.usage(this, System.out);
  }

  public static void main(String[] args) {
    int exit = new CommandLine(new CachedFsCli()).execute(args);
    System.exit(exit);
  }
}
