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
 * {@code cached-fs purge <regex>} — closes every cached file handle whose key matches the given
 * Java regex pattern.
 *
 * <p>Mutating operation; requires {@code --yes}. Invalid regex is reported as exit code 2; without
 * {@code --yes} the command exits with code 3.
 */
@Command(
    name = "purge",
    description = "Close every cached handle matching a regex (requires --yes).",
    mixinStandardHelpOptions = true)
public final class PurgeCommand implements Callable<Integer> {

  @Parameters(
      index = "0",
      paramLabel = "<regex>",
      description = "Java regex (java.util.regex.Pattern) matched against the full key.")
  String regex;

  @Mixin JmxOptions jmx;

  @Option(
      names = {"-y", "--yes"},
      description = "Required confirmation. Without this flag the command exits with code 3.")
  boolean confirmed;

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    PrintWriter out = spec != null ? spec.commandLine().getOut() : new PrintWriter(System.out);
    PrintWriter err = spec != null ? spec.commandLine().getErr() : new PrintWriter(System.err);
    if (!confirmed) {
      err.println("purge is destructive; re-run with --yes to confirm.");
      err.flush();
      return 3;
    }
    // Pre-validate the regex client-side so a typo doesn't reach the JMX server and surface as an
    // opaque MBeanException. Exit code 2 = bad input, matching ConfigCommand's range-error code.
    try {
      java.util.regex.Pattern.compile(regex);
    } catch (java.util.regex.PatternSyntaxException ex) {
      err.println("invalid regex: " + ex.getDescription() + " at index " + ex.getIndex());
      err.flush();
      return 2;
    }
    try (JmxClient client = jmx.connect()) {
      long closed = client.proxy().purgeMatching(regex);
      out.printf("purged: %d handles matching /%s/%n", closed, regex);
      out.flush();
    }
    return 0;
  }
}
