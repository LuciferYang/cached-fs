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
 * {@code cached-fs drain} — closes every cached file handle on the target JVM.
 *
 * <p>This is a mutating operation. Without {@code --yes} the command refuses to run and prints a
 * confirmation message — operators MUST acknowledge before any handles are closed. Reads in-flight
 * finish from already-pinned chunks; new opens after drain repopulate the cache.
 */
@Command(
    name = "drain",
    description = "Close every cached file handle on a running cached-fs JVM (requires --yes).",
    mixinStandardHelpOptions = true)
public final class DrainCommand implements Callable<Integer> {

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
      err.println("drain is destructive; re-run with --yes to confirm.");
      err.flush();
      return 3;
    }
    try (JmxClient client = jmx.connect()) {
      long closed = client.proxy().drain();
      out.printf("drained: %d handles%n", closed);
      out.flush();
    }
    return 0;
  }
}
