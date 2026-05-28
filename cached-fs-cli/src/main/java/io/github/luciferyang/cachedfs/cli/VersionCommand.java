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

import java.io.PrintStream;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;

/**
 * {@code cached-fs version} — print the cached-fs build version. The version is read from the
 * package's {@code Implementation-Version} manifest attribute populated by Maven at jar time. When
 * the class is loaded outside a packaged jar (e.g. during unit tests) the manifest attribute is
 * absent and we fall back to {@code "unknown"} — sufficient for a smoke test.
 */
@Command(
    name = "version",
    description = "Print the cached-fs build version.",
    mixinStandardHelpOptions = true)
public final class VersionCommand implements Callable<Integer> {

  @Override
  public Integer call() {
    return run(System.out);
  }

  int run(PrintStream out) {
    String v = VersionCommand.class.getPackage().getImplementationVersion();
    out.println("cached-fs " + (v == null ? "unknown" : v));
    return 0;
  }
}
