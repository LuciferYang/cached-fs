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
package io.github.luciferyang.cachedfs.cli.jmx;

import java.io.IOException;
import picocli.CommandLine.Option;

/**
 * Picocli mixin shared by every JMX-using subcommand: {@code --jmx-url} and {@code --object-name}.
 * Build a connected {@link JmxClient} via {@link #connect()} — switches between remote and local
 * mode based on whether {@code --jmx-url} was provided.
 */
public final class JmxOptions {

  @Option(
      names = {"--jmx-url"},
      description =
          "JMX service URL of the target JVM (e.g. service:jmx:rmi:///jndi/rmi://host:9999/jmxrmi)."
              + " Omit to use the local JVM's platform MBean server (test mode / in-process).")
  String jmxUrl;

  @Option(
      names = {"--object-name"},
      defaultValue = "io.github.luciferyang.cachedfs:type=CachedFsBootstrap",
      description = "JMX ObjectName of the cached-fs MBean. Default: ${DEFAULT-VALUE}")
  String objectName;

  /** Opens a JMX client per the resolved options. Callers must close. */
  public JmxClient connect() throws IOException {
    if (jmxUrl == null || jmxUrl.isBlank()) {
      return JmxClient.local(objectName);
    }
    return JmxClient.connect(jmxUrl, objectName);
  }
}
