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

import io.github.luciferyang.cachedfs.hadoop.CachedFsConfig;
import java.io.File;
import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * {@code cached-fs config} — load Hadoop XML config files (and optional inline {@code -D key=value}
 * pairs), apply the {@code fs.cached.*} resolvers from {@link CachedFsConfig}, and dump the
 * effective values with defaults filled in. Catches range/type errors that would otherwise blow up
 * the JVM at {@code CacheBootstrap.installIfNeeded} time.
 *
 * <p>Exit codes: {@code 0} on success; {@code 2} when one of the {@link CachedFsConfig} resolvers
 * throws {@link IllegalArgumentException} (the offending key is named in the error message).
 *
 * <p>Output is plain key=value lines, alphabetized by key, one per line. Sized for grep-and-pipe
 * use ("does this conf set prefetch.threads to 0?") rather than human reading.
 */
@Command(
    name = "config",
    description =
        "Load Hadoop config XMLs + inline overrides, validate fs.cached.* keys, dump effective"
            + " values.",
    mixinStandardHelpOptions = true)
public final class ConfigCommand implements Callable<Integer> {

  @Option(
      names = {"-c", "--conf"},
      description =
          "Hadoop config XML file(s) to load, in precedence order (later wins on conflict). Repeat"
              + " the flag for each file.")
  List<File> confFiles = List.of();

  @Option(
      names = {"-D"},
      description =
          "Inline key=value override(s), applied AFTER --conf files. Repeat the flag for each pair.")
  Map<String, String> overrides = Map.of();

  @Override
  public Integer call() {
    return run(System.out, System.err);
  }

  /** Test-visible entry point: returns the exit code without calling {@code System.exit}. */
  int run(PrintStream out, PrintStream err) {
    Configuration conf = new Configuration(/* loadDefaults= */ false);
    for (File f : confFiles) {
      if (!f.isFile()) {
        err.println("config file not found: " + f);
        return 2;
      }
      conf.addResource(new org.apache.hadoop.fs.Path(f.toURI()));
    }
    for (Map.Entry<String, String> e : overrides.entrySet()) {
      conf.set(e.getKey(), e.getValue());
    }
    try {
      Map<String, String> effective = resolveEffective(conf);
      effective.forEach((k, v) -> out.println(k + "=" + v));
      return 0;
    } catch (IllegalArgumentException ex) {
      err.println("invalid cached-fs config: " + ex.getMessage());
      return 2;
    }
  }

  /**
   * Calls every {@link CachedFsConfig} resolver and returns the effective values as a sorted map.
   * Resolvers that throw on bad values fail-fast here, surfacing the offending key BEFORE the JVM
   * attempts to install the cache.
   */
  private static Map<String, String> resolveEffective(Configuration conf) {
    Map<String, String> m = new LinkedHashMap<>();
    m.put(CachedFsConfig.ENABLED, String.valueOf(CachedFsConfig.isEnabled(conf)));
    m.put(CachedFsConfig.INNER_IMPL, String.valueOf(conf.get(CachedFsConfig.INNER_IMPL)));
    m.put(CachedFsConfig.LOAD_QUANTUM_BYTES, String.valueOf(CachedFsConfig.loadQuantumBytes(conf)));
    m.put(
        CachedFsConfig.SSD_CHECKSUM_ENABLED,
        String.valueOf(
            conf.getBoolean(
                CachedFsConfig.SSD_CHECKSUM_ENABLED, CachedFsConfig.DEFAULT_SSD_CHECKSUM_ENABLED)));
    m.put(
        CachedFsConfig.SCAN_TRACKER_ENABLED,
        String.valueOf(CachedFsConfig.scanTrackerEnabled(conf)));
    m.put(
        CachedFsConfig.SCAN_TRACKER_MAX_ENTRIES_PER_TRACKER,
        String.valueOf(CachedFsConfig.scanTrackerMaxEntriesPerTracker(conf)));
    m.put(
        CachedFsConfig.METRICS_ENABLED,
        String.valueOf(
            conf.getBoolean(
                CachedFsConfig.METRICS_ENABLED, CachedFsConfig.DEFAULT_METRICS_ENABLED)));
    m.put(
        CachedFsConfig.COALESCE_ENABLED,
        String.valueOf(
            conf.getBoolean(
                CachedFsConfig.COALESCE_ENABLED, CachedFsConfig.DEFAULT_COALESCE_ENABLED)));
    m.put(CachedFsConfig.PREFETCH_ENABLED, String.valueOf(CachedFsConfig.prefetchEnabled(conf)));
    return new java.util.TreeMap<>(m);
  }
}
