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
package io.github.luciferyang.cachedfs.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.luciferyang.cachedfs.spark.CachedFsScanIdPlugin.CachedFsScanIdExecutorPlugin;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CachedFsScanIdPluginTest {

  @Test
  @DisplayName("plugin returns non-null driver and executor plugin instances")
  void wiresBothPlugins() {
    CachedFsScanIdPlugin plugin = new CachedFsScanIdPlugin();
    DriverPlugin driver = plugin.driverPlugin();
    ExecutorPlugin executor = plugin.executorPlugin();
    assertThat(driver).isNotNull();
    assertThat(executor).isInstanceOf(CachedFsScanIdExecutorPlugin.class);
  }

  @Test
  @DisplayName(
      "onTaskStart degrades silently when no TaskContext is active — the unit test JVM has none")
  void onTaskStartNoTaskContext() {
    // Outside any real SparkContext / TaskContext, TaskContext.get() returns null. The plugin
    // MUST not throw — it should silently no-op and leave the cached-fs scanId resolution chain
    // to fall back to "default". A throw here would break every task on an executor that loaded
    // the plugin but encountered a non-task callsite (e.g. plugin-init test paths).
    ExecutorPlugin executor = new CachedFsScanIdPlugin().executorPlugin();
    executor.onTaskStart();
    executor.onTaskSucceeded(); // idempotent / safe even when no scope was opened
  }

  @Test
  @DisplayName("driver plugin init returns an empty conf map (no driver-side config injection)")
  void driverPluginInitReturnsEmptyMap() {
    DriverPlugin driver = new CachedFsScanIdPlugin().driverPlugin();
    // We pass nulls — the no-op driver init never reads either argument. This is a sanity check
    // that the contract returns an immutable empty map rather than null (Spark dereferences it).
    assertThat(driver.init(null, null)).isEmpty();
  }
}
