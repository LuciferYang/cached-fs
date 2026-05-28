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

import io.github.luciferyang.cachedfs.hadoop.CacheBootstrap;
import java.util.Map;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskFailedReason;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.api.plugin.SparkPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark plugin that derives a per-task {@code fs.cached.scan-id} from {@link TaskContext} and wires
 * it into the cached-fs bootstrap's per-thread scope via {@link CacheBootstrap#withScanId(String)}.
 * Closes the scope on task end so the {@code ScanTracker} for that scanId is GC-eligible.
 *
 * <p><b>Why.</b> Without this, every Spark task on every executor collapses to the {@code
 * "default"} scanId, which means concurrent queries share one {@code ScanTracker} per JVM and
 * contaminate each other's density tracking. With it, every task attempt gets its own tracker keyed
 * by {@code task-{stageId}-{partitionId}-{taskAttemptId}} — partition-level isolation, retry-aware.
 *
 * <p><b>How to wire.</b> Set on the Spark driver:
 *
 * <pre>{@code
 * spark.plugins=io.github.luciferyang.cachedfs.spark.CachedFsScanIdPlugin
 * }</pre>
 *
 * Compose with other plugins as a comma-separated list.
 *
 * <p><b>Cross-thread caveat.</b> The cached-fs {@code withScanId} ThreadLocal is bound to the
 * task-runner thread; reads that escape to a separate executor (e.g. a {@code ForkJoinPool} or
 * async future inside the task) won't see the scope and fall back to {@code "default"}. This is the
 * same constraint Spark's {@code TaskContext.get()} has — if a future Spark release adds a
 * propagating context, the plugin will pick it up automatically.
 *
 * <p><b>Defensive degradation.</b> If {@link TaskContext#get()} returns null (the plugin fired
 * outside a task) or {@link CacheBootstrap#get()} returns empty (the bootstrap hasn't been
 * installed yet on this executor — typically the very first task before any FileSystem.get fires),
 * the hook silently no-ops. The fallback scanId resolution chain in {@code CachedFileSystem.open()}
 * picks up {@code conf.getTrimmed(SCAN_ID)} or {@code "default"} on that one task.
 */
public final class CachedFsScanIdPlugin implements SparkPlugin {

  @Override
  public DriverPlugin driverPlugin() {
    return new NoOpDriverPlugin();
  }

  @Override
  public ExecutorPlugin executorPlugin() {
    return new CachedFsScanIdExecutorPlugin();
  }

  /** Driver-side hook is a no-op — the scanId-per-task wiring is purely an executor concern. */
  static final class NoOpDriverPlugin implements DriverPlugin {
    @Override
    public Map<String, String> init(SparkContext sc, PluginContext ctx) {
      return Map.of();
    }
  }

  /**
   * Opens a {@link CacheBootstrap#withScanId(String)} scope at task start and closes it at task end
   * (success or failure). All three hooks fire on the same task-runner thread, so the AutoCloseable
   * stashed in a ThreadLocal is safely retrieved and closed by the same thread that opened it —
   * honoring {@code withScanId}'s single-thread close contract.
   */
  static final class CachedFsScanIdExecutorPlugin implements ExecutorPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(CachedFsScanIdExecutorPlugin.class);

    /**
     * Per-thread holder for the active scanId scope. ThreadLocal because Spark reuses task-runner
     * threads across tasks; we need each task's open/close pair to find its own AutoCloseable.
     */
    private static final ThreadLocal<AutoCloseable> ACTIVE_SCOPE = new ThreadLocal<>();

    /**
     * Test-only counter incremented on every successful {@code onTaskStart} (i.e., every task that
     * produced a {@code withScanId} scope). Used by ITs to verify per-task firing because the
     * trackers themselves are removed on task end and cannot be observed after the fact. Reset to
     * {@code 0} between tests via {@link #resetScopesOpenedForTesting()}. Production code MUST NOT
     * read this — there is no concurrency contract beyond the atomic counter itself.
     */
    private static final java.util.concurrent.atomic.AtomicLong SCOPES_OPENED_FOR_TESTING =
        new java.util.concurrent.atomic.AtomicLong();

    /** Test-only. Returns the cumulative count of scopes opened since the last reset. */
    public static long scopesOpenedForTesting() {
      return SCOPES_OPENED_FOR_TESTING.get();
    }

    /** Test-only. Resets the counter so a subsequent test observes a known starting value. */
    public static void resetScopesOpenedForTesting() {
      SCOPES_OPENED_FOR_TESTING.set(0L);
    }

    @Override
    public void onTaskStart() {
      TaskContext tc = TaskContext.get();
      if (tc == null) {
        return;
      }
      CacheBootstrap b = CacheBootstrap.get().orElse(null);
      if (b == null) {
        // Bootstrap not yet installed on this executor (very first task before any FileSystem.get
        // call). The fallback "default" scanId applies for this one task; subsequent tasks pick
        // up the bootstrap correctly.
        return;
      }
      String scanId = "task-" + tc.stageId() + "-" + tc.partitionId() + "-" + tc.taskAttemptId();
      try {
        AutoCloseable prev = ACTIVE_SCOPE.get();
        if (prev != null) {
          // Defensive: a prior onTaskStart didn't pair with onTaskSucceeded/Failed. Close it now
          // so the new scope replaces it cleanly. Stale-slot recovery inside withScanId will also
          // catch this and emit a WARN — that WARN is the audit signal.
          try {
            prev.close();
          } catch (Exception ignored) {
            // best-effort
          }
        }
        ACTIVE_SCOPE.set(b.withScanId(scanId));
        SCOPES_OPENED_FOR_TESTING.incrementAndGet();
      } catch (RuntimeException ex) {
        // Log once per cause-class and degrade silently — never let a plugin failure break the
        // task.
        LOG.warn(
            "CachedFsScanIdPlugin.onTaskStart failed for scanId={}; task will use the fallback "
                + "scanId resolution (\"default\")",
            scanId,
            ex);
      }
    }

    @Override
    public void onTaskSucceeded() {
      closeActiveScope();
    }

    @Override
    public void onTaskFailed(TaskFailedReason failureReason) {
      closeActiveScope();
    }

    private static void closeActiveScope() {
      AutoCloseable scope = ACTIVE_SCOPE.get();
      if (scope == null) {
        return;
      }
      ACTIVE_SCOPE.remove();
      try {
        scope.close();
      } catch (Exception ex) {
        // withScanId.close is best-effort by contract; log + swallow.
        LOG.warn("CachedFsScanIdPlugin scope close raised; ignoring", ex);
      }
    }
  }
}
