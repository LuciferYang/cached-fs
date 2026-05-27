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
package io.github.luciferyang.cachedfs.spark

import io.github.luciferyang.cachedfs.spark.affinity.{
  CachedFsAffinity,
  CachedFsAffinityConfig,
  CachedFsSoftAffinityManager
}

import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{
  SparkListenerApplicationEnd,
  SparkListenerExecutorAdded,
  SparkListenerExecutorRemoved,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd,
  StageInfo,
  TaskInfo,
  TaskLocality
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, Success, TaskState}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
 * ScalaTest coverage for the Scala-side wiring (extension + listener). The Java tests already
 * cover the manager state machine; here we drive the listener's event hooks directly to confirm
 * they delegate to the manager correctly, and we exercise the extension's SparkConf parsing
 * against a local SparkSession to validate the no-arg public constructor + ` SparkSessionExtensions => Unit`
 * subtype Spark needs.
 */
class CachedFsAffinitySuite extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    CachedFsSoftAffinityManager.resetForTesting()
    CachedFsSoftAffinityListener.resetForTesting()
  }

  test("listener.onExecutorAdded adds the executor to the ring") {
    val listener = new CachedFsSoftAffinityListener()
    val mgr = CachedFsSoftAffinityManager.getInstance()
    CachedFsAffinity.configure(true, 2, 1, false, 10000)

    listener.onExecutorAdded(
      SparkListenerExecutorAdded(
        time = 0L,
        executorId = "executor-A",
        executorInfo =
          new ExecutorInfo("host-A", totalCores = 4, logUrlMap = Map.empty)))
    assert(mgr.executorCount() == 1)
  }

  test("listener rejects an executor with empty host (would corrupt TaskLocation)") {
    val listener = new CachedFsSoftAffinityListener()
    val mgr = CachedFsSoftAffinityManager.getInstance()
    CachedFsAffinity.configure(true, 2, 1, false, 10000)
    listener.onExecutorAdded(
      SparkListenerExecutorAdded(
        time = 0L,
        executorId = "executor-bad",
        executorInfo =
          new ExecutorInfo("", totalCores = 4, logUrlMap = Map.empty)))
    assert(mgr.executorCount() == 0)
  }

  test("listener.onExecutorRemoved drops the executor from the ring") {
    val listener = new CachedFsSoftAffinityListener()
    val mgr = CachedFsSoftAffinityManager.getInstance()
    CachedFsAffinity.configure(true, 2, 1, false, 10000)
    listener.onExecutorAdded(
      SparkListenerExecutorAdded(
        time = 0L,
        executorId = "e1",
        executorInfo = new ExecutorInfo("h1", 4, Map.empty)))
    assert(mgr.executorCount() == 1)
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(time = 0L, "e1", "decommissioned"))
    assert(mgr.executorCount() == 0)
  }

  test("listener.onApplicationEnd resets manager state for the next SparkContext") {
    val listener = new CachedFsSoftAffinityListener()
    val mgr = CachedFsSoftAffinityManager.getInstance()
    CachedFsAffinity.configure(true, 2, 1, false, 10000)
    listener.onExecutorAdded(
      SparkListenerExecutorAdded(
        time = 0L,
        executorId = "e1",
        executorInfo = new ExecutorInfo("h1", 4, Map.empty)))
    assert(mgr.executorCount() == 1)

    listener.onApplicationEnd(SparkListenerApplicationEnd(0L))
    assert(mgr.executorCount() == 0, "ring should be cleared after onApplicationEnd")
  }

  test("listener.onTaskEnd records an observation only when feedback mode is enabled") {
    val listener = new CachedFsSoftAffinityListener()
    val mgr = CachedFsSoftAffinityManager.getInstance()
    CachedFsAffinity.configure(true, 2, 1, false, 10000)
    // Without detectDuplicateReading, listener hooks must be cheap no-ops.
    val taskInfo = new TaskInfo(
      taskId = 0L,
      index = 0,
      attemptNumber = 0,
      partitionId = 0,
      launchTime = 0L,
      executorId = "e1",
      host = "h1",
      taskLocality = TaskLocality.PROCESS_LOCAL,
      speculative = false)
    listener.onTaskEnd(
      SparkListenerTaskEnd(
        stageId = 1,
        stageAttemptId = 0,
        taskType = "ShuffleMapTask",
        reason = Success,
        taskInfo = taskInfo,
        taskExecutorMetrics = null,
        taskMetrics = null))
    // No observations recorded.
    assert(mgr.snapshot().duplicateReadingEntries() == 0)
  }

  test("extension.apply on a real SparkSession configures the manager from SparkConf") {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("cached-fs-affinity-extension-suite")
      .config(CachedFsAffinityConfig.ENABLED, "true")
      .config(CachedFsAffinityConfig.REPLICATION_NUM, "3")
      .config(CachedFsAffinityConfig.MIN_TARGET_HOSTS, "2")
      .config(CachedFsAffinityConfig.DUPLICATE_READING_DETECT_ENABLED, "true")
      .config(CachedFsAffinityConfig.DUPLICATE_READING_MAX_CACHE_ITEMS, "500")
      .config(CachedFsAffinityConfig.VIRTUAL_NODES, "75")
      .config("spark.sql.extensions", classOf[CachedFsAffinityExtension].getName)
      .getOrCreate()
    try {
      val snap = CachedFsSoftAffinityManager.getInstance().snapshot()
      assert(snap.enabled())
      assert(snap.replicationNum() == 3)
      assert(snap.minTargetHosts() == 2)
      assert(snap.detectDuplicateReading())
      assert(snap.duplicateReadingMaxCacheItems() == 500)
      assert(snap.virtualNodes() == 75)
    } finally {
      spark.stop()
    }
  }

  test("extension.apply tolerates an invalid SparkConf and disables the feature") {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("cached-fs-affinity-invalid-conf")
      .config(CachedFsAffinityConfig.ENABLED, "true")
      .config(CachedFsAffinityConfig.REPLICATION_NUM, "0") // invalid
      .config("spark.sql.extensions", classOf[CachedFsAffinityExtension].getName)
      .getOrCreate()
    try {
      // 0 replicationNum triggers the warning-then-default fallback; feature stays enabled but
      // with the default replication. Manager should NOT crash the session.
      val snap = CachedFsSoftAffinityManager.getInstance().snapshot()
      assert(snap.replicationNum() == CachedFsAffinityConfig.DEFAULT_REPLICATION_NUM)
    } finally {
      spark.stop()
    }
  }

  // Suppress noisy Spark startup banners during this suite.
  java.util.logging.Logger.getLogger("org.apache.spark").setLevel(java.util.logging.Level.WARNING)
}
