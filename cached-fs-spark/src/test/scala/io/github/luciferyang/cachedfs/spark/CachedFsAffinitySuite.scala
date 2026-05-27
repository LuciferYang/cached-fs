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

import io.github.luciferyang.cachedfs.hadoop.CacheBootstrap
import io.github.luciferyang.cachedfs.spark.affinity.{
  CachedFsAffinity,
  CachedFsAffinityConfig,
  CachedFsSoftAffinityManager
}

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{
  SparkListenerApplicationEnd,
  SparkListenerExecutorAdded,
  SparkListenerExecutorRemoved
}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
 * ScalaTest coverage for the Scala-side wiring (extension + listener). The Java tests already
 * cover the manager state machine; here we drive the listener's event hooks directly to confirm
 * they delegate through CachedFsAffinity to the manager, and we exercise the extension's
 * SparkConf parsing against a local SparkSession to validate the no-arg public constructor +
 * `SparkSessionExtensions => Unit` subtype Spark needs.
 */
class CachedFsAffinitySuite extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    CachedFsSoftAffinityManager.resetForTesting()
    CachedFsSoftAffinityListener.resetForTesting()
    CacheBootstrap.uninstallForTesting()
  }

  override def afterEach(): Unit = {
    CacheBootstrap.uninstallForTesting()
  }

  /**
   * Builds a SparkContext for testing listener event delivery. local[1] is sufficient because we
   * dispatch events directly to the listener — we don't rely on Spark generating them.
   */
  private def withSparkContext(testBody: SparkContext => Unit): Unit = {
    val sc = new SparkContext("local[1]", "cached-fs-affinity-suite")
    try testBody(sc)
    finally sc.stop()
  }

  test("listener.onExecutorAdded adds the executor to the ring") {
    withSparkContext { sc =>
      val listener = new CachedFsSoftAffinityListener(sc)
      CachedFsAffinity.configure(true, 2, 1, false, 10000)
      val mgr = CachedFsSoftAffinityManager.getInstance()
      listener.onExecutorAdded(
        SparkListenerExecutorAdded(
          time = 0L,
          executorId = "executor-A",
          executorInfo = new ExecutorInfo("host-A", totalCores = 4, logUrlMap = Map.empty)))
      assert(mgr.executorCount() == 1)
    }
  }

  test("listener rejects an executor with empty host (would corrupt TaskLocation)") {
    withSparkContext { sc =>
      val listener = new CachedFsSoftAffinityListener(sc)
      CachedFsAffinity.configure(true, 2, 1, false, 10000)
      val mgr = CachedFsSoftAffinityManager.getInstance()
      listener.onExecutorAdded(
        SparkListenerExecutorAdded(
          time = 0L,
          executorId = "executor-bad",
          executorInfo = new ExecutorInfo("", totalCores = 4, logUrlMap = Map.empty)))
      assert(mgr.executorCount() == 0)
    }
  }

  test("listener.onExecutorRemoved drops the executor from the ring") {
    withSparkContext { sc =>
      val listener = new CachedFsSoftAffinityListener(sc)
      CachedFsAffinity.configure(true, 2, 1, false, 10000)
      val mgr = CachedFsSoftAffinityManager.getInstance()
      listener.onExecutorAdded(
        SparkListenerExecutorAdded(
          time = 0L,
          executorId = "e1",
          executorInfo = new ExecutorInfo("h1", 4, Map.empty)))
      assert(mgr.executorCount() == 1)
      listener.onExecutorRemoved(SparkListenerExecutorRemoved(time = 0L, "e1", "decommissioned"))
      assert(mgr.executorCount() == 0)
    }
  }

  test(
    "listener.onApplicationEnd from a non-registered listener does NOT clear shared state") {
    withSparkContext { sc =>
      // Register a listener via ensureRegistered so the registry slot is occupied.
      CachedFsSoftAffinityListener.ensureRegistered(sc)
      CachedFsAffinity.configure(true, 2, 1, false, 10000)
      val mgr = CachedFsSoftAffinityManager.getInstance()
      CachedFsAffinity.onExecutorAdded("e1", "h1")
      assert(mgr.executorCount() == 1)

      // A stranger listener (never registered) fires onApplicationEnd → must NOT wipe state
      // because clearRegistered's identity check fails.
      val stranger = new CachedFsSoftAffinityListener(sc)
      stranger.onApplicationEnd(SparkListenerApplicationEnd(0L))
      assert(
        mgr.executorCount() == 1,
        "non-registered listener's onApplicationEnd must not clear shared state")
    }
  }

  test(
    "listener.onApplicationEnd from the REGISTERED listener does wipe manager state " +
      "(integration via ensureRegistered)") {
    val sc = new SparkContext("local[1]", "cached-fs-affinity-suite-app-end")
    try {
      CachedFsSoftAffinityListener.ensureRegistered(sc)
      CachedFsAffinity.configure(true, 2, 1, false, 10000)
      val mgr = CachedFsSoftAffinityManager.getInstance()
      CachedFsAffinity.onExecutorAdded("e1", "h1")
      assert(mgr.executorCount() == 1)

      // sc.stop fires SparkApplicationEnd → registered listener triggers reset. Poll on the
      // observable side-effect (executor count) rather than Thread.sleep'ing on a fixed
      // wall-clock interval that's flaky on a busy CI runner. 10 s is generous for a local-mode
      // listener-bus drain and only consumes wall time if the side-effect didn't happen.
      sc.stop()
      val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10)
      while (mgr.executorCount() != 0 && System.nanoTime() < deadline) {
        Thread.sleep(20)
      }
      assert(mgr.executorCount() == 0)
    } finally {
      if (!sc.isStopped) sc.stop()
    }
  }

  test("two sequential SparkContexts in the same JVM: listener swaps cleanly") {
    val sc1 = new SparkContext("local[1]", "cached-fs-affinity-suite-ctx1")
    try {
      CachedFsSoftAffinityListener.ensureRegistered(sc1)
      // Registration slot must point at sc1's listener.
      val reg1 = CachedFsSoftAffinityListener.currentRegistrationForTesting()
      assert(reg1.isDefined && (reg1.get._1 eq sc1))
      CachedFsAffinity.configure(true, 2, 1, false, 10000)
      CachedFsAffinity.onExecutorAdded("e1", "h1")
      assert(CachedFsSoftAffinityManager.getInstance().executorCount() == 1)
      sc1.stop()
    } finally {
      if (!sc1.isStopped) sc1.stop()
    }
    // Wait for listener bus to drain (sc1.stop → onApplicationEnd → state wipe + clearRegistered).
    val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10)
    while (CachedFsSoftAffinityManager.getInstance().executorCount() != 0
      && System.nanoTime() < deadline) Thread.sleep(20)
    assert(CachedFsSoftAffinityManager.getInstance().executorCount() == 0)

    // sc2 in the same JVM — ensureRegistered must swap to a fresh listener.
    val sc2 = new SparkContext("local[1]", "cached-fs-affinity-suite-ctx2")
    try {
      CachedFsSoftAffinityListener.ensureRegistered(sc2)
      // Registration slot must now point at sc2's listener — proves the swap took effect, not
      // a coincidence of state being cleared by sc1.stop.
      val reg2 = CachedFsSoftAffinityListener.currentRegistrationForTesting()
      assert(reg2.isDefined && (reg2.get._1 eq sc2))
      CachedFsAffinity.configure(true, 2, 1, false, 10000)
      CachedFsAffinity.onExecutorAdded("e2", "h2")
      assert(CachedFsSoftAffinityManager.getInstance().executorCount() == 1)
    } finally {
      sc2.stop()
    }
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

  test(
    "extension.apply silently falls back to defaults when a SparkConf integer is non-positive; " +
      "session startup is not crashed") {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("cached-fs-affinity-invalid-conf")
      .config(CachedFsAffinityConfig.ENABLED, "true")
      .config(CachedFsAffinityConfig.REPLICATION_NUM, "0") // invalid — extension warns + defaults
      .config("spark.sql.extensions", classOf[CachedFsAffinityExtension].getName)
      .getOrCreate()
    try {
      val snap = CachedFsSoftAffinityManager.getInstance().snapshot()
      // The extension's readPositiveInt pre-sanitizes 0 → DEFAULT_REPLICATION_NUM, so configure()
      // succeeds and enabled remains true. This documents the actual behavior (warn + default,
      // NOT disable-on-failure) so an operator can rely on the fallback semantics.
      assert(snap.replicationNum() == CachedFsAffinityConfig.DEFAULT_REPLICATION_NUM)
      assert(snap.enabled(), "feature stays enabled with default replication-num")
    } finally {
      spark.stop()
    }
  }

  // Suppress noisy Spark startup banners during this suite.
  java.util.logging.Logger.getLogger("org.apache.spark").setLevel(java.util.logging.Level.WARNING)
}
