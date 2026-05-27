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
  CachedFsAffinityBlockLocationsProvider,
  CachedFsAffinityConfig
}

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

/**
 * Entry point for `spark.sql.extensions=io.github.luciferyang.cachedfs.spark.CachedFsAffinityExtension`.
 *
 * On apply this:
 *   1. Initializes the affinity manager singleton with the configured ring density (virtual-nodes).
 *      MUST happen first so the singleton materializes with the right density before any other
 *      code touches it.
 *   2. Applies the SparkConf-driven knobs to the manager via [[CachedFsAffinity.configure]]. The
 *      `enabled` flag is set LAST so a partially-configured manager can never be observed as on.
 *   3. Registers the [[CachedFsSoftAffinityListener]] on the active SparkContext to track executor
 *      lifecycle + (when feedback mode is on) per-stage task-end events.
 *   4. Stages the [[CachedFsAffinityBlockLocationsProvider]] onto the cached-fs bootstrap so
 *      `CachedFileSystem.getFileBlockLocations` (and the listLocatedStatus / listFiles overrides)
 *      surface affinity-aware preferred locations to Spark's planner.
 *
 * Invalid SparkConf values are caught and logged WITHOUT failing session startup — the affinity
 * feature defaults to disabled, so a misconfigured key should not take down the whole driver.
 */
class CachedFsAffinityExtension extends (SparkSessionExtensions => Unit) with Logging {

  override def apply(ext: SparkSessionExtensions): Unit = {
    // We can't use SparkContext.getActive (package-private to org.apache.spark) and the
    // SparkSession itself isn't yet constructed when apply() fires — Spark's
    // SparkSession.applyExtensions runs BEFORE the session is registered as active. The safe
    // alternative: SparkEnv.get returns the active SparkEnv iff a SparkContext is initialized;
    // when it's non-null, SparkContext.getOrCreate() is guaranteed to return the existing
    // context rather than spawn a default one. Bail out (without creating any context) when no
    // SparkEnv exists — that's the non-standard caller case (unit test instantiating the
    // extension class directly).
    if (SparkEnv.get == null) {
      logWarning(
        "CachedFsAffinityExtension.apply invoked without an initialized SparkContext — " +
          "skipping. If you see this in production, your extension wiring is non-standard.")
      return
    }
    val sc: SparkContext = SparkContext.getOrCreate()

    // Step 1: ring density must be locked in BEFORE the singleton materializes from any other
    // code path. initializeWithVirtualNodes is idempotent and logs a WARN if the manager was
    // already constructed with a different value.
    val virtualNodes = readPositiveInt(
      sc,
      CachedFsAffinityConfig.VIRTUAL_NODES,
      CachedFsAffinityConfig.DEFAULT_VIRTUAL_NODES)
    try {
      CachedFsAffinity.initializeWithVirtualNodes(virtualNodes)
    } catch {
      case ex: IllegalArgumentException =>
        logError(
          s"Invalid ${CachedFsAffinityConfig.VIRTUAL_NODES}: $virtualNodes (${ex.getMessage}). " +
            "Affinity feature is disabled for this session.",
          ex)
        return
    }

    // Step 2: apply knobs. CachedFsAffinity.configure validates inputs and rethrows with messages
    // that name the offending SparkConf key. We catch + log + disable on failure rather than
    // throwing up into the SparkSession startup.
    val enabled = readBooleanSafe(
      sc,
      CachedFsAffinityConfig.ENABLED,
      CachedFsAffinityConfig.DEFAULT_ENABLED)
    val replicationNum = readPositiveInt(
      sc,
      CachedFsAffinityConfig.REPLICATION_NUM,
      CachedFsAffinityConfig.DEFAULT_REPLICATION_NUM)
    val minTargetHosts = readNonNegativeInt(
      sc,
      CachedFsAffinityConfig.MIN_TARGET_HOSTS,
      CachedFsAffinityConfig.DEFAULT_MIN_TARGET_HOSTS)
    val detectDuplicateReading = readBooleanSafe(
      sc,
      CachedFsAffinityConfig.DUPLICATE_READING_DETECT_ENABLED,
      CachedFsAffinityConfig.DEFAULT_DUPLICATE_READING_DETECT_ENABLED)
    val duplicateReadingMaxCacheItems = readPositiveInt(
      sc,
      CachedFsAffinityConfig.DUPLICATE_READING_MAX_CACHE_ITEMS,
      CachedFsAffinityConfig.DEFAULT_DUPLICATE_READING_MAX_CACHE_ITEMS)
    try {
      CachedFsAffinity.configure(
        enabled,
        replicationNum,
        minTargetHosts,
        detectDuplicateReading,
        duplicateReadingMaxCacheItems)
    } catch {
      case ex: IllegalArgumentException =>
        logError(
          s"Invalid SparkConf for cached-fs affinity (${ex.getMessage}). Feature disabled.",
          ex)
        return
    }

    // Step 3: register the listener AFTER the manager is fully configured. Order matters because
    // SparkListenerExecutorAdded events arriving between addSparkListener and the manager being
    // enabled would be recorded in the ring with enabled=false — harmless but wasteful. The
    // listener is per-SparkContext; sequential SparkSessions sharing one SparkContext share the
    // same listener (idempotent on identity).
    CachedFsSoftAffinityListener.ensureRegistered(sc)

    // Step 4: stage the block-locations provider. stageBlockLocationsProvider takes the bootstrap
    // LOCK so this is race-free against a concurrent installIfNeeded (Hadoop-side fix). The
    // provider singleton is recreated per apply() but installs idempotently into the bootstrap.
    CacheBootstrap.stageBlockLocationsProvider(new CachedFsAffinityBlockLocationsProvider())

    logInfo(
      s"CachedFsAffinityExtension applied to SparkContext ${sc.applicationId}: enabled=$enabled, " +
        s"replicationNum=$replicationNum, minTargetHosts=$minTargetHosts, " +
        s"detectDuplicateReading=$detectDuplicateReading, virtualNodes=$virtualNodes")
  }

  // SparkConf.getInt throws NumberFormatException, getBoolean throws IllegalArgumentException
  // for unparseable values. Spark 4's applyExtensions only catches CCE|CNFE|NCDFE, so an
  // unwrapped parse failure kills the whole SparkSession build. These helpers swallow the
  // parse failure, log a clear WARN naming the key, and fall back to the default — keeping the
  // contract that a misconfigured cached-fs knob never takes down the driver.
  private def readPositiveInt(sc: SparkContext, key: String, default: Int): Int = {
    val raw = readIntSafe(sc, key, default)
    if (raw <= 0) {
      logWarning(s"$key=$raw is not positive; falling back to default $default")
      default
    } else raw
  }

  private def readNonNegativeInt(sc: SparkContext, key: String, default: Int): Int = {
    val raw = readIntSafe(sc, key, default)
    if (raw < 0) {
      logWarning(s"$key=$raw is negative; falling back to default $default")
      default
    } else raw
  }

  private def readIntSafe(sc: SparkContext, key: String, default: Int): Int = {
    try sc.getConf.getInt(key, default)
    catch {
      case ex: NumberFormatException =>
        logWarning(
          s"$key has a non-integer SparkConf value (${ex.getMessage}); falling back to default $default")
        default
      case ex: IllegalArgumentException =>
        logWarning(
          s"$key has an invalid SparkConf value (${ex.getMessage}); falling back to default $default")
        default
    }
  }

  private def readBooleanSafe(sc: SparkContext, key: String, default: Boolean): Boolean = {
    try sc.getConf.getBoolean(key, default)
    catch {
      case ex: IllegalArgumentException =>
        logWarning(
          s"$key has a non-boolean SparkConf value (${ex.getMessage}); falling back to default $default")
        default
    }
  }
}
