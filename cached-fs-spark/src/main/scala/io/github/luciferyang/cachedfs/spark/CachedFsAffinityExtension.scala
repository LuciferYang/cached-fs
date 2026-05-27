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
  CachedFsAffinityBlockLocationsProvider,
  CachedFsAffinityConfig,
  CachedFsSoftAffinityManager
}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

/**
 * Entry point for {@code spark.sql.extensions=io.github.luciferyang.cachedfs.spark.CachedFsAffinityExtension}.
 *
 * On apply this:
 *   - Reads the cached-fs.affinity.* SparkConf keys and configures
 *     [[CachedFsSoftAffinityManager]]'s singleton with them.
 *   - Registers a [[CachedFsSoftAffinityListener]] on the active SparkContext to track executor
 *     lifecycle + (when feedback mode is on) per-stage task-end events.
 *
 * Future work: also inject a Catalyst rule that overrides {@code FilePartition.preferredLocations}
 * for the V1 FileScan path. For now the per-file hint flows through the FileSystem decorator's
 * {@code getFileBlockLocations} override, which Spark consults to compute preferred locations on
 * the driver during planning.
 */
class CachedFsAffinityExtension extends (SparkSessionExtensions => Unit) with Logging {

  override def apply(ext: SparkSessionExtensions): Unit = {
    val sc = SparkContext.getOrCreate()
    configureFromConf(sc)
    CachedFsSoftAffinityListener.ensureRegistered(sc)
    installBlockLocationsProvider()
    logInfo(
      "CachedFsAffinityExtension applied (manager configured, listener registered," +
        " block-locations provider installed)")
  }

  /**
   * Wires the affinity-aware {@link CachedFsAffinityBlockLocationsProvider} into the cached-fs
   * bootstrap, but only if it is already installed. The bootstrap installs on the first
   * {@code CachedFileSystem.initialize} call, which may not have happened yet when the Spark
   * extension fires; in that case the provider is installed lazily via {@link
   * CacheBootstrap#setBlockLocationsProvider} on the next bootstrap install (see follow-up).
   *
   * <p>Idempotent: re-applying the extension over an already-installed provider is a no-op
   * (instance is replaced with an equivalent one).
   */
  private[spark] def installBlockLocationsProvider(): Unit = {
    // stageBlockLocationsProvider handles both cases: bootstrap already installed → install
    // directly on the live instance; bootstrap not yet installed → stage so the next
    // installIfNeeded call picks it up.
    CacheBootstrap.stageBlockLocationsProvider(new CachedFsAffinityBlockLocationsProvider())
  }

  /**
   * Pulls the cached-fs.affinity.* SparkConf keys onto the manager. Called from {@link #apply} but
   * exposed package-private so a unit test can drive it without spinning a full SparkSession.
   */
  private[spark] def configureFromConf(sc: SparkContext): Unit = {
    val conf = sc.getConf
    val mgr = CachedFsSoftAffinityManager.getInstance()

    mgr.setEnabled(
      conf.getBoolean(CachedFsAffinityConfig.ENABLED, CachedFsAffinityConfig.DEFAULT_ENABLED))
    mgr.setReplicationNum(
      conf.getInt(
        CachedFsAffinityConfig.REPLICATION_NUM,
        CachedFsAffinityConfig.DEFAULT_REPLICATION_NUM))
    mgr.setMinTargetHosts(
      conf.getInt(
        CachedFsAffinityConfig.MIN_TARGET_HOSTS,
        CachedFsAffinityConfig.DEFAULT_MIN_TARGET_HOSTS))
    mgr.setDetectDuplicateReading(
      conf.getBoolean(
        CachedFsAffinityConfig.DUPLICATE_READING_DETECT_ENABLED,
        CachedFsAffinityConfig.DEFAULT_DUPLICATE_READING_DETECT_ENABLED))
    mgr.setDuplicateReadingMaxCacheItems(
      conf.getInt(
        CachedFsAffinityConfig.DUPLICATE_READING_MAX_CACHE_ITEMS,
        CachedFsAffinityConfig.DEFAULT_DUPLICATE_READING_MAX_CACHE_ITEMS))
  }
}
