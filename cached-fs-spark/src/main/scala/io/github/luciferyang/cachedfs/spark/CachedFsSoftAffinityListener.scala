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

import io.github.luciferyang.cachedfs.spark.affinity.CachedFsSoftAffinityManager

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerExecutorAdded,
  SparkListenerExecutorRemoved,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd
}

import scala.util.control.NonFatal

/**
 * SparkListener that feeds executor lifecycle + per-stage task-end events into
 * [[CachedFsSoftAffinityManager]]. Registered once per SparkContext via
 * [[CachedFsSoftAffinityListener.ensureRegistered]].
 *
 * Two responsibilities:
 *   1. Static-mode upkeep: executor add/remove → ring add/remove.
 *   2. Feedback-mode upkeep: stage submit/complete + task end → split-key observations.
 *
 * Feedback-mode hooks short-circuit cheaply when {@code detectDuplicateReading} is off, so we
 * register a single listener regardless of mode.
 */
class CachedFsSoftAffinityListener extends SparkListener with Logging {

  private def mgr: CachedFsSoftAffinityManager = CachedFsSoftAffinityManager.getInstance()

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    val host = Option(event.executorInfo.executorHost).getOrElse("")
    mgr.handleExecutorAdded(event.executorId, host)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    mgr.handleExecutorRemoved(event.executorId)
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    if (!mgr.isDetectDuplicateReading) return
    val rddIds = event.stageInfo.rddInfos.map(_.id).toArray
    mgr.updateStageSubmitted(event.stageInfo.stageId, rddIds)
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    if (!mgr.isDetectDuplicateReading) return
    val rddIds = event.stageInfo.rddInfos.map(_.id).toArray
    mgr.cleanMiddleStatusMap(event.stageInfo.stageId, rddIds)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    if (!mgr.isDetectDuplicateReading) return
    event.reason match {
      case org.apache.spark.Success =>
        val info = event.taskInfo
        mgr.updateTaskEnd(event.stageId, info.partitionId, info.executorId, info.host)
      case _ => // skip failed tasks; we only learn from successful placements
    }
  }
}

object CachedFsSoftAffinityListener extends Logging {

  @volatile private var registered: Boolean = false

  /**
   * Idempotent: registers exactly one listener per JVM. Also seeds the ring from the current
   * executor set so a listener attached AFTER executors came up still has a populated ring.
   */
  def ensureRegistered(sc: SparkContext): Unit = {
    if (registered) return
    synchronized {
      if (registered) return
      try {
        // We do NOT seed the ring from sc's existing executor set: Spark's public API does not
        // expose an (id, host) tuple for already-running executors (statusStore is
        // package-private under org.apache.spark; statusTracker.getExecutorInfos lacks the id
        // field). The listener instead relies on incremental SparkListenerExecutorAdded events
        // from registration time forward. Register the extension early — ideally in
        // spark.sql.extensions at session start — so executors come up AFTER the listener is in
        // place. Dynamic-allocation churn is handled cleanly because each new executor fires its
        // own onExecutorAdded.
        sc.addSparkListener(new CachedFsSoftAffinityListener())
        registered = true
        logInfo("CachedFsSoftAffinityListener registered")
      } catch {
        case NonFatal(t) =>
          logWarning(s"Failed to register CachedFsSoftAffinityListener: ${t.getMessage}")
      }
    }
  }

  private def extractHost(hostPort: String): String = {
    if (hostPort == null || hostPort.isEmpty) "unknown"
    else {
      val lastColon = hostPort.lastIndexOf(':')
      if (lastColon <= 0) hostPort else hostPort.substring(0, lastColon)
    }
  }

  /** Test-only. Clears the registered flag so a fresh SparkContext can re-register. */
  private[spark] def resetForTesting(): Unit = synchronized {
    registered = false
  }
}
