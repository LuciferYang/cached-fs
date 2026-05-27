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

import io.github.luciferyang.cachedfs.spark.affinity.{CachedFsAffinity, CachedFsSoftAffinityManager}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerApplicationEnd,
  SparkListenerExecutorAdded,
  SparkListenerExecutorRemoved,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd
}

import java.util.concurrent.atomic.AtomicReference
import scala.util.control.NonFatal

/**
 * SparkListener that feeds executor lifecycle + per-stage task-end events into
 * [[CachedFsSoftAffinityManager]]. Registered once per SparkContext via
 * [[CachedFsSoftAffinityListener.ensureRegistered]].
 *
 * Three responsibilities:
 *   1. Static-mode upkeep: executor add/remove → ring add/remove.
 *   2. Feedback-mode upkeep: stage submit/complete + task end → split-key observations.
 *   3. Per-SparkContext reset: on `SparkApplicationEnd`, drop ring + observation state so the
 *      next SparkContext in the same JVM (REPL / notebook / Spark Connect) doesn't inherit
 *      stale executor IDs.
 *
 * Feedback-mode hooks short-circuit cheaply when {@code detectDuplicateReading} is off, so we
 * register a single listener regardless of mode.
 */
class CachedFsSoftAffinityListener extends SparkListener with Logging {

  private def mgr: CachedFsSoftAffinityManager = CachedFsSoftAffinityManager.getInstance()

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    val host = event.executorInfo.executorHost
    // ExecutorInfo.executorHost is a non-null val per Spark's source, but be defensive against
    // future field renames; reject empty hosts at the manager boundary anyway.
    if (host == null || host.isEmpty) {
      logWarning(
        s"Ignoring executor ${event.executorId} with null/empty host — would corrupt " +
          "TaskLocation parsing")
      return
    }
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

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    // Per-SparkContext reset so a sequential SparkContext in the same JVM (REPL workflow,
    // SparkSession.builder().getOrCreate() after stop, Spark Connect, etc.) starts fresh
    // without inheriting stale executor IDs from the previous context.
    CachedFsAffinity.notifyApplicationEnd()
    CachedFsSoftAffinityListener.clearRegistered()
  }
}

object CachedFsSoftAffinityListener extends Logging {

  // Tracks the SparkContext we have a listener registered against, so a NEW SparkContext in the
  // same JVM triggers a fresh registration. Using AtomicReference rather than a plain @volatile
  // Boolean so the (compare-and-swap to the new context) operation is atomic.
  private val registeredCtx = new AtomicReference[SparkContext]()

  /**
   * Idempotent: registers exactly one listener per (JVM, SparkContext). A new SparkContext
   * registers a fresh listener.
   */
  def ensureRegistered(sc: SparkContext): Unit = {
    if (registeredCtx.get() eq sc) return
    if (registeredCtx.compareAndSet(null, sc) || registeredCtx.compareAndSet(other(sc), sc)) {
      try {
        // We do NOT seed the ring from the existing executor set: Spark's public API does not
        // expose an (id, host) tuple for already-running executors (statusStore is
        // package-private; statusTracker.getExecutorInfos lacks the id field). The listener
        // instead relies on incremental SparkListenerExecutorAdded events from registration time
        // forward. Spark's SparkSessionExtensions fires very early — before executors come up on
        // typical cluster deployments — so this is sufficient. Dynamic-allocation churn is
        // handled cleanly because each new executor fires its own onExecutorAdded.
        sc.addSparkListener(new CachedFsSoftAffinityListener())
        logInfo(s"CachedFsSoftAffinityListener registered on SparkContext ${sc.applicationId}")
      } catch {
        case NonFatal(t) =>
          // Roll back the CAS so a subsequent caller can retry.
          registeredCtx.set(null)
          logWarning(
            s"Failed to register CachedFsSoftAffinityListener on SparkContext ${sc.applicationId}",
            t)
      }
    }
  }

  /** Returns the captured-context reference IF different from sc — used by the second CAS branch. */
  private def other(sc: SparkContext): SparkContext = {
    val prior = registeredCtx.get()
    if (prior eq sc) sc else prior
  }

  /** Called by the listener instance on `SparkApplicationEnd` so the next ctx re-registers. */
  private[spark] def clearRegistered(): Unit = {
    registeredCtx.set(null)
  }

  /** Test-only. Clears the registered flag so a fresh SparkContext can re-register. */
  private[spark] def resetForTesting(): Unit = {
    registeredCtx.set(null)
  }
}
