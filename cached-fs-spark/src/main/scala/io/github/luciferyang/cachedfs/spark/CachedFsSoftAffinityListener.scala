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

import io.github.luciferyang.cachedfs.spark.affinity.CachedFsAffinity

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerApplicationEnd,
  SparkListenerExecutorAdded,
  SparkListenerExecutorExcluded,
  SparkListenerExecutorRemoved,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd
}

import java.util.concurrent.atomic.AtomicReference
import scala.util.control.NonFatal

/**
 * SparkListener that feeds executor lifecycle + per-stage task-end events into
 * [[CachedFsSoftAffinityManager]]. Constructed with the SparkContext it was registered against so
 * its event hooks can filter on identity — preventing cross-context state corruption when
 * multiple SparkContexts run sequentially in the same JVM (Spark Connect, REPL).
 */
class CachedFsSoftAffinityListener(private val ownerCtx: SparkContext)
    extends SparkListener
    with Logging {

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    val host = event.executorInfo.executorHost
    if (host == null || host.isEmpty) {
      logWarning(
        s"Ignoring executor ${event.executorId} with null/empty host — would corrupt " +
          "TaskLocation parsing")
      return
    }
    CachedFsAffinity.onExecutorAdded(event.executorId, host)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    CachedFsAffinity.onExecutorRemoved(event.executorId)
  }

  override def onExecutorExcluded(event: SparkListenerExecutorExcluded): Unit = {
    // Spark's HealthTracker fires this when an executor accumulates too many failures and is
    // taken out of the placement pool. The executor may still be alive (depending on
    // spark.excludeOnFailure.killExcludedExecutors), but PROCESS_LOCAL hints pointing at it will
    // be silently downgraded by the DAGScheduler — so drop it from the ring proactively.
    CachedFsAffinity.onExecutorRemoved(event.executorId)
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val rddIds = event.stageInfo.rddInfos.map(_.id).toArray
    CachedFsAffinity.onStageSubmitted(event.stageInfo.stageId, rddIds)
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val rddIds = event.stageInfo.rddInfos.map(_.id).toArray
    CachedFsAffinity.onStageCompleted(event.stageInfo.stageId, rddIds)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    event.reason match {
      case org.apache.spark.Success =>
        val info = event.taskInfo
        CachedFsAffinity.onTaskEnd(event.stageId, info.partitionId, info.executorId, info.host)
      case _ =>
      // Skip non-Success reasons (Resubmitted, FetchFailed, ExceptionFailure, …). For
      // Resubmitted the original task succeeded but the executor was lost — recording the
      // placement would point future work at a dead executor; the onExecutorRemoved hook
      // scrubs that executor from the ring anyway.
    }
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    // Only the currently-registered (ctx, listener) tuple may wipe manager state. A
    // never-registered hand-built listener firing onApplicationEnd (test fixture) and an OLD
    // listener whose ctx has been superseded both observe `clearRegistered` returning false and
    // exit without touching shared state. Owner-identity is captured BOTH ways: clearRegistered
    // does the atomic identity CAS, and only if it succeeded do we proceed to wipe.
    val wasOwner = CachedFsSoftAffinityListener.clearRegistered(ownerCtx, this)
    if (wasOwner) {
      CachedFsAffinity.notifyApplicationEnd()
    }
  }
}

object CachedFsSoftAffinityListener extends Logging {

  /** (SparkContext, registered-listener-instance) pair currently owning the singleton state. */
  private val registration = new AtomicReference[Registration]()

  private final case class Registration(ctx: SparkContext, listener: CachedFsSoftAffinityListener)

  /**
   * Registers a listener iff none is registered for {@code sc} yet. Returns true when this call
   * installed the listener, false when a prior call already owned the slot. A previously-owned
   * context (different sc) is REPLACED — the old listener is removed from its (presumed-stopped)
   * SparkContext to avoid a stale handler firing into the manager.
   */
  def ensureRegistered(sc: SparkContext): Unit = {
    if (sc == null) return
    while (true) {
      val prior = registration.get()
      if (prior != null && (prior.ctx eq sc)) return // already registered for this ctx
      val listener = new CachedFsSoftAffinityListener(sc)
      val next = Registration(sc, listener)
      if (registration.compareAndSet(prior, next)) {
        // ADD the new listener FIRST. If addSparkListener throws (e.g. listenerBus stopped),
        // we have NOT touched the prior context yet — rollback restores prior intact. Only after
        // a successful add do we remove the prior listener from its (possibly still-live) ctx.
        try {
          sc.addSparkListener(listener)
        } catch {
          case NonFatal(t) =>
            // CAS-roll back ONLY if we are still the registered party — never clobber a
            // concurrent registrant that won the race after our successful CAS.
            registration.compareAndSet(next, prior)
            logWarning(
              s"Failed to register CachedFsSoftAffinityListener on SparkContext " +
                s"${sc.applicationId}",
              t)
            return
        }
        // New listener installed; safe to detach prior. Failure here is best-effort — the new
        // registration is already valid and the prior ctx is presumed stopped (or the prior
        // listener will get a harmless onApplicationEnd later that no-ops via the identity
        // guard).
        if (prior != null) {
          val priorAppId =
            try prior.ctx.applicationId
            catch { case NonFatal(_) => "<stopped-ctx>" }
          try prior.ctx.removeSparkListener(prior.listener)
          catch {
            case NonFatal(t) =>
              logWarning(
                s"Failed to remove prior listener from SparkContext $priorAppId (best-effort)",
                t)
          }
        }
        logInfo(s"CachedFsSoftAffinityListener registered on SparkContext ${sc.applicationId}")
        return
      }
      // CAS lost — another thread registered something; loop and re-check.
    }
  }

  /**
   * Clear the registration iff the passed (ctx, listener) tuple is still current. Returns true
   * when the caller WAS the registered owner (and the slot has been atomically cleared). Returns
   * false when a different registration is current or when no registration is present — caller
   * must NOT wipe shared state in that case.
   */
  private[spark] def clearRegistered(
      ctx: SparkContext,
      listener: CachedFsSoftAffinityListener): Boolean = {
    val prior = registration.get()
    if (prior != null && (prior.ctx eq ctx) && (prior.listener eq listener)) {
      registration.compareAndSet(prior, null)
    } else {
      false
    }
  }

  /** Test-only. Clears the registration so a fresh SparkContext can re-register. */
  private[spark] def resetForTesting(): Unit = {
    val r = registration.getAndSet(null)
    if (r != null) {
      try r.ctx.removeSparkListener(r.listener)
      catch { case NonFatal(_) => /* best-effort */ }
    }
  }

  /** Test-only. Returns the currently-registered (ctx, listener) pair, if any. */
  private[spark] def currentRegistrationForTesting(): Option[(SparkContext, AnyRef)] = {
    val r = registration.get()
    if (r == null) None else Some((r.ctx, r.listener))
  }
}
