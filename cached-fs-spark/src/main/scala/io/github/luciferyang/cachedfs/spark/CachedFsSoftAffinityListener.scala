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
    // Spark's local mode synthesizes a SparkListenerExecutorAdded with executorId="driver"
    // for the in-process executor (SparkContext.DRIVER_IDENTIFIER, which is private-to-spark
    // so we inline the literal). Adding it to the affinity ring is harmless in local mode
    // (PROCESS_LOCAL hints route to the driver which IS the only executor anyway) but
    // pollutes the ring in mixed local-test / unit-test scenarios and serves no purpose: real
    // cluster deployments never emit a "driver" executor event through this listener.
    if ("driver" == event.executorId) {
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
    // Serialize the entire (read-prior, install-on-new-ctx, detach-from-prior, publish) sequence
    // under the companion-object monitor. ensureRegistered fires once per Spark
    // session-extension load — rare and short — so the monitor is uncontended in practice. The
    // monitor closes a race window that a pure CAS-loop cannot: with CAS-only, two concurrent
    // ensureRegistered calls with DIFFERENT SparkContexts can interleave such that thread A's
    // CAS publishes (scA, listenerA) before A's addSparkListener actually attaches, allowing
    // thread B to CAS over it and call removeSparkListener on listenerA (a no-op because A
    // hasn't attached yet); A then attaches listenerA but the registration says B owns it —
    // listenerA fires events from scA into the manager singleton that B's scB owns,
    // cross-corrupting state.
    LOCK.synchronized {
      val prior = registration.get()
      if (prior != null && (prior.ctx eq sc)) return
      val listener = new CachedFsSoftAffinityListener(sc)
      // Step 1: addSparkListener BEFORE publishing the registration. A throw here (listenerBus
      // stopped) leaves the registration slot untouched.
      try {
        sc.addSparkListener(listener)
      } catch {
        case NonFatal(t) =>
          logWarning(
            s"Failed to register CachedFsSoftAffinityListener on SparkContext " +
              s"${sc.applicationId}",
            t)
          return
      }
      // Step 2: publish the registration. Inside the monitor so no concurrent ensureRegistered
      // can read a stale prior between this set and the prior-listener detach below.
      registration.set(Registration(sc, listener))
      // Step 3: detach the prior listener from its (possibly stopped) SparkContext. Best-effort.
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
    }
  }

  /** Companion-object monitor for the ensureRegistered serialization. Final + private. */
  private val LOCK: Object = new Object()

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
