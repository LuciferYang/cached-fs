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
package io.github.luciferyang.cachedfs.hadoop;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * rejection policy: discard the rejected task, bump {@code prefetchSkipped("queue_full")}, complete
 * the task's future exceptionally so the consumer's {@code pendingPrefetch.await} does NOT
 * deadlock, and CAS-clear the owner's pendingPrefetch slot.
 *
 * <p>Sibling top-level class (not nested) so {@code DiscardAndCountHandlerTest} can exercise the
 * handler in isolation against a mock {@code CachingInputStream}. This requires {@code
 * CachingInputStream.PENDING_VH} / {@code clearPendingPrefetchIf} / {@code lastRejectionNanos} to
 * be package-private — see the §Class layout decision in the plan.
 *
 * <p><b>Synchronous-on-submit-thread invariant:</b> per {@link ThreadPoolExecutor}'s JDK contract,
 * the handler runs on the SUBMITTING thread (the consumer's read thread). The handler writes {@code
 * task.owner.lastRejectionNanos} and reads/CAS-clears {@code pendingPrefetch} — same thread that
 * reads them in the admission gate, so no cross-thread visibility concern.
 */
final class DiscardAndCountHandler implements RejectedExecutionHandler {

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    if (!(r instanceof PrefetchTask task)) {
      // Defensive — the submission site always wraps in PrefetchTask. If a future change submits
      // a raw Runnable, falling through silently would drop the task with no observability.
      throw new RejectedExecutionException(
          "DiscardAndCountHandler received non-PrefetchTask runnable: " + r.getClass());
    }
    task.ioStats().incPrefetchSkipped("queue_full", task.chunkSize());
    task.future().completeExceptionally(new RejectedExecutionException("prefetch queue full"));
    task.owner().clearPendingPrefetchIf(task.future());
    task.owner().setLastRejectionNanos(System.nanoTime());
  }
}
