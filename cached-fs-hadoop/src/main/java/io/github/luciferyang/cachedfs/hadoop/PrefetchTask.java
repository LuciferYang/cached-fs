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

import io.github.luciferyang.cachedfs.core.AsyncDataCache;
import io.github.luciferyang.cachedfs.core.CachePin;
import io.github.luciferyang.cachedfs.core.FindResult;
import io.github.luciferyang.cachedfs.core.RawFileCacheKey;
import io.github.luciferyang.cachedfs.core.io.ReadFile;
import io.github.luciferyang.cachedfs.core.stats.IoStatistics;
import java.util.concurrent.CompletableFuture;

/**
 * Named class (NOT a lambda) so the rejection handler can downcast a queued Runnable and recover
 * state. Phase 5c submission ALWAYS goes through {@code prefetchExecutor.execute(prefetchTask)} —
 * never {@code submit(...)}, which would wrap the Runnable in a {@code FutureTask} and defeat the
 * downcast.
 *
 * <p>Invariants on every exit path that {@link #run} reaches (i.e. excluding rejected tasks, which
 * are handled by {@link DiscardAndCountHandler}):
 *
 * <ol>
 *   <li>{@code cache.incrementPendingPrefetch(chunkSize)} fires at the top of run; paired with the
 *       decrement in finally — one-to-one regardless of throws.
 *   <li>The {@link #future()} is completed exactly once.
 *   <li>{@code owner.clearPendingPrefetchIf(future)} releases the CAS slot.
 * </ol>
 *
 * <p>Finally-block ordering: complete the future FIRST (CompletableFuture.complete is documented
 * no-throw), then nest a try/finally so a hypothetical clearPendingPrefetchIf throw still runs the
 * decrement — the consumer's await is never stranded.
 */
final class PrefetchTask implements Runnable {

  private final CachingInputStream owner;
  private final IoStatistics ioStats;
  private final AsyncDataCache cache;
  private final ReadFile readFile;
  private final int chunkSize;
  private final RawFileCacheKey nextKey;
  private final long nextOffset;
  private final CompletableFuture<RawFileCacheKey> future;

  PrefetchTask(
      CachingInputStream owner,
      IoStatistics ioStats,
      AsyncDataCache cache,
      ReadFile readFile,
      int chunkSize,
      RawFileCacheKey nextKey,
      long nextOffset,
      CompletableFuture<RawFileCacheKey> future) {
    this.owner = owner;
    this.ioStats = ioStats;
    this.cache = cache;
    this.readFile = readFile;
    this.chunkSize = chunkSize;
    this.nextKey = nextKey;
    this.nextOffset = nextOffset;
    this.future = future;
  }

  CachingInputStream owner() {
    return owner;
  }

  IoStatistics ioStats() {
    return ioStats;
  }

  int chunkSize() {
    return chunkSize;
  }

  CompletableFuture<RawFileCacheKey> future() {
    return future;
  }

  @Override
  public void run() {
    // Increment FIRST so the byte-budget counter and the decrement in finally are paired one-to-
    // one regardless of any throw in the body. The submission site does not pre-increment — see
    // phase-5c.md §step 4 ("sole site that increments/decrements pendingPrefetchBytes").
    cache.incrementPendingPrefetch(chunkSize);
    Throwable failure = null;
    try {
      FindResult r = cache.findOrCreate(nextKey, chunkSize, /* contiguous= */ false);
      switch (r) {
        case FindResult.Hit hit -> hit.pin().close(); // already cached; nothing to do
        case FindResult.Waiting w -> {
          // Peer is filling. Do NOT await on the executor thread (a full pool would self-deadlock
          // if every worker were awaiting a peer fill). Just leave it to the consumer to find
          // either Hit or another Waiting on the next findOrCreate.
        }
        case FindResult.Exclusive ex -> {
          CachePin excPin = ex.pin();
          try {
            CachingInputStream.fillExclusive(readFile, excPin, nextOffset, chunkSize);
            excPin.exclusiveToShared(/* ssdSavable= */ true).close();
            ioStats.incPrefetch(chunkSize);
          } catch (Throwable t) {
            // Release the failed exclusive so peers don't deadlock waiting on the promise.
            try {
              excPin.close();
            } catch (RuntimeException | Error suppressed) {
              t.addSuppressed(suppressed);
            }
            throw t;
          }
        }
      }
    } catch (Throwable t) {
      failure = t;
    } finally {
      // 1) Complete the future first — CompletableFuture.complete is documented no-throw.
      if (failure != null) {
        future.completeExceptionally(failure);
      } else {
        future.complete(nextKey);
      }
      // 2) Nested try/finally so a clearPendingPrefetchIf throw cannot strand the byte-budget
      //    counter (would leak prefetch bytes for the JVM lifetime).
      try {
        owner.clearPendingPrefetchIf(future);
      } finally {
        cache.decrementPendingPrefetch(chunkSize);
      }
    }
  }
}
