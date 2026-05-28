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
import io.github.luciferyang.cachedfs.core.CacheEntry;
import io.github.luciferyang.cachedfs.core.CachePin;
import io.github.luciferyang.cachedfs.core.CoalesceIo;
import io.github.luciferyang.cachedfs.core.FindResult;
import io.github.luciferyang.cachedfs.core.RawFileCacheKey;
import io.github.luciferyang.cachedfs.core.handle.CachedFactory;
import io.github.luciferyang.cachedfs.core.handle.FileHandle;
import io.github.luciferyang.cachedfs.core.io.ReadFile;
import io.github.luciferyang.cachedfs.core.stats.AggregatedIoStatistics;
import io.github.luciferyang.cachedfs.core.stats.IoStatistics;
import io.github.luciferyang.cachedfs.core.tracker.ScanTracker;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * {@link InputStream} that satisfies reads from the RAM cache, populating it on miss from the
 * underlying {@link ReadFile}. Implements {@link Seekable} and {@link PositionedReadable} so it can
 * be wrapped in a Hadoop {@link org.apache.hadoop.fs.FSDataInputStream} unchanged.
 *
 * <p>The file is logically chopped into {@code loadQuantum}-sized chunks aligned at multiples of
 * {@code loadQuantum}; each chunk is one cache entry keyed by {@code (fileNum, chunkOffset)}. A
 * sequential or random read computes the chunk(s) it overlaps and pulls each through the cache.
 *
 * <p><b>Pin lifetime:</b> chunk pins are acquired and released within a single read call. The
 * stream does not hold pins across calls — that's the cache's job via its LRU.
 */
public final class CachingInputStream extends InputStream
    implements Seekable, PositionedReadable, IOStatisticsSource {

  private final CachedFactory.CachedPtr<String, FileHandle> handlePtr;
  private final FileHandle handle;
  private final AsyncDataCache cache;
  private final int loadQuantum;
  private final long fileSize;
  private final long fileNum;

  // per-(scanId, fileNum) density tracker keyed by the existing `fileNum` field above + per-stream
  // IO counters. Aggregate sink is the bootstrap-level AggregatedIoStatistics; merged exactly once
  // on close() via the `aggregated` AtomicBoolean to keep close() idempotent.
  private final ScanTracker tracker;
  private final IoStatistics ioStats;
  private final AggregatedIoStatistics aggregateIoStats;
  private final AtomicBoolean aggregated = new AtomicBoolean();

  // coalescing knobs — captured at open time from the Configuration.
  private final boolean coalesceEnabled;
  private final int coalesceMaxGapBytes;
  private final int coalesceMaxChunksPerGroup;
  private final int coalesceMaxRestarts;

  // --- admission knobs (captured at open time) -------------------

  /** Bootstrap reference for the heap-pressure check + executor handle. */
  private final CacheBootstrap bootstrap;

  /** When false, the bump-site is a no-op regardless of state. */
  private final boolean prefetchEnabled;

  private final boolean heapPressureCheckEnabled;
  private final double triggerTailFraction;
  private final int densityThresholdPct;
  private final long rejectionBackoffNs;

  // --- prefetch state (no callers in this commit; wired in next) -

  /**
   * In-flight prefetch handle (key, not pin — key-not-pin design). VarHandle CAS is used to
   * coordinate submission vs rejection-handler reset; the helper {@link
   * #clearPendingPrefetchIf(CompletableFuture)} is the single mutation surface from sibling classes
   * ({@link PrefetchTask}, {@link DiscardAndCountHandler}).
   */
  private volatile CompletableFuture<io.github.luciferyang.cachedfs.core.RawFileCacheKey>
      pendingPrefetch;

  /** Package-private; sibling classes use {@link #clearPendingPrefetchIf}. */
  static final VarHandle PENDING_VH;

  static {
    try {
      PENDING_VH =
          MethodHandles.lookup()
              .findVarHandle(CachingInputStream.class, "pendingPrefetch", CompletableFuture.class);
    } catch (ReflectiveOperationException ex) {
      throw new ExceptionInInitializerError(ex);
    }
  }

  /**
   * Per-stream nanoTime of the last rejection-handler bump. Read by the admission gate to enforce a
   * per-stream backoff after a saturated-queue rejection. Initialized in the constructor as {@code
   * System.nanoTime() - REJECTION_BACKOFF_NS - 1} so the first prefetch attempt is never
   * back-pressured AND the static-sentinel overflow edge (146-year uptime) is avoided. Volatile for
   * defense-in-depth: write site (rejection handler) and read site (admission gate) BOTH run on the
   * consumer thread per the {@link ThreadPoolExecutor} synchronous-on-submit-thread invariant, but
   * {@code volatile} removes the assumption against a future executor wrapper.
   */
  volatile long lastRejectionNanos;

  // volatile so a caller who holds a raw CachingInputStream reference (bypassing
  // FSDataInputStream's own monitor) cannot observe a torn 64-bit read on JVMs where long writes
  // are not atomic. Cost is one fence per access; the I/O dwarfs it.
  private volatile long position;
  // AtomicBoolean so close() is idempotent under accidental concurrent close — the cache pin
  // must release exactly once even if a caller closes the FSDataInputStream from multiple
  // threads (Hadoop's own FSDataInputStream serializes via its monitor, but defensive).
  private final AtomicBoolean closed = new AtomicBoolean();

  CachingInputStream(
      CachedFactory.CachedPtr<String, FileHandle> handlePtr,
      AsyncDataCache cache,
      int loadQuantum,
      ScanTracker tracker,
      IoStatistics ioStats,
      AggregatedIoStatistics aggregateIoStats,
      boolean coalesceEnabled,
      int coalesceMaxGapBytes,
      int coalesceMaxChunksPerGroup,
      int coalesceMaxRestarts,
      CacheBootstrap bootstrap,
      boolean prefetchEnabled,
      boolean heapPressureCheckEnabled,
      double triggerTailFraction,
      int densityThresholdPct,
      long rejectionBackoffNs) {
    this.handlePtr = handlePtr;
    this.handle = handlePtr.value();
    this.cache = cache;
    this.loadQuantum = loadQuantum;
    try {
      this.fileSize = handle.readFile().size();
    } catch (IOException ex) {
      throw new java.io.UncheckedIOException(ex);
    }
    this.fileNum = handle.fileNum();
    this.tracker = tracker;
    this.ioStats = ioStats;
    this.aggregateIoStats = aggregateIoStats;
    this.coalesceEnabled = coalesceEnabled;
    this.coalesceMaxGapBytes = coalesceMaxGapBytes;
    this.coalesceMaxChunksPerGroup = coalesceMaxChunksPerGroup;
    this.coalesceMaxRestarts = coalesceMaxRestarts;
    this.bootstrap = bootstrap;
    this.prefetchEnabled = prefetchEnabled;
    this.heapPressureCheckEnabled = heapPressureCheckEnabled;
    this.triggerTailFraction = triggerTailFraction;
    this.densityThresholdPct = densityThresholdPct;
    this.rejectionBackoffNs = rejectionBackoffNs;
    // Place lastRejectionNanos in the "far past" so the very first prefetch attempt is never
    // back-pressured. Long.MIN_VALUE/2 makes `now - lastRejectionNanos = now + |MIN/2|`, which is
    // a large positive value regardless of the configured backoff — robust even if a future
    // deployment sets rejectionBackoffNs near Long.MAX_VALUE/2 where the prior `now - 2*backoff`
    // formula would have signed-overflowed back into a small positive and inadvertently gated the
    // cold start.
    this.lastRejectionNanos = Long.MIN_VALUE / 2;
  }

  // --- sibling helpers (package-private; called by PrefetchTask + Handler) -

  /**
   * CAS-resets the {@link #pendingPrefetch} slot to null iff its current value is {@code expected}.
   * Safe no-op if a peer already replaced the slot. Used by {@link PrefetchTask#run} and {@link
   * DiscardAndCountHandler} to release ownership of the CAS slot without exposing the underlying
   * {@link VarHandle} cross-class.
   */
  void clearPendingPrefetchIf(CompletableFuture<?> expected) {
    PENDING_VH.compareAndSet(this, expected, null);
  }

  /** Sibling-class setter for the per-stream rejection backoff timestamp. */
  void setLastRejectionNanos(long nanos) {
    this.lastRejectionNanos = nanos;
  }

  // --- seqHWM state machine ------------------------------

  /**
   * Sentinel value for {@link #seqHWM} meaning "no read observed yet; prefetch arming disabled".
   * Picked as {@code -1L} because the smallest legitimate HWM is {@code 0 + chunkSize > 0}.
   */
  static final long SEQ_HWM_SENTINEL = -1L;

  /**
   * Sequential read high-water mark. Driven by three update paths:
   *
   * <ul>
   *   <li>Sequential reads ({@link #read(byte[], int, int)} via the per-chunk-completed cursor) use
   *       an explicit CAS loop with three branches: sentinel→bootstrap, regime-change reset (|prev
   *       − candidate| &gt; 2 × chunkSize), monotone advance via Math.max.
   *   <li>Positional reads ({@link #read(long, byte[], int, int)} / {@link #readFully(long, byte[],
   *       int, int)}) use {@link AtomicLong#updateAndGet} with a lambda that bootstraps from {@code
   *       -1L} or advances on strict contiguity (pos == prev); scattered calls are no-ops.
   *   <li>{@link #seek(long)} resets via {@code seqHWM.set(-1L)} — single-threaded per the Hadoop
   *       Seekable non-thread-safety contract; valid because the read paths use CAS so a concurrent
   *       positional CAS at most loses one round-trip vs the reset.
   * </ul>
   *
   * <p>{@link IoStatistics#incSeqHwmRegimeResets} is bumped EXACTLY ONCE per logical regime-change
   * event (derived from the same {@code prev} value the winning {@link AtomicLong#compareAndSet}
   * observes — see {@link #advanceSeqHwmSequential(long, int)}).
   */
  private final java.util.concurrent.atomic.AtomicLong seqHWM =
      new java.util.concurrent.atomic.AtomicLong(SEQ_HWM_SENTINEL);

  /**
   * Bumps {@link #seqHWM} for the sequential read path via an explicit CAS loop. Returns the new
   * HWM value on success. The {@code wasReset} flag — derived from the SAME {@code prev} the
   * winning CAS observes — drives the exactly-once {@code seqHwmRegimeResets} counter bump.
   *
   * @param newReadEnd offset one past the last byte of the just-completed sequential chunk ({@code
   *     position + chunkSize})
   * @param chunkSize chunk size of the just-completed read; the 2-chunk regime-change threshold is
   *     computed from this value
   */
  long advanceSeqHwmSequential(long newReadEnd, int chunkSize) {
    long regimeChangeThreshold = 2L * chunkSize;
    while (true) {
      long prev = seqHWM.get();
      long next;
      boolean wasReset;
      if (prev == SEQ_HWM_SENTINEL) {
        next = newReadEnd;
        wasReset = false; // bootstrap, not a reset
      } else if (Math.abs(prev - newReadEnd) > regimeChangeThreshold) {
        next = newReadEnd;
        wasReset = true; // regime change — breaks the positional-first dead-zone
      } else {
        next = Math.max(prev, newReadEnd);
        wasReset = false;
      }
      if (seqHWM.compareAndSet(prev, next)) {
        if (wasReset) {
          ioStats.incSeqHwmRegimeResets();
        }
        return next;
      }
      // CAS lost the race — peer thread advanced or reset HWM; retry with fresh prev.
    }
  }

  /**
   * Bumps {@link #seqHWM} for the positional read path. Uses {@link AtomicLong#updateAndGet}
   * because positional reads can run concurrently with sequential reads on the same stream
   * (Hadoop's {@code PositionedReadable} contract is multi-reader thread-safe).
   *
   * <p>Three lambda outcomes:
   *
   * <ul>
   *   <li>Sentinel → bootstrap to {@code pos + len}.
   *   <li>{@code pos == prev} → contiguous-positional advance to {@code pos + len}.
   *   <li>Otherwise → no change (scattered positional; intentionally does NOT update HWM).
   * </ul>
   */
  long advanceSeqHwmPositional(long pos, int len) {
    return seqHWM.updateAndGet(
        prev -> {
          if (prev == SEQ_HWM_SENTINEL) {
            return pos + len; // bootstrap from any starting offset
          }
          if (pos == prev) {
            return pos + len; // contiguous extend
          }
          return prev; // scattered read — leave HWM untouched
        });
  }

  /** Test-only accessor for the {@link #seqHWM} value. */
  long seqHwmForTesting() {
    return seqHWM.get();
  }

  /** Test-only accessor for {@link #ioStats} so tests can observe per-stream counters directly. */
  IoStatistics ioStatsForTesting() {
    return ioStats;
  }

  /** Test-only accessor for the {@link #pendingPrefetch} slot. */
  CompletableFuture<io.github.luciferyang.cachedfs.core.RawFileCacheKey>
      pendingPrefetchForTesting() {
    return pendingPrefetch;
  }

  /**
   * Debug counter incremented every time {@link #submitPrefetch} successfully CAS-installed the
   * future into the {@code pendingPrefetch} slot AND submitted the task (whether the task later
   * runs to completion or gets rejected). Hadoop's {@code PositionedReadable} contract is
   * multi-reader thread-safe, so two concurrent positional readers can both reach {@code
   * submitPrefetch}; we use a {@link java.util.concurrent.atomic.LongAdder} so neither's bump is
   * lost. (Used by tests; production code does not read this.)
   */
  private final java.util.concurrent.atomic.LongAdder prefetchSubmissions =
      new java.util.concurrent.atomic.LongAdder();

  long prefetchSubmissionsForTesting() {
    return prefetchSubmissions.sum();
  }

  // --- admission gate + submission ----------------------

  /**
   * Evaluates the admission gate's resource predicates and returns the appropriate flyweight {@link
   * AdmissionResult}. NOT a guard on the trigger conditions (position-tail-fraction, backoff,
   * density) — caller checks those BEFORE invoking this. The gate covers ONLY the resource limits:
   * byte budget and (optionally) heap pressure.
   */
  private AdmissionResult admissionGate(int chunkSize) {
    if (cache.pendingPrefetchBytes() + chunkSize > bootstrap.maxPendingPrefetchBytes()) {
      return AdmissionResult.BUDGET_REJECT;
    }
    if (heapPressureCheckEnabled && bootstrap.isHeapPressureHigh()) {
      return AdmissionResult.HEAP_REJECT;
    }
    return AdmissionResult.ADMIT;
  }

  /**
   * Trigger predicate + 3-branch bump-site for the prefetch admission gate. Called from {@link
   * #read(byte[], int, int)} and the positional read paths after each completed read so the gate
   * fires once per "logical chunk advance".
   *
   * <p>Trigger guards (all must hold):
   *
   * <ol>
   *   <li>{@link #prefetchEnabled} (master toggle) AND {@link #bootstrap}.prefetchExecutor() !=
   *       null.
   *   <li>No in-flight prefetch already pinned in the CAS slot ({@code pendingPrefetch == null}).
   *   <li>State machine armed ({@code seqHWM > 0}).
   *   <li>Position past the trigger-tail fraction of the current chunk.
   *   <li>Next-chunk start is within the file.
   *   <li>Rejection backoff elapsed ({@code nanoTime() - lastRejectionNanos >=
   *       rejectionBackoffNs}).
   * </ol>
   *
   * <p>When the guards pass, the gate evaluates {@code adm} (resource gate) and density
   * INDEPENDENTLY, then accumulates observability counters per failed predicate:
   *
   * <ul>
   *   <li>Both pass → submit the prefetch.
   *   <li>Admission rejects → bump {@code incPrefetchSkipped(adm.reason(), chunkSize)}.
   *   <li>Density fails → bump {@code incPrefetchEligibleSuppressed(chunkSize)}.
   * </ul>
   *
   * <p>When BOTH predicates fail, BOTH counters are bumped — they are independent observability
   * signals. Operators investigating capacity-driven prefetch loss must not see only the density
   * bucket when the cache is also over-budget.
   */
  private void maybeTriggerPrefetch(long currentPos) {
    if (!prefetchEnabled) return;
    java.util.concurrent.ThreadPoolExecutor exec = bootstrap.prefetchExecutor();
    if (exec == null) return;
    if (pendingPrefetch != null) return;

    long hwm = seqHWM.get();
    if (hwm <= 0L) return; // state machine not armed yet

    // Use the LAST byte read (currentPos - 1) to identify the chunk we just touched. Avoids the
    // boundary case where currentPos lands exactly on a chunk start (progressInChunk would be 0
    // for the next chunk and we'd never trigger — the read just CONSUMED 100% of the prior
    // chunk and is ready to advance).
    if (currentPos <= 0) return;
    long lastByte = currentPos - 1;
    long thisChunkStart = (lastByte / loadQuantum) * (long) loadQuantum;
    long thisChunkEnd = thisChunkStart + loadQuantum;
    double progressInChunk = (double) (currentPos - thisChunkStart) / (double) loadQuantum;
    if (progressInChunk < triggerTailFraction) return;

    long nextChunkStart = thisChunkEnd;
    if (nextChunkStart >= fileSize) return;
    int nextChunkSize = (int) Math.min((long) loadQuantum, fileSize - nextChunkStart);

    long now = System.nanoTime();
    if (now - lastRejectionNanos < rejectionBackoffNs) return;

    // Resolve resource gate + density predicate independently and accumulate observability
    // counters per failed predicate. Submitting requires BOTH to pass.
    AdmissionResult adm = admissionGate(nextChunkSize);
    boolean densityPass = tracker.data(fileNum).readPct() >= densityThresholdPct;

    if (adm.admit() && densityPass) {
      submitPrefetch(nextChunkStart, nextChunkSize);
      return;
    }
    if (!adm.admit()) {
      ioStats.incPrefetchSkipped(adm.reason(), nextChunkSize);
    }
    if (!densityPass) {
      ioStats.incPrefetchEligibleSuppressed(nextChunkSize);
    }
  }

  /**
   * Submits a prefetch for {@code (nextChunkStart, nextChunkSize)} via the bootstrap's executor.
   * Performs the CAS to install the {@code pendingPrefetch} slot before submission so the rejection
   * handler (synchronous-on-submit-thread) sees the slot already pointing at our future. Catches
   * {@link java.util.concurrent.RejectedExecutionException} for the post-shutdown race (executor
   * moved to SHUTDOWN between our null check and our {@code execute()} call) and mirrors the {@link
   * DiscardAndCountHandler} recovery.
   */
  private void submitPrefetch(long nextChunkStart, int nextChunkSize) {
    java.util.concurrent.CompletableFuture<io.github.luciferyang.cachedfs.core.RawFileCacheKey>
        future = new java.util.concurrent.CompletableFuture<>();
    if (!PENDING_VH.compareAndSet(this, null, future)) {
      // Another path raced us to install — skip silently.
      future.cancel(false);
      return;
    }
    io.github.luciferyang.cachedfs.core.RawFileCacheKey key =
        new io.github.luciferyang.cachedfs.core.RawFileCacheKey(fileNum, nextChunkStart);
    PrefetchTask task =
        new PrefetchTask(
            this, ioStats, cache, handle.readFile(), nextChunkSize, key, nextChunkStart, future);
    try {
      bootstrap.prefetchExecutor().execute(task);
      prefetchSubmissions.increment();
    } catch (java.util.concurrent.RejectedExecutionException ex) {
      // Mirror DiscardAndCountHandler recovery for the post-shutdown race.
      ioStats.incPrefetchSkipped("queue_full", nextChunkSize);
      future.completeExceptionally(ex);
      clearPendingPrefetchIf(future);
      lastRejectionNanos = System.nanoTime();
    }
  }

  /**
   * Package-private static extracted from the original instance {@code fillExclusive} so {@link
   * PrefetchTask} can fill cache entries without duplicating the preadv logic. The instance method
   * below is now a thin wrapper around this static so the existing read path is unchanged.
   */
  static void fillExclusive(
      ReadFile readFile, CachePin exclusivePin, long chunkStart, int chunkSize) throws IOException {
    CacheEntry entry = exclusivePin.entry();
    List<ByteBuffer> ranges = entry.dataRanges(chunkSize);
    try {
      readFile.preadv(chunkStart, ranges);
    } catch (IOException | RuntimeException | Error ex) {
      // Release the failed exclusive so waiters retry instead of deadlocking on the promise.
      // OutOfMemoryError from preadv would otherwise leak the exclusive pin, leaving the cache
      // slot in EXCLUSIVE state forever — every future findOrCreate for that key would block on
      // the promise that no thread will complete.
      try {
        exclusivePin.close();
      } catch (RuntimeException | Error suppressed) {
        ex.addSuppressed(suppressed);
      }
      throw ex;
    }
  }

  // --- InputStream ---------------------------------------------------------

  @Override
  public int read() throws IOException {
    byte[] one = new byte[1];
    int n = read(one, 0, 1);
    return n < 0 ? -1 : (one[0] & 0xff);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    // InputStream contract: validate bounds and throw IndexOutOfBoundsException — Hadoop's
    // contract tests expect the standard exception type, not a deep ArrayIndexOutOfBoundsException
    // thrown later from a buffer copy.
    Objects.checkFromIndexSize(off, len, b.length);
    ensureOpen();
    if (len == 0) return 0;
    if (position >= fileSize) return -1;
    int n = (int) Math.min(len, fileSize - position);
    readFullyFromCache(position, b, off, n);
    position += n;
    // advance the sequential HWM via the explicit CAS loop. Uses chunkSize as
    // the regime-change threshold computed from the just-completed read's natural quantum (n vs.
    // loadQuantum). Picking `n` keeps the threshold semantically aligned to the actual stride;
    // sequential consumers reading less than loadQuantum (e.g. small reads spanning multiple
    // calls) get a finer threshold than the per-chunk plan describes but still satisfy the
    // dead-zone-breaking property.
    advanceSeqHwmSequential(position, Math.max(n, 1));
    // after each completed sequential read, consider firing a prefetch for the
    // next chunk if all trigger guards + density + resource gates pass.
    maybeTriggerPrefetch(position);
    return n;
  }

  @Override
  public int available() {
    long remaining = fileSize - position;
    return remaining > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) Math.max(0, remaining);
  }

  @Override
  public long skip(long n) {
    if (n <= 0) return 0;
    long target = Math.min(position + n, fileSize);
    long skipped = target - position;
    position = target;
    return skipped;
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    // cancel any in-flight prefetch promise. PrefetchTask's run-finally will detect the
    // cancellation (cancel(false) marks the future as CANCELLED) and skip the future.complete
    // calls; the decrementPendingPrefetch + clearPendingPrefetchIf still fire so the byte-budget
    // stays balanced. cancel(false) does NOT interrupt the worker thread — that would race the
    // exclusivePin.close() inside fillExclusive's catch.
    @SuppressWarnings("unchecked")
    java.util.concurrent.CompletableFuture<io.github.luciferyang.cachedfs.core.RawFileCacheKey>
        pending = pendingPrefetch;
    if (pending != null) {
      pending.cancel(false);
      // Bounded drain so an in-flight PrefetchTask running on the executor thread finishes its
      // run-finally (decrement + clearPendingPrefetchIf + any final ioStats bumps) BEFORE we
      // merge ioStats into the bootstrap aggregate. The slot-clear is the canonical "task done"
      // signal — PrefetchTask null's pendingPrefetch in its finally AFTER the ioStats inc calls.
      // If the drain times out, the post-deadline counter bumps land in this stream's ioStats
      // unobserved; that's the documented soft-loss trade-off (the alternative — blocking close
      // indefinitely on a stuck preadv — is worse than slight counter imprecision).
      long deadlineNanos =
          System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(500);
      while (pendingPrefetch != null && System.nanoTime() < deadlineNanos) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    // Merge per-stream stats into the bootstrap aggregate exactly once. The merge contract
    // requires `ioStats` to be quiescent here — Hadoop's FSDataInputStream.close is non-thread-
    // safe vs reads; the prefetch drain above covers the executor-thread bumps. NO_OP merges as
    // zeros.
    if (aggregated.compareAndSet(false, true)) {
      aggregateIoStats.add(ioStats);
      // Push a snapshot into the bootstrap's recent-streams ring so post-hoc debugging can see
      // what this stream observed. RecentStreams.DISABLED swallows the push when the operator has
      // disabled the ring; the conditional avoids the allocation cost in that case.
      io.github.luciferyang.cachedfs.core.stats.RecentStreams ring = bootstrap.recentStreams();
      if (ring.capacity() > 0) {
        ring.add(io.github.luciferyang.cachedfs.core.stats.IoStatisticsSnapshot.of(ioStats));
      }
    }
    handlePtr.close();
  }

  @Override
  public IOStatistics getIOStatistics() {
    return IoStatisticsAdapter.forStream(ioStats);
  }

  // --- Seekable ------------------------------------------------------------

  @Override
  public void seek(long pos) throws IOException {
    ensureOpen();
    if (pos < 0) {
      // Hadoop's AbstractContractSeekTest.testNegativeSeek prefers EOFException for negative
      // positions (with plain IOException accepted only as a relaxed-compliance fallback). Use
      // the preferred type so frameworks that catch EOFException as an out-of-range signal
      // (HBase, some Parquet readers) work without specialized error handling.
      throw new java.io.EOFException("Cannot seek to negative offset: " + pos);
    }
    // Hadoop convention: seeking past EOF is allowed; subsequent reads return -1.
    position = pos;
    // a seek invalidates the sequential trajectory; reset the HWM to the
    // sentinel so the next read on the new region re-bootstraps. Seekable.seek is single-thread
    // per Hadoop contract (incompatible with concurrent reads on the same stream), so a plain
    // set is sufficient — the CAS-based read paths cannot race with seek by contract.
    seqHWM.set(SEQ_HWM_SENTINEL);
  }

  @Override
  public long getPos() {
    return position;
  }

  @Override
  public boolean seekToNewSource(long targetPos) {
    return false;
  }

  // --- PositionedReadable --------------------------------------------------

  @Override
  public int read(long pos, byte[] buffer, int offset, int length) throws IOException {
    Objects.checkFromIndexSize(offset, length, buffer.length);
    ensureOpen();
    if (length == 0) return 0;
    if (pos < 0) {
      throw new IllegalArgumentException("negative position: " + pos);
    }
    if (pos >= fileSize) return -1;
    int n = (int) Math.min(length, fileSize - pos);
    readFullyFromCache(pos, buffer, offset, n);
    // positional path uses updateAndGet — bootstrap from -1L on first call,
    // contiguous-extend on pos == prev, otherwise leave HWM unchanged (scattered reads do not
    // perturb the sequential trajectory).
    advanceSeqHwmPositional(pos, n);
    maybeTriggerPrefetch(pos + n);
    return n;
  }

  @Override
  public void readFully(long pos, byte[] buffer, int offset, int length) throws IOException {
    Objects.checkFromIndexSize(offset, length, buffer.length);
    ensureOpen();
    if (length == 0) return;
    // PositionedReadable.readFully: EOFException means "end of stream reached"; a negative
    // position is an argument error, not an EOF condition. Distinguish so callers that catch
    // EOFException as a short-file signal do not misinterpret a programming bug.
    if (pos < 0) {
      throw new IllegalArgumentException("negative position: " + pos);
    }
    if ((long) pos + length > fileSize) {
      throw new java.io.EOFException(
          "readFully past EOF: pos=" + pos + " len=" + length + " size=" + fileSize);
    }
    readFullyFromCache(pos, buffer, offset, length);
    advanceSeqHwmPositional(pos, length);
    maybeTriggerPrefetch(pos + length);
  }

  @Override
  public void readFully(long pos, byte[] buffer) throws IOException {
    readFully(pos, buffer, 0, buffer.length);
  }

  // --- core read-with-cache loop ------------------------------------------

  /**
   * Copies exactly {@code length} bytes starting at file offset {@code pos} into {@code
   * dst[off..]}. Walks the overlapping cache chunks; each chunk is hit-or-fill via {@link
   * AsyncDataCache#findOrCreate}.
   */
  private void readFullyFromCache(long pos, byte[] dst, int off, int length) throws IOException {
    // record top-of-call signals BEFORE issuing IO so a throw mid-read still reflects
    // the planned reference. recordReference is a no-op on ScanTracker.DISABLED;
    // incRead is a no-op on IoStatistics.NO_OP.
    tracker.recordReference(fileNum, length);
    ioStats.incRead(length);

    // if the read crosses 2+ chunks AND coalescing is enabled, attempt the coalesce
    // path. Falls back to per-chunk on Waiting-driven restart exhaustion.
    long firstChunkStart = (pos / loadQuantum) * (long) loadQuantum;
    long endExclusive = pos + length;
    long lastChunkStart = ((endExclusive - 1) / loadQuantum) * (long) loadQuantum;
    int chunkCount = (int) ((lastChunkStart - firstChunkStart) / loadQuantum) + 1;

    if (coalesceEnabled && chunkCount >= 2) {
      for (int restart = 0; restart <= coalesceMaxRestarts; restart++) {
        CoalesceOutcome outcome = readCoalesced(pos, dst, off, length, firstChunkStart, chunkCount);
        if (outcome == CoalesceOutcome.OK) {
          tracker.recordRead(fileNum, length);
          return;
        }
        // RESTART: pins released, waited-on future completed; loop and try again.
      }
      // Bound exceeded — fall through to the per-chunk path below. This is correctness-preserving;
      // we just lose the coalesce benefit on this read. ioStats.read was already incremented
      // above, so we don't double-count.
    }

    // Per-chunk fallback (also handles single-chunk reads and the disabled-coalesce path).
    long cursor = pos;
    int dstCursor = off;
    int remaining = length;
    while (remaining > 0) {
      long chunkStart = (cursor / loadQuantum) * (long) loadQuantum;
      int chunkSize = (int) Math.min((long) loadQuantum, fileSize - chunkStart);
      int withinChunk = (int) (cursor - chunkStart);
      int copyLen = Math.min(remaining, chunkSize - withinChunk);
      copyChunk(chunkStart, chunkSize, withinChunk, dst, dstCursor, copyLen);
      cursor += copyLen;
      dstCursor += copyLen;
      remaining -= copyLen;
    }
    // Bottom-of-call: bytes actually consumed equal `length` (we always satisfy the whole
    // request via the loop or throw). recordRead drives readPct() / adjustedReadPct() in the
    // tracker; the prefetch admission gate reads those values to gate prefetch.
    tracker.recordRead(fileNum, length);
  }

  // --- coalesce path -------------------------------------------

  /** Outcome of a single coalesce attempt — caller decides whether to retry or move on. */
  private enum CoalesceOutcome {
    /** All chunks served; bytes copied into the destination. */
    OK,
    /**
     * A {@link FindResult.Waiting} forced us to release pins and await a peer fill. Caller restarts
     * the walk from scratch (up to {@code coalesceMaxRestarts}).
     */
    RESTART
  }

  /**
   * One chunk slot in the walk + classify list. Holds a single pin: the original Hit/Exclusive pin
   * on entry; replaced in-place with the promoted Shared pin after preadv completes successfully.
   * Setting {@code pin = null} releases ownership (used after {@code close} in the finally block to
   * avoid double-close).
   */
  private static final class Resolved {
    final long chunkStart;
    final int chunkSize;

    /** True if this slot needs filling via preadv; false for already-cached Hit. */
    final boolean exclusive;

    CachePin pin;

    Resolved(long chunkStart, int chunkSize, boolean exclusive, CachePin pin) {
      this.chunkStart = chunkStart;
      this.chunkSize = chunkSize;
      this.exclusive = exclusive;
      this.pin = pin;
    }
  }

  /**
   * One attempt at the coalesce path. On {@link CoalesceOutcome#RESTART} all pins acquired in this
   * attempt have been released and the awaited future has completed; the caller re-issues {@link
   * #readCoalesced} to walk-classify-coalesce again from {@code firstChunkStart}.
   */
  private CoalesceOutcome readCoalesced(
      long pos, byte[] dst, int off, int length, long firstChunkStart, int chunkCount)
      throws IOException {
    List<Resolved> resolved = new ArrayList<>(chunkCount);
    CompletableFuture<Void> waitOn = null;
    boolean copyDone = false;
    try {
      // Step 1: Walk + classify. Acquire pins for Hit/Exclusive, abort on Waiting.
      for (int i = 0; i < chunkCount; i++) {
        long chunkStart = firstChunkStart + (long) i * loadQuantum;
        int chunkSize = (int) Math.min((long) loadQuantum, fileSize - chunkStart);
        RawFileCacheKey key = new RawFileCacheKey(fileNum, chunkStart);
        FindResult r = cache.findOrCreate(key, chunkSize, /* contiguous= */ false);
        switch (r) {
          case FindResult.Hit hit ->
              resolved.add(new Resolved(chunkStart, chunkSize, /* exclusive= */ false, hit.pin()));
          case FindResult.Exclusive ex ->
              resolved.add(new Resolved(chunkStart, chunkSize, /* exclusive= */ true, ex.pin()));
          case FindResult.Waiting w -> {
            waitOn = w.future();
          }
        }
        if (waitOn != null) break;
      }

      if (waitOn != null) {
        // Abort sub-routine: release pins (ascending offset), then await the future, then signal
        // restart. Failures during release are suppressed onto the await exception (if any).
        IOException releaseFailure = closeAllPinsCollect(resolved);
        try {
          awaitFuture(waitOn);
        } catch (IOException ex) {
          if (releaseFailure != null) ex.addSuppressed(releaseFailure);
          throw ex;
        }
        if (releaseFailure != null) throw releaseFailure;
        // Pins were released; clear so the finally block doesn't try again.
        for (Resolved r : resolved) r.pin = null;
        return CoalesceOutcome.RESTART;
      }

      // Step 2-3: Coalesce consecutive Exclusives and issue preadv per group; record overread.
      fillCoalescedExclusives(resolved);

      // Step 4: Promote each Exclusive to Shared. On any throw, the failed exclusive is closed
      // inside the helper; the finally block below cleans up the rest.
      promoteExclusivesToShared(resolved);

      // Step 5: Copy bytes out of the now-Shared (and Hit) entries.
      copyFromResolved(resolved, pos, dst, off, length);
      copyDone = true;
      return CoalesceOutcome.OK;
    } finally {
      // Close every pin still owned by `resolved` (Hit pins; Shared pins from successful promotion;
      // un-promoted Exclusives left over after a mid-step failure). Best-effort: collect into the
      // already-propagating exception via the JVM's default suppression machinery.
      for (Resolved r : resolved) {
        if (r.pin != null) {
          try {
            r.pin.close();
          } catch (RuntimeException | Error ignored) {
            // Throwing again here would mask the original exception. Tests assert
            // PinLeakAssertions.assertNoLeak() so a genuine leak would surface there.
          }
        }
      }
      if (!copyDone && waitOn == null) {
        // Unexpected exception path: ensure ioStats reflects what we did attempt. recordRead
        // happens in the caller only on OK; failed reads do NOT bump readBytes-consumed since
        // the consumer has no usable bytes yet.
      }
    }
  }

  /**
   * Coalesces consecutive {@code exclusive} entries in {@code resolved} (preserving offset order)
   * and issues one {@code preadv} per group via the {@link CoalesceIo} helper. Adjacent Hit entries
   * split groups. Throws on the FIRST preadv failure; entries in the failed group keep their
   * Exclusive pins (caller's finally closes them).
   */
  private void fillCoalescedExclusives(List<Resolved> resolved) throws IOException {
    int n = resolved.size();
    int i = 0;
    while (i < n) {
      if (!resolved.get(i).exclusive) {
        i++;
        continue;
      }
      // Find the end of the contiguous Exclusive run, bounded by coalesceMaxChunksPerGroup.
      int j = i + 1;
      while (j < n && resolved.get(j).exclusive && (j - i) < coalesceMaxChunksPerGroup) {
        j++;
      }
      int groupCount = j - i;
      final int start = i;
      ReadFile readFile = handle.readFile();
      CoalesceIo.CoalesceIoStats stats =
          CoalesceIo.coalesce(
              groupCount,
              k -> resolved.get(start + k).chunkStart,
              k -> resolved.get(start + k).chunkSize,
              k ->
                  resolved.get(start + k).pin.entry().dataRanges(resolved.get(start + k).chunkSize),
              coalesceMaxGapBytes,
              groupCount, // already bounded by the outer loop; per-call cap == group size
              readFile::preadv);
      // CoalesceIo's extraBytes is the gap bytes the coalescer absorbed (0 for adjacent chunks).
      if (stats.extraBytes() > 0) {
        ioStats.incRawOverreadBytes(stats.extraBytes());
      }
      i = j;
    }
  }

  /**
   * Promotes every Exclusive in {@code resolved} to Shared via {@link CachePin#exclusiveToShared}.
   * Mutates the list in-place: {@code resolved[k].pin} becomes the Shared pin on success. On any
   * throw, closes the failing Exclusive and leaves earlier already-promoted Shared pins in place
   * for the caller's finally to clean up.
   */
  private void promoteExclusivesToShared(List<Resolved> resolved) {
    for (Resolved r : resolved) {
      if (!r.exclusive) continue;
      CachePin excPin = r.pin;
      CachePin shared;
      try {
        shared = excPin.exclusiveToShared(/* ssdSavable= */ true);
      } catch (RuntimeException | Error ex) {
        try {
          excPin.close();
        } catch (RuntimeException | Error suppressed) {
          ex.addSuppressed(suppressed);
        }
        r.pin = null; // we just closed it; finally must not double-close
        throw ex;
      }
      r.pin = shared;
    }
  }

  /** Copies the requested byte range out of every {@link Resolved} entry into {@code dst}. */
  private static void copyFromResolved(
      List<Resolved> resolved, long pos, byte[] dst, int off, int length) {
    long cursor = pos;
    int dstCursor = off;
    int remaining = length;
    for (Resolved r : resolved) {
      if (remaining == 0) break;
      int withinChunk = (int) (cursor - r.chunkStart);
      int copyLen = Math.min(remaining, r.chunkSize - withinChunk);
      copyOutOfEntry(r.pin.entry(), withinChunk, copyLen, dst, dstCursor);
      cursor += copyLen;
      dstCursor += copyLen;
      remaining -= copyLen;
    }
    if (remaining != 0) {
      throw new IllegalStateException(
          "copyFromResolved under-read: remaining=" + remaining + " expected=0");
    }
  }

  /**
   * Releases all pins in {@code resolved} in ascending offset order (their natural order in the
   * list). Returns the first failure wrapped as IOException (with subsequent failures attached as
   * suppressed), or {@code null} if every release succeeded. Used by the abort sub-routine. {@link
   * CachePin#close} declares no checked exceptions; we catch {@link RuntimeException} and {@link
   * Error} for defense-in-depth and surface them as {@link IOException} so the consumer sees a
   * checked exception consistent with the rest of the read path.
   */
  private static IOException closeAllPinsCollect(List<Resolved> resolved) {
    IOException primary = null;
    for (Resolved r : resolved) {
      if (r.pin == null) continue;
      try {
        r.pin.close();
      } catch (RuntimeException | Error ex) {
        if (primary == null) primary = new IOException("pin release failure", ex);
        else primary.addSuppressed(ex);
      }
    }
    return primary;
  }

  /**
   * Resolves the cache entry for the chunk at {@code chunkStart} (creating + filling on miss) and
   * copies {@code copyLen} bytes starting at byte {@code withinChunk} of the chunk into {@code
   * dst[dstCursor..]}.
   */
  private void copyChunk(
      long chunkStart, int chunkSize, int withinChunk, byte[] dst, int dstCursor, int copyLen)
      throws IOException {
    RawFileCacheKey key = new RawFileCacheKey(fileNum, chunkStart);
    while (true) {
      FindResult result = cache.findOrCreate(key, chunkSize, /* contiguous= */ false);
      switch (result) {
        case FindResult.Hit hit -> {
          try (CachePin pin = hit.pin()) {
            // Sealed-result branch is the canonical hit signal — no separate cache.exists probe.
            ioStats.incRamHit(copyLen);
            copyOutOfEntry(pin.entry(), withinChunk, copyLen, dst, dstCursor);
            return;
          }
        }
        case FindResult.Exclusive exclusive -> {
          // We are the producer: fill the entry from the underlying file, then promote to shared
          // and copy out. exclusiveToShared atomically converts the pin so subsequent waiters see
          // the populated entry.
          CachePin exclusivePin = exclusive.pin();
          fillExclusive(exclusivePin, chunkStart, chunkSize);
          // exclusiveToShared transfers ownership on success; if it throws (e.g. CAS state
          // assertion failed because the entry was racily mutated), we must close the exclusive
          // pin ourselves — otherwise the entry stays EXCLUSIVE forever and deadlocks waiters.
          CachePin shared;
          try {
            shared = exclusivePin.exclusiveToShared(/* ssdSavable= */ true);
          } catch (RuntimeException | Error ex) {
            try {
              exclusivePin.close();
            } catch (RuntimeException | Error suppressed) {
              ex.addSuppressed(suppressed);
            }
            throw ex;
          }
          try (shared) {
            copyOutOfEntry(shared.entry(), withinChunk, copyLen, dst, dstCursor);
            return;
          }
        }
        case FindResult.Waiting waiting -> {
          // Another thread is filling. Wait then retry — on retry we should see a Hit.
          awaitFuture(waiting.future());
        }
      }
    }
  }

  private void fillExclusive(CachePin exclusivePin, long chunkStart, int chunkSize)
      throws IOException {
    fillExclusive(handle.readFile(), exclusivePin, chunkStart, chunkSize);
  }

  private static void copyOutOfEntry(
      CacheEntry entry, int withinChunk, int copyLen, byte[] dst, int dstCursor) {
    List<ByteBuffer> ranges = entry.dataRanges(entry.size());
    int produced = 0;
    int skipRemaining = withinChunk;
    int needRemaining = copyLen;
    for (ByteBuffer buf : ranges) {
      if (needRemaining == 0) break;
      ByteBuffer view = buf.duplicate();
      view.position(0).limit(buf.capacity());
      int avail = view.remaining();
      if (skipRemaining >= avail) {
        skipRemaining -= avail;
        continue;
      }
      view.position(skipRemaining);
      skipRemaining = 0;
      int n = Math.min(needRemaining, view.remaining());
      view.get(dst, dstCursor + produced, n);
      produced += n;
      needRemaining -= n;
    }
    if (produced != copyLen) {
      throw new IllegalStateException(
          "copyOutOfEntry under-read: produced=" + produced + " expected=" + copyLen);
    }
  }

  private static void awaitFuture(CompletableFuture<Void> future) throws IOException {
    try {
      future.get();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new java.io.InterruptedIOException(
          "Interrupted waiting for cache fill: " + ex.getMessage());
    } catch (java.util.concurrent.CancellationException ex) {
      // Peer cancelled the fill promise (seek/close on the owning stream, prefetch
      // abandonment, etc.). Treat as an IOException so Hadoop retry harnesses + the coalesce
      // restart loop see a checked exception rather than an unchecked one bubbling up.
      throw new IOException("Cache fill cancelled", ex);
    } catch (ExecutionException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof IOException io) throw io;
      if (cause instanceof RuntimeException re) throw re;
      throw new IOException("Cache fill failed", cause);
    }
  }

  private void ensureOpen() throws IOException {
    if (closed.get()) {
      throw new IOException("Stream is closed");
    }
  }
}
