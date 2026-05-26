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
import io.github.luciferyang.cachedfs.core.tracker.TrackingId;
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

  // Phase 5a: per-(scanId, file) density tracker + packed TrackingId + per-stream IO counters.
  // Aggregate sink is the bootstrap-level AggregatedIoStatistics; merged exactly once on close()
  // via the `aggregated` AtomicBoolean to keep close() idempotent.
  private final ScanTracker tracker;
  private final TrackingId trackingId;
  private final IoStatistics ioStats;
  private final AggregatedIoStatistics aggregateIoStats;
  private final AtomicBoolean aggregated = new AtomicBoolean();

  // Phase 5b coalescing knobs — captured at open time from the Configuration.
  private final boolean coalesceEnabled;
  private final int coalesceMaxGapBytes;
  private final int coalesceMaxChunksPerGroup;
  private final int coalesceMaxRestarts;

  // --- Phase 5c prefetch state (no callers in this commit; wired in next) -

  /**
   * In-flight prefetch handle (key, not pin — Phase 5c key-not-pin design). VarHandle CAS is used
   * to coordinate submission vs rejection-handler reset; the helper {@link
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
      TrackingId trackingId,
      IoStatistics ioStats,
      AggregatedIoStatistics aggregateIoStats,
      boolean coalesceEnabled,
      int coalesceMaxGapBytes,
      int coalesceMaxChunksPerGroup,
      int coalesceMaxRestarts) {
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
    this.trackingId = trackingId;
    this.ioStats = ioStats;
    this.aggregateIoStats = aggregateIoStats;
    this.coalesceEnabled = coalesceEnabled;
    this.coalesceMaxGapBytes = coalesceMaxGapBytes;
    this.coalesceMaxChunksPerGroup = coalesceMaxChunksPerGroup;
    this.coalesceMaxRestarts = coalesceMaxRestarts;
    // Place lastRejectionNanos far in the past so the very first prefetch attempt is never
    // back-pressured. Constructor-time computation avoids the overflow edge of a static sentinel.
    this.lastRejectionNanos = System.nanoTime() - 1_000_000_000L;
  }

  // --- Phase 5c sibling helpers (package-private; called by PrefetchTask + Handler) -

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
    // Merge per-stream stats into the bootstrap aggregate exactly once. The merge contract
    // requires `ioStats` to be quiescent here — Hadoop's FSDataInputStream.close is non-thread-
    // safe vs reads, so callers that follow the contract satisfy this. NO_OP merges as zeros.
    if (aggregated.compareAndSet(false, true)) {
      aggregateIoStats.add(ioStats);
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
    // Phase 5a: record top-of-call signals BEFORE issuing IO so a throw mid-read still reflects
    // the planned reference. recordReference is a no-op on ScanTracker.DISABLED and on
    // TrackingId.EMPTY; incRead is a no-op on IoStatistics.NO_OP.
    tracker.recordReference(trackingId, length);
    ioStats.incRead(length);

    // Phase 5b: if the read crosses 2+ chunks AND coalescing is enabled, attempt the coalesce
    // path. Falls back to per-chunk on Waiting-driven restart exhaustion.
    long firstChunkStart = (pos / loadQuantum) * (long) loadQuantum;
    long endExclusive = pos + length;
    long lastChunkStart = ((endExclusive - 1) / loadQuantum) * (long) loadQuantum;
    int chunkCount = (int) ((lastChunkStart - firstChunkStart) / loadQuantum) + 1;

    if (coalesceEnabled && chunkCount >= 2) {
      for (int restart = 0; restart <= coalesceMaxRestarts; restart++) {
        CoalesceOutcome outcome = readCoalesced(pos, dst, off, length, firstChunkStart, chunkCount);
        if (outcome == CoalesceOutcome.OK) {
          tracker.recordRead(trackingId, length);
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
    // tracker; the prefetch admission gate (Phase 5c) reads those values to gate prefetch.
    tracker.recordRead(trackingId, length);
  }

  // --- coalesce path (Phase 5b) -------------------------------------------

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
