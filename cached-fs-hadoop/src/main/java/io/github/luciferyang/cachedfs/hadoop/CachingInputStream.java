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
import java.nio.ByteBuffer;
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
      AggregatedIoStatistics aggregateIoStats) {
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
    CacheEntry entry = exclusivePin.entry();
    List<ByteBuffer> ranges = entry.dataRanges(chunkSize);
    try {
      handle.readFile().preadv(chunkStart, ranges);
    } catch (IOException | RuntimeException | Error ex) {
      // Release the failed exclusive so waiters retry instead of deadlocking on the promise.
      // Catching Error too: an OutOfMemoryError from preadv would otherwise leak the
      // exclusive pin, leaving the cache slot in EXCLUSIVE state forever — every future
      // findOrCreate for that key would block on the promise that no thread will complete.
      try {
        exclusivePin.close();
      } catch (RuntimeException | Error suppressed) {
        ex.addSuppressed(suppressed);
      }
      throw ex;
    }
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
