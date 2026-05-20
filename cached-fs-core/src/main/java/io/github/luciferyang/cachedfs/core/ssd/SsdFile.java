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
package io.github.luciferyang.cachedfs.core.ssd;

import io.github.luciferyang.cachedfs.core.RawFileCacheKey;
import io.github.luciferyang.cachedfs.core.id.StringIdMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

/**
 * One SSD shard. Mirrors velox {@code SsdFile} (velox §4).
 *
 * <p>On-disk layout: a single regular file divided into fixed-size 64 MiB regions. Entries never
 * span regions. {@code entries} maps {@link RawFileCacheKey} → {@link SsdRun}. Region pins prevent
 * in-use bytes from being overwritten or evicted.
 *
 * <p>Failure model: on {@code ENOSPC}, transitions to {@link State#NO_SPACE} and short-circuits
 * future writes. On checkpoint failure, {@link Config#checkpointIntervalBytes} is zeroed so the
 * cache continues without checkpointing.
 *
 * <p><b>Checksum algorithm:</b> uses {@link CRC32C} (Castagnoli polynomial 0x1EDC6F41). This is an
 * intentional deviation from velox, which uses standard CRC-32 (IEEE polynomial). The on-disk
 * checkpoint/data files are not wire-compatible with velox in either direction (different magic
 * headers, different on-disk format), so the choice is internal and Castagnoli provides better
 * error detection on modern hardware with CRC32C SIMD acceleration.
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls.
 */
public final class SsdFile implements AutoCloseable {

  /** Velox: {@code kRegionSize = 1 << 26} (64 MiB). */
  public static final int REGION_SIZE = 1 << 26;

  /**
   * Velox: {@code kMaxErasedSizePct = 50} — opportunistic compaction threshold for partially
   * cleared regions.
   */
  public static final int MAX_ERASED_SIZE_PCT = 50;

  /** Disk state. {@link #NO_SPACE} is a terminal state for the writer (reads still work). */
  public enum State {
    ACTIVE,
    NO_SPACE
  }

  /** Denominator used with {@link #MAX_ERASED_SIZE_PCT} to express the threshold as a fraction. */
  private static final int PERCENT_DENOM = 100;

  /** Upper bound on the close() drain wait; prevents a hung reader from blocking shutdown. */
  private static final long DRAIN_TIMEOUT_NANOS = 10L * 1_000_000_000L;

  /** SsdFile configuration. */
  public record Config(
      Path dataPath,
      Path checkpointPath,
      Path logPath,
      Path checkpointTmpPath,
      int maxRegions,
      int maxEntries,
      long checkpointIntervalBytes,
      boolean checksumEnabled,
      boolean checksumReadVerificationEnabled) {

    public Config {
      Objects.requireNonNull(dataPath, "dataPath");
      Objects.requireNonNull(checkpointPath, "checkpointPath");
      Objects.requireNonNull(logPath, "logPath");
      Objects.requireNonNull(checkpointTmpPath, "checkpointTmpPath");
      if (maxRegions <= 0) {
        throw new IllegalArgumentException("maxRegions must be > 0: " + maxRegions);
      }
    }

    /** Default config — checkpoint disabled, no checksum. */
    public static Config defaults(Path dir, String name, int maxRegions) {
      return new Config(
          dir.resolve(name + ".data"),
          dir.resolve(name + ".cpt"),
          dir.resolve(name + ".log"),
          dir.resolve(name + ".cpt.tmp"),
          maxRegions,
          /* maxEntries= */ 0,
          /* checkpointIntervalBytes= */ 0,
          /* checksumEnabled= */ false,
          /* checksumReadVerificationEnabled= */ false);
    }
  }

  private final Config config;
  private final StringIdMap fileIds;

  // Effective checksum-read flag: must be enabled AND checksumEnabled (per velox
  // SsdFile.cpp:112-113)
  private final boolean checksumReadVerificationEnabled;

  private final ReentrantLock lock = new ReentrantLock();

  // volatile: read on the no-lock load() path, written under lock by open()/close().
  private volatile FileChannel channel;
  // volatile: read by state() without lock; flipped by NO_SPACE / writer paths under lock.
  private volatile State state = State.ACTIVE;

  /**
   * Coordinator between {@link #load} (which reads {@link #channel} outside the shard lock) and
   * {@link #close} (which must close the channel). {@code closed} is set first; close then waits
   * for {@code activeReads} to reach zero before invoking {@code channel.close()} so an in-flight
   * read can never observe a torn-down channel. Pin protection alone is not enough — the pin
   * blocks region overwrite, not channel teardown.
   */
  private volatile boolean closed;

  private final AtomicInteger activeReads = new AtomicInteger();

  private final Map<RawFileCacheKey, SsdRun> entries = new HashMap<>();
  private final SsdFileTracker tracker;

  /** Per-region pin counter (read-only protection). */
  private final int[] regionPins;

  /** Bytes erased per region (used for opportunistic compaction). */
  private final long[] regionErased;

  /** Bytes occupied per region. */
  private final long[] regionSizes;

  /** Stack of regions currently writable (head = current writable region). */
  private final Deque<Integer> writableRegions = new ArrayDeque<>();

  /** Number of regions currently in use (grows up to {@code maxRegions}). */
  private int numRegions;

  /** Bytes accumulated since last checkpoint. */
  private long bytesSinceCheckpoint;

  /** Effective checkpoint interval; zeroed on checkpoint failure. */
  private long checkpointIntervalBytes;

  /**
   * Velox SsdFile.cpp suspended_ semantic: set when a write() saw zero eviction candidates (all
   * regions pinned). On the next pin-release that drops a region to zero pins, re-attempt
   * growOrEvict so a subsequent write can succeed without waiting for the next write() entry.
   */
  private boolean suspended;

  /** Constructor. Does NOT open the data file or read the checkpoint — call {@link #open()}. */
  public SsdFile(Config config, StringIdMap fileIds) {
    this.config = config;
    this.fileIds = Objects.requireNonNull(fileIds, "fileIds");
    this.checksumReadVerificationEnabled =
        config.checksumEnabled && config.checksumReadVerificationEnabled;
    this.regionPins = new int[config.maxRegions];
    this.regionErased = new long[config.maxRegions];
    this.regionSizes = new long[config.maxRegions];
    this.tracker = new SsdFileTracker(config.maxRegions);
    this.checkpointIntervalBytes = config.checkpointIntervalBytes;
  }

  /**
   * Opens the data file and replays any existing checkpoint + eviction log. If checkpointing is
   * disabled, removes any pre-existing meta files (toxic combination per velox §4.6).
   */
  public void open() throws IOException {
    Path parent = config.dataPath.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    lock.lock();
    try {
      FileChannel ch =
          FileChannel.open(
              config.dataPath,
              StandardOpenOption.CREATE,
              StandardOpenOption.READ,
              StandardOpenOption.WRITE);
      channel = ch;
      // If any post-open step throws (checkpoint replay, log init, first-region growth), the
      // outer SsdCache rollback never sees this shard (opened[i] stays false), so we must close
      // the channel here ourselves to avoid leaking the FD until GC.
      boolean success = false;
      try {
        if (checkpointEnabled()) {
          initializeCheckpoint();
          // Ensure the eviction log exists with its magic header so the first eviction can
          // append without NoSuchFileException.
          if (!Files.exists(config.logPath)) {
            Checkpoint.truncateLog(config.logPath);
          }
        } else {
          reuseDataFileScratchLocked();
        }
        if (numRegions == 0) {
          // Cold start — grow the first region.
          growOneRegionLocked();
        }
        success = true;
      } finally {
        if (!success) {
          try {
            ch.close();
          } catch (IOException ignored) {
            // best-effort; the primary exception is propagating
          }
          channel = null;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private boolean checkpointEnabled() {
    return checkpointIntervalBytes > 0;
  }

  /**
   * Disabled-checkpoint open: meta files cannot be trusted, but the data file CAN be reused as
   * scratch space (matches velox SsdFile.cpp:127-134). Floor-align to {@link #REGION_SIZE} and seed
   * {@link #writableRegions} with all existing regions; previous bytes are garbage that will be
   * overwritten by subsequent writes from offset 0 within each region.
   */
  private void reuseDataFileScratchLocked() throws IOException {
    Files.deleteIfExists(config.checkpointPath);
    Files.deleteIfExists(config.logPath);
    Files.deleteIfExists(config.checkpointTmpPath);
    long existingBytes = channel.size();
    int existingRegions = (int) Math.min(config.maxRegions, existingBytes / REGION_SIZE);
    if (existingRegions > 0) {
      channel.truncate((long) existingRegions * REGION_SIZE);
      numRegions = existingRegions;
      for (int i = 0; i < existingRegions; i++) {
        writableRegions.addLast(i);
      }
    }
  }

  /** Recovers state from {@code .cpt}/{@code .log}. */
  private void initializeCheckpoint() throws IOException {
    if (!Files.exists(config.checkpointPath)) {
      // Missing checkpoint with checkpointing enabled. Velox SsdFile.cpp:126-134 seeds
      // writableRegions_ from the data file's existing size in the constructor before
      // readCheckpoint runs, so a crash before the first checkpoint does not lose region
      // capacity. Reuse the data file as scratch space (the bytes are unrecoverable, but the
      // space is).
      reuseDataFileScratchLocked();
      return;
    }
    Checkpoint.Snapshot snap;
    try {
      snap = Checkpoint.read(config.checkpointPath);
    } catch (IOException ex) {
      // Graceful degrade — meta files corrupt. Reuse data file capacity as scratch.
      Files.deleteIfExists(config.checkpointPath);
      Files.deleteIfExists(config.logPath);
      Files.deleteIfExists(config.checkpointTmpPath);
      reuseDataFileScratchLocked();
      return;
    }
    // Replay file id ↔ name bindings.
    for (var e : snap.fileIdToName().entrySet()) {
      fileIds.recoverId(e.getKey(), e.getValue());
    }
    numRegions = snap.numRegions();
    // Restore region scores into the tracker's live array (NOT a clone).
    tracker.restoreScores(snap.regionScores(), snap.regionScores().length);
    // Truncate the data file to numRegions × REGION_SIZE.
    channel.truncate((long) numRegions * REGION_SIZE);

    // Apply eviction log — entries in evicted regions must be skipped.
    Set<Integer> evicted = new HashSet<>(Checkpoint.readEvictedRegions(config.logPath));
    replayEntriesLocked(snap, evicted);
    // Initialize writableRegions: ONLY regions cleared by the eviction log are writable
    // (matches velox SsdFile.cpp:1161-1164). Partially-filled regions from the previous run
    // never accept new writes.
    writableRegions.clear();
    for (int r : evicted) {
      if (r < numRegions && regionPins[r] == 0) {
        writableRegions.addLast(r);
      }
    }
  }

  private void replayEntriesLocked(Checkpoint.Snapshot snap, Set<Integer> evicted) {
    // Per-region sum of currently-live bytes; later subtracted from the watermark to recover
    // erased gaps left by TTL-driven removeFileEntries calls between checkpoints.
    long[] liveBytesPerRegion = new long[numRegions];
    for (Checkpoint.Entry e : snap.entries()) {
      SsdRun run = new SsdRun(e.fileBits(), e.checksum());
      int region = (int) (run.offset() / REGION_SIZE);
      if (region < numRegions && !evicted.contains(region)) {
        entries.put(new RawFileCacheKey(e.fileNum(), e.offset()), run);
        // Watermark, NOT sum — partial mid-region erases must keep the original write head so
        // new writes never collide with bytes the previous run already used.
        long endInRegion = (run.offset() % REGION_SIZE) + run.size();
        if (endInRegion > regionSizes[region]) {
          regionSizes[region] = endInRegion;
        }
        liveBytesPerRegion[region] += run.size();
      }
    }
    for (int r = 0; r < numRegions; r++) {
      regionErased[r] = regionSizes[r] - liveBytesPerRegion[r];
    }
  }

  /** Looks up an SSD entry. Returns an empty pin on miss. */
  public SsdPin find(long fileNum, long offset) {
    var key = new RawFileCacheKey(fileNum, offset);
    lock.lock();
    try {
      // Velox SsdFile.cpp:169-171: while suspended (no eviction candidate available), refuse
      // new pins so callers fall back to storage and pinned regions can drain. Without this,
      // new pins accumulate against the very regions blocking eviction.
      if (suspended) {
        return SsdPin.empty();
      }
      tracker.fileTouched(entries.size());
      SsdRun run = entries.get(key);
      if (run == null) {
        return SsdPin.empty();
      }
      int region = (int) (run.offset() / REGION_SIZE);
      regionPins[region]++;
      return new SsdPin(this, region, run);
    } finally {
      lock.unlock();
    }
  }

  /** Releases one pin on {@code regionIndex}. */
  void unpinRegion(int regionIndex) {
    lock.lock();
    try {
      regionPins[regionIndex]--;
      if (suspended && regionPins[regionIndex] == 0) {
        // Velox SsdFile.cpp:155-162 — a previous write found no eviction candidates because
        // every region was pinned. Now that this region is unpinned, recover space immediately
        // so a concurrent retry can succeed without waiting for the next write() entry.
        try {
          growOrEvictLocked();
        } catch (IOException ignored) {
          // best-effort; next write() will retry and surface a fresh error if any
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /** Returns the number of cached entries. */
  public int size() {
    lock.lock();
    try {
      return entries.size();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the current shard {@link State}. Lock-free; reflects the most recent transition. The
   * value is advisory — concurrent writers may transition to {@link State#NO_SPACE} immediately
   * after the call returns, and a {@link #write(RawFileCacheKey, ByteBuffer)} call that returns
   * {@link Optional#empty()} may itself have just triggered such a transition (re-check {@code
   * state()} afterward if the distinction matters). Critical decisions must rely on the value
   * returned by {@code write()} rather than on this accessor.
   */
  public State state() {
    return state;
  }

  /** Reads the bytes for a previously-found run. The pin must still be open. */
  public void load(SsdPin pin, ByteBuffer dst) throws IOException {
    if (pin.isEmpty()) {
      throw new IllegalStateException("load on empty pin");
    }
    SsdRun run = pin.run();
    if (run == null) {
      // The pin was closed concurrently between isEmpty() and run() — see SsdPin's
      // AtomicReference getAndSet(null) in close(). Violates the single-owner pin contract.
      throw new ClosedChannelException();
    }
    int region = pin.regionIndex();
    int size = run.size();
    if (dst.remaining() < size) {
      throw new IllegalArgumentException(
          "dst remaining=" + dst.remaining() + " < run size=" + size);
    }
    if (closed) {
      throw new ClosedChannelException();
    }
    // Register as in-flight BEFORE the second `closed` check so close() either (a) sees this
    // reader and waits for it, or (b) we observe closed=true and bail without touching the
    // channel. Pair the increment with a guaranteed decrement in finally.
    activeReads.incrementAndGet();
    try {
      if (closed) {
        throw new ClosedChannelException();
      }
      FileChannel ch = channel;
      if (ch == null) {
        throw new ClosedChannelException();
      }
      int dstStart = dst.position();
      int dstLimit = dst.limit();
      long offset = run.offset();
      try {
        dst.limit(dstStart + size);
        while (dst.hasRemaining()) {
          int n = ch.read(dst, offset + (dst.position() - dstStart));
          if (n < 0) {
            throw new IOException("Premature EOF reading SSD entry at offset=" + offset);
          }
        }
      } finally {
        // Always restore the caller's original limit, even if the read threw partway —
        // otherwise the buffer is left with a corrupted window and any subsequent reuse
        // silently truncates or trips IndexOutOfBoundsException.
        dst.limit(dstLimit);
      }
      if (checksumReadVerificationEnabled) {
        verifyChecksum(dst, dstStart, size, run.checksum(), offset);
      }
    } finally {
      activeReads.decrementAndGet();
    }
    // Tracker update happens outside the activeReads window because tracker is a plain
    // in-memory object — it cannot be torn down by close(). The region is still pinned by the
    // caller's SsdPin (regionPins[region] > 0 until the caller invokes pin.close()), so
    // findEvictionCandidates cannot select this region while we are scoring it — no risk of
    // updating the score of a recycled region.
    lock.lock();
    try {
      tracker.regionRead(region, size);
    } finally {
      lock.unlock();
    }
  }

  private void verifyChecksum(ByteBuffer dst, int start, int size, int expected, long offset)
      throws IOException {
    var crc = new CRC32C();
    var slice = dst.duplicate();
    slice.position(start).limit(start + size);
    crc.update(slice);
    int actual = (int) crc.getValue();
    if (actual != expected) {
      throw new IOException(
          "IOERR: SSD checksum mismatch at offset="
              + offset
              + " expected="
              + expected
              + " actual="
              + actual);
    }
  }

  /**
   * Writes one entry's bytes to the SSD. Returns the {@link SsdRun} that was stored, or {@link
   * Optional#empty()} when no space can be made for the entry.
   *
   * <p>Empty is returned on any of:
   *
   * <ul>
   *   <li>shard is in {@link State#NO_SPACE} (terminal write-disable, set previously by a
   *       data-write ENOSPC);
   *   <li>{@code maxEntries} cap configured and would be exceeded by this write;
   *   <li>no writable region exists, growth fails (including ENOSPC), and no eviction candidate is
   *       unpinned. The shard stays {@link State#ACTIVE} so a future call may succeed if disk space
   *       is freed externally;
   *   <li>the data-write to the SSD region reports {@code ENOSPC}, in which case the shard
   *       transitions to {@link State#NO_SPACE} (terminal write-disable).
   * </ul>
   *
   * <p>Other I/O errors propagate as {@link IOException}; the cache stays usable.
   *
   * <p>Caller must NOT mutate {@code data} until this returns.
   */
  public Optional<SsdRun> write(RawFileCacheKey key, ByteBuffer data) throws IOException {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(data, "data");
    int size = data.remaining();
    if (size <= 0 || size > SsdRun.MAX_SIZE) {
      throw new IllegalArgumentException("size out of range: " + size);
    }
    int checksum = config.checksumEnabled ? computeChecksum(data) : 0;
    lock.lock();
    try {
      if (state == State.NO_SPACE) {
        return Optional.empty();
      }
      // Velox SsdFile.cpp:376 — reject when adding this entry would reach or exceed the cap.
      // Earlier code used `entries.size() + 1 > maxEntries`, equivalent to `size >= max`, which
      // admits one entry beyond the limit (size grows to maxEntries before next write rejects).
      if (config.maxEntries > 0 && entries.size() + 1 >= config.maxEntries) {
        return Optional.empty();
      }
      Long offset = getSpaceLocked(size);
      if (offset == null) {
        return Optional.empty();
      }
      if (!writeBytesAtLocked(offset, data, size)) {
        // Out-of-space — state already flipped to NO_SPACE inside the helper.
        return Optional.empty();
      }
      SsdRun run = SsdRun.of(offset, size, checksum);
      int region = (int) (offset / REGION_SIZE);
      entries.put(key, run);
      regionSizes[region] += size;
      bytesSinceCheckpoint += size;
      maybeCheckpointLocked();
      return Optional.of(run);
    } finally {
      lock.unlock();
    }
  }

  private static int computeChecksum(ByteBuffer data) {
    CRC32C crc = new CRC32C();
    crc.update(data.duplicate());
    return (int) crc.getValue();
  }

  /**
   * Writes {@code size} bytes from {@code data} at absolute file offset {@code fileOff}. Returns
   * {@code true} on success, {@code false} only if the underlying channel reports ENOSPC (in which
   * case the shard is transitioned to {@link State#NO_SPACE}). Other I/O errors propagate.
   */
  private boolean writeBytesAtLocked(long fileOff, ByteBuffer data, int size) throws IOException {
    try {
      ByteBuffer slice = data.duplicate();
      while (slice.hasRemaining()) {
        int written = channel.write(slice, fileOff + (size - slice.remaining()));
        if (written < 0) {
          throw new IOException("Premature EOF writing SSD entry");
        }
      }
      return true;
    } catch (IOException ex) {
      // Match velox kNoSpace semantics: only flip to NO_SPACE on genuine "out of space"
      // failures. Other IOExceptions are surfaced to the caller; the cache stays usable.
      if (isOutOfSpace(ex)) {
        state = State.NO_SPACE;
        return false;
      }
      throw ex;
    }
  }

  /**
   * Returns the absolute file offset for an entry of {@code size} bytes, growing or evicting if the
   * head writable region has insufficient remaining space. Returns {@code null} if no space can be
   * made.
   */
  private Long getSpaceLocked(int size) throws IOException {
    while (true) {
      if (writableRegions.isEmpty()) {
        if (!growOrEvictLocked()) {
          return null;
        }
      }
      int head = writableRegions.peekFirst();
      long offsetInRegion = regionSizes[head];
      if (offsetInRegion + size > REGION_SIZE) {
        // Region full — pop and roll to next.
        writableRegions.pollFirst();
        tracker.regionFilled(head);
        continue;
      }
      return (long) head * REGION_SIZE + offsetInRegion;
    }
  }

  private boolean growOrEvictLocked() throws IOException {
    if (numRegions < config.maxRegions) {
      try {
        growOneRegionLocked();
        suspended = false;
        return true;
      } catch (IOException ex) {
        if (!isOutOfSpace(ex)) {
          throw ex;
        }
        // ENOSPC during grow — fall through to the eviction path so we can still recycle
        // existing region bytes (matches velox SsdFile.cpp:308-313 growFileErrors handling).
      }
    }
    // Velox SsdFile.cpp:316-325 — if the disk is already known to be out of space, the
    // eviction log append cannot succeed either; skip eviction to avoid an inconsistent
    // index update with no durable log record of the eviction.
    if (state == State.NO_SPACE) {
      return false;
    }
    List<Integer> candidates = tracker.findEvictionCandidates(numRegions, regionPins);
    if (candidates.isEmpty()) {
      suspended = true;
      return false;
    }
    evictRegionsLocked(candidates);
    suspended = false;
    return true;
  }

  /** Returns true if the IOException represents an "out of space" condition. */
  static boolean isOutOfSpace(IOException ex) {
    String msg = ex.getMessage();
    return msg != null && msg.toLowerCase(Locale.ROOT).contains("no space");
  }

  private void growOneRegionLocked() throws IOException {
    long newSize = (long) (numRegions + 1) * REGION_SIZE;
    // Lazy growth — extend the file by one region using the existing channel. Writing one
    // zero byte at the last position creates a sparse hole on POSIX filesystems. ENOSPC and
    // other I/O errors propagate; growOrEvictLocked decides whether to fall through to
    // eviction (ENOSPC) or rethrow (other errors).
    if (channel.size() < newSize) {
      ByteBuffer one = ByteBuffer.allocate(1);
      one.put((byte) 0).flip();
      channel.write(one, newSize - 1);
    }
    int idx = numRegions++;
    writableRegions.addLast(idx);
  }

  private void evictRegionsLocked(List<Integer> regions) throws IOException {
    if (checkpointEnabled()) {
      // Synchronous append to log BEFORE region bytes are reused.
      Checkpoint.appendEvictions(config.logPath, toIntArray(regions));
    }
    // Drop entries whose region is in `regions`.
    Set<Integer> regSet = new HashSet<>(regions);
    entries.entrySet().removeIf(e -> regSet.contains((int) (e.getValue().offset() / REGION_SIZE)));
    for (int r : regions) {
      regionSizes[r] = 0;
      regionErased[r] = 0;
      tracker.resetRegion(r);
      // Precondition: regions returned by findEvictionCandidates are unpinned and were
      // never in writableRegions (full regions are popped before eviction).
      writableRegions.addLast(r);
    }
  }

  /** Removes all entries belonging to {@code filesToRemove}. Returns retained file ids. */
  public Set<Long> removeFileEntries(Set<Long> filesToRemove) {
    Objects.requireNonNull(filesToRemove, "filesToRemove");
    // Defensive copy — caller is free to mutate after the call returns.
    Set<Long> targets = Set.copyOf(filesToRemove);
    Set<Long> retained = new HashSet<>();
    lock.lock();
    try {
      removeMatchingEntriesLocked(targets, retained);
      compactErodedRegionsLocked();
    } finally {
      lock.unlock();
    }
    return retained;
  }

  private void removeMatchingEntriesLocked(Set<Long> targets, Set<Long> retained) {
    var it = entries.entrySet().iterator();
    while (it.hasNext()) {
      var e = it.next();
      long fn = e.getKey().fileNum();
      if (!targets.contains(fn)) {
        continue;
      }
      int r = (int) (e.getValue().offset() / REGION_SIZE);
      if (regionPins[r] > 0) {
        retained.add(fn);
        continue;
      }
      regionErased[r] += e.getValue().size();
      it.remove();
    }
  }

  private void compactErodedRegionsLocked() {
    // Regions whose erased > MAX_ERASED_SIZE_PCT% of their watermark become eligible for
    // full compaction. Collect first so the eviction log is appended BEFORE region bytes
    // are reused (matches velox SsdFile.cpp:639-654).
    List<Integer> toFree = new ArrayList<>();
    for (int r = 0; r < numRegions; r++) {
      if (regionPins[r] == 0
          && regionSizes[r] > 0
          && regionErased[r] > regionSizes[r] * MAX_ERASED_SIZE_PCT / PERCENT_DENOM) {
        toFree.add(r);
      }
    }
    if (toFree.isEmpty()) {
      return;
    }
    if (checkpointEnabled()) {
      try {
        Checkpoint.appendEvictions(config.logPath, toIntArray(toFree));
      } catch (IOException ex) {
        // INTENTIONAL deviation from velox: velox SsdFile.cpp:639-654 still clears and
        // reuses regions even when logEviction (lines 667-679) swallowed an error — that
        // path can corrupt subsequent reads if the process crashes before the next
        // checkpoint, because the on-disk checkpoint still references the now-overwritten
        // bytes. We choose the safer path: per-entry erasures from the prior loop stay,
        // but the over-threshold regions are NOT cleared/reused; the next removeFileEntries
        // call will retry. Trade-off is delayed compaction in exchange for crash safety.
        return;
      }
    }
    Set<Integer> freedSet = new HashSet<>(toFree);
    entries
        .entrySet()
        .removeIf(e -> freedSet.contains((int) (e.getValue().offset() / REGION_SIZE)));
    for (int r : toFree) {
      regionSizes[r] = 0;
      regionErased[r] = 0;
      tracker.resetRegion(r);
      if (!writableRegions.contains(r)) {
        writableRegions.addLast(r);
      }
    }
  }

  private static int[] toIntArray(List<Integer> list) {
    int[] arr = new int[list.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = list.get(i);
    }
    return arr;
  }

  /** Forces a checkpoint regardless of {@link Config#checkpointIntervalBytes}. */
  public void checkpoint() throws IOException {
    lock.lock();
    try {
      checkpointLocked();
    } finally {
      lock.unlock();
    }
  }

  /** Best-effort auto-checkpoint triggered by the write path; never propagates exceptions. */
  private void maybeCheckpointLocked() {
    if (!checkpointEnabled() || bytesSinceCheckpoint < checkpointIntervalBytes) {
      return;
    }
    try {
      checkpointLocked();
    } catch (IOException ignored) {
      // checkpointLocked already disabled checkpointing on failure; the cache continues.
    }
  }

  private void checkpointLocked() throws IOException {
    if (!checkpointEnabled() || state == State.NO_SPACE) {
      return;
    }
    try {
      // Flush data file BEFORE writing the checkpoint end marker.
      channel.force(false);
      // Snapshot file ids for entries currently in cache.
      Map<Long, String> fileIdToName = new HashMap<>();
      List<Checkpoint.Entry> snapEntries = new ArrayList<>(entries.size());
      for (var e : entries.entrySet()) {
        long fn = e.getKey().fileNum();
        String name = fileIds.stringForId(fn);
        if (name != null) fileIdToName.put(fn, name);
        snapEntries.add(
            new Checkpoint.Entry(
                fn, e.getKey().offset(), e.getValue().fileBits(), e.getValue().checksum()));
      }
      // Use snapshotScores() — production accessor, NOT testingScores(). Snapshot at full
      // maxRegions length so the on-disk record matches velox SsdFile.cpp:848.
      double[] scores = tracker.snapshotScores();
      var snap =
          new Checkpoint.Snapshot(config.maxRegions, numRegions, scores, fileIdToName, snapEntries);
      Checkpoint.write(
          config.checkpointPath, config.checkpointTmpPath, snap, config.checksumEnabled);
      // Truncate eviction log AFTER checkpoint succeeds — recovery now reflects the new state.
      Checkpoint.truncateLog(config.logPath);
      bytesSinceCheckpoint = 0;
    } catch (IOException ex) {
      // Disable checkpointing to keep the cache running (matches velox checkpointError).
      // DO NOT delete checkpointPath or logPath — if Checkpoint.write already atomically
      // renamed a valid .cpt into place and the failure came from truncateLog, deleting the
      // .cpt would force a cold cold-start on next process boot and silently lose work. Velox
      // takes the same approach: log, disable, leave files alone. Only the staging tmp file
      // is cleaned up.
      checkpointIntervalBytes = 0;
      try {
        Files.deleteIfExists(config.checkpointTmpPath);
      } catch (IOException cleanupEx) {
        ex.addSuppressed(cleanupEx);
      }
      throw ex;
    }
  }

  @Override
  public void close() throws IOException {
    FileChannel toClose;
    IOException checkpointEx = null;
    lock.lock();
    try {
      // Double-close is a no-op: channel == null indicates a prior close already flushed
      // and released. Skipping the checkpoint avoids NPE in checkpointLocked's channel.force.
      if (channel == null) {
        return;
      }
      // Mark closed BEFORE the final checkpoint so any concurrent load() that arrives during
      // the checkpoint bails out cleanly instead of starting a read against a channel that is
      // about to be closed.
      closed = true;
      try {
        if (checkpointEnabled() && state == State.ACTIVE) {
          checkpointLocked();
        }
      } catch (IOException ex) {
        // Defer the throw until the channel has been closed and the lock released so we don't
        // leak the FD when the checkpoint fails.
        checkpointEx = ex;
      }
      toClose = channel;
      channel = null;
    } finally {
      lock.unlock();
    }
    // Drain in-flight readers OUTSIDE the shard lock. Holding the lock here would deadlock
    // any reader that needs the shard lock for its post-read tracker update.
    //
    // Bounded by DRAIN_TIMEOUT_NANOS so a hung underlying read (e.g., stalled NFS mount) cannot
    // pin the shutdown thread forever. On expiry we proceed to close the channel anyway —
    // in-flight readers will then surface AsynchronousCloseException / ClosedChannelException,
    // which is the correct shutdown-time signal that the cache is gone.
    long deadline = System.nanoTime() + DRAIN_TIMEOUT_NANOS;
    while (activeReads.get() > 0) {
      if (System.nanoTime() - deadline >= 0) {
        break;
      }
      LockSupport.parkNanos(1_000_000L);
    }
    try {
      toClose.close();
    } catch (IOException ex) {
      if (checkpointEx != null) {
        checkpointEx.addSuppressed(ex);
      } else {
        checkpointEx = ex;
      }
    }
    if (checkpointEx != null) {
      throw checkpointEx;
    }
  }

  // --- test helpers -----------------------------------------------------------

  int testingNumRegions() {
    lock.lock();
    try {
      return numRegions;
    } finally {
      lock.unlock();
    }
  }

  int testingRegionPins(int r) {
    lock.lock();
    try {
      return regionPins[r];
    } finally {
      lock.unlock();
    }
  }

  /**
   * Test-only: forcibly transition the shard's state (e.g. simulate ENOSPC). Production code must
   * NOT call this — concurrent writers will observe the change immediately and may behave
   * inconsistently with the on-disk state.
   */
  void testingForceState(State newState) {
    lock.lock();
    try {
      this.state = newState;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Test-only: replace the data file channel with a fault-injecting wrapper. The previous channel
   * is closed by the caller — this helper does not close it. Used to exercise ENOSPC handling on
   * {@link #write(RawFileCacheKey, ByteBuffer)} and the grow ENOSPC fall-through to eviction.
   */
  void testingReplaceChannel(FileChannel newChannel) {
    lock.lock();
    try {
      this.channel = newChannel;
    } finally {
      lock.unlock();
    }
  }
}
