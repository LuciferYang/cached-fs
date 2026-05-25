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

import io.github.luciferyang.cachedfs.core.id.StringIdMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-shard SSD tier. Mirrors velox {@code SsdCache} with explicit design simplifications for the
 * Java port's reactive driver model (no executor pool, no async save-back).
 *
 * <p>Each {@link SsdFile} shard owns a single regular file. Routing: {@code Math.floorMod(fileNum,
 * numShards)} — all of one file's pages live in a single SSD shard, which is what makes coalesced
 * reads from one file efficient (velox §4.1).
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls. Per-shard mutexes
 * serialize within a shard; cross-shard operations have no global ordering.
 *
 * <p><b>Velox-parity divergences:</b>
 *
 * <ul>
 *   <li>{@link #removeFileEntries} has no equivalent of velox's {@code startWrite()} cache-level
 *       write-lock. Velox serializes save-back writes against TTL-driven removal; the Java port
 *       relies solely on per-shard {@link SsdFile} mutexes. A TTL pass can interleave with a
 *       save-back batch at the cache level (but not within a single shard).
 *   <li>{@link #checkpoint()} runs synchronously and has no {@code writesInProgress} precondition.
 *       Velox's checkpoint requires the caller hold every shard's write-token and schedules
 *       per-shard work onto an executor as fire-and-forget. The Java caller blocks until every
 *       shard fsyncs.
 *   <li>{@link #removeFileEntries} per-shard fail-soft: if a shard throws, its targets surface as
 *       retained and remaining shards continue. Matches the symmetric behavior on the RAM tier; see
 *       {@link io.github.luciferyang.cachedfs.core.AsyncDataCache#removeFileEntries}.
 *   <li>{@link #close} flushes a final checkpoint per shard before closing (velox parity for
 *       durability). Checkpoint failures are recorded as suppressed; close still proceeds.
 *   <li>No {@code clear()} or {@code waitForWriteToFinish()}. Velox exposes both for test rigs and
 *       Prestissimo worker ops. Java-port embedders that need an SSD-tier reset call {@link #close}
 *       and reconstruct, or open shards directly for surgical operations.
 * </ul>
 */
public final class SsdCache implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(SsdCache.class);

  /**
   * SsdCache configuration.
   *
   * <p><b>Multi-directory support:</b> {@code directories} is a non-empty list of mount points;
   * shards are placed round-robin: shard {@code i} lives in {@code directories.get(i %
   * directories.size())}. {@code numShards} is independent of {@code directories.size()} — an
   * operator can have 8 shards across 2 disks (4/disk) or 1 shard per disk, etc. Typical
   * deployments on AWS i3en / i4i (2–8 local NVMe SSDs) want one entry per disk; failing to
   * configure all disks would silently waste capacity.
   *
   * <p><b>Note on {@code checkpointIntervalBytes}:</b> this is the <em>cache-wide</em> budget; each
   * shard auto-checkpoints after roughly {@code checkpointIntervalBytes / numShards} bytes have
   * been written to it (matches velox SsdCache.cpp:70). If the value is less than {@code numShards}
   * the per-shard interval floors to 0, which disables auto-checkpointing for every shard. A value
   * of {@code 0} also disables auto-checkpointing entirely. Explicit {@link SsdCache#checkpoint()}
   * always runs regardless of this setting.
   *
   * @param directories non-empty list of mount points; shards are placed round-robin
   * @param shardPrefix filename prefix for the per-shard data/log/checkpoint files; should not
   *     contain path separators or characters reserved by the host filesystem
   * @param numShards positive shard count, independent of {@code directories.size()}
   * @param regionsPerShard positive number of 64 MiB regions allocated per shard
   * @param maxEntriesPerShard upper bound on cached entries per shard; {@code 0} means unbounded
   * @param checkpointIntervalBytes see note above
   * @param checksumEnabled when true, payload checksums are computed on write and stored
   * @param checksumReadVerificationEnabled when true, payload checksums are verified on read
   *     (requires {@code checksumEnabled})
   */
  public record Config(
      List<Path> directories,
      String shardPrefix,
      int numShards,
      int regionsPerShard,
      int maxEntriesPerShard,
      long checkpointIntervalBytes,
      boolean checksumEnabled,
      boolean checksumReadVerificationEnabled) {
    public Config {
      Objects.requireNonNull(directories, "directories");
      Objects.requireNonNull(shardPrefix, "shardPrefix");
      // List.copyOf rejects nulls element-wise and gives us an immutable snapshot.
      directories = List.copyOf(directories);
      if (directories.isEmpty()) {
        throw new IllegalArgumentException("directories must be non-empty");
      }
      if (numShards <= 0) {
        throw new IllegalArgumentException("numShards must be > 0: " + numShards);
      }
      if (regionsPerShard <= 0) {
        throw new IllegalArgumentException("regionsPerShard must be > 0: " + regionsPerShard);
      }
      // Velox SsdCache.cpp:47-51 — read-verification requires checksum.
      if (checksumReadVerificationEnabled && !checksumEnabled) {
        throw new IllegalArgumentException(
            "checksumReadVerificationEnabled requires checksumEnabled");
      }
    }

    /**
     * Convenience for the common single-directory case. Wraps {@code directory} in a one-element
     * list and forwards the rest of the arguments to the canonical constructor.
     */
    public static Config single(
        Path directory,
        String shardPrefix,
        int numShards,
        int regionsPerShard,
        int maxEntriesPerShard,
        long checkpointIntervalBytes,
        boolean checksumEnabled,
        boolean checksumReadVerificationEnabled) {
      return new Config(
          List.of(directory),
          shardPrefix,
          numShards,
          regionsPerShard,
          maxEntriesPerShard,
          checkpointIntervalBytes,
          checksumEnabled,
          checksumReadVerificationEnabled);
    }
  }

  private final Config config;
  private final SsdFile[] shards;

  /** Constructs and opens the cache. Recovers existing shard files if present. */
  public SsdCache(Config config, StringIdMap fileIds) throws IOException {
    this.config = config;
    this.shards = new SsdFile[config.numShards];
    // Fail-fast: every configured directory must exist and be writable BEFORE any shard opens.
    // Skip-and-continue was rejected by design — a silently-degraded multi-SSD setup would
    // waste capacity on the missing disk and confuse operator capacity planning.
    for (Path d : config.directories) {
      if (!Files.isDirectory(d)) {
        throw new IOException("SsdCache directory does not exist or is not a directory: " + d);
      }
      if (!Files.isWritable(d)) {
        throw new IOException("SsdCache directory is not writable: " + d);
      }
    }
    boolean[] opened = new boolean[config.numShards];
    // Velox SsdCache.cpp:70 — caller's interval is the cache-wide target; each shard auto-
    // checkpoints after roughly 1/numShards of that many bytes have been written. Plain floor
    // division: if the interval is smaller than numShards, per-shard floors to 0 which
    // disables checkpointing for that shard (matches velox isCheckpointEnabled() semantics).
    long perShardInterval = config.checkpointIntervalBytes / config.numShards;
    int numDirs = config.directories.size();
    try {
      for (int i = 0; i < config.numShards; i++) {
        // Round-robin: shard i → directories[i % numDirs]. With numShards == numDirs every
        // shard gets its own disk; with numShards > numDirs, shards are striped evenly.
        Path dir = config.directories.get(i % numDirs);
        String name = config.shardPrefix + "-" + i;
        SsdFile.Config sc =
            new SsdFile.Config(
                dir.resolve(name + ".data"),
                dir.resolve(name + ".cpt"),
                dir.resolve(name + ".log"),
                dir.resolve(name + ".cpt.tmp"),
                config.regionsPerShard,
                config.maxEntriesPerShard,
                perShardInterval,
                config.checksumEnabled,
                config.checksumReadVerificationEnabled);
        SsdFile f = new SsdFile(sc, fileIds);
        f.open();
        shards[i] = f;
        opened[i] = true;
      }
    } catch (IOException ex) {
      // Roll back any successfully-opened shards before propagating.
      for (int i = 0; i < shards.length; i++) {
        if (opened[i] && shards[i] != null) {
          try {
            shards[i].close();
          } catch (IOException suppressed) {
            ex.addSuppressed(suppressed);
          }
        }
      }
      throw ex;
    }
  }

  public Config config() {
    return config;
  }

  public int numShards() {
    return shards.length;
  }

  /**
   * Returns the SSD shard for {@code fileNum}. Routing: {@code fileNum % numShards}. The returned
   * {@link SsdFile} is owned by this cache and closed when {@link #close()} is invoked — callers
   * must NOT close it independently.
   */
  public SsdFile shardFor(long fileNum) {
    int idx = (int) Math.floorMod(fileNum, shards.length);
    return shards[idx];
  }

  /**
   * Returns the {@code i}-th shard (0-based). Throws {@link IndexOutOfBoundsException} if out of
   * range. The returned {@link SsdFile} shares this cache's lifecycle — callers must NOT close it
   * independently.
   */
  public SsdFile shard(int i) {
    Objects.checkIndex(i, shards.length);
    return shards[i];
  }

  /**
   * Removes all entries belonging to {@code filesToRemove} from every shard. Per-shard exceptions
   * are caught and the target set is added to the retained result so the caller retries those files
   * next cycle; remaining shards continue. Matches {@link
   * io.github.luciferyang.cachedfs.core.AsyncDataCache#removeFileEntries} and velox's per-shard
   * fail-soft. Exceptions are logged but otherwise swallowed — operators should monitor logs for
   * shard-level removal failures.
   */
  public Set<Long> removeFileEntries(Set<Long> filesToRemove) {
    Objects.requireNonNull(filesToRemove, "filesToRemove");
    Set<Long> targets = Set.copyOf(filesToRemove);
    Set<Long> retained = new HashSet<>();
    for (int i = 0; i < shards.length; i++) {
      SsdFile s = shards[i];
      try {
        retained.addAll(s.removeFileEntries(targets));
      } catch (RuntimeException ex) {
        LOG.warn("SSD shard {} removeFileEntries failed; reporting all targets as retained", i, ex);
        retained.addAll(targets);
      }
    }
    return Set.copyOf(retained);
  }

  /** Forces a checkpoint on every shard. */
  public void checkpoint() throws IOException {
    IOException primary = null;
    for (SsdFile s : shards) {
      try {
        s.checkpoint();
      } catch (IOException ex) {
        if (primary == null) primary = ex;
        else primary.addSuppressed(ex);
      }
    }
    if (primary != null) throw primary;
  }

  /**
   * Persists state and closes every shard. Best-effort — collects exceptions as suppressed.
   *
   * <p>Each shard is asked to flush a final checkpoint before its file handle is closed (matches
   * velox {@code SsdCache::shutdown} which calls {@code file->checkpoint(true)} per shard). A
   * checkpoint failure on any shard does not abort the close path — the exception is recorded as
   * primary/suppressed and the remaining shards still get their {@code close()} call. Without this
   * final flush, entries written since the last auto-checkpoint would be lost on the next startup's
   * recovery.
   */
  @Override
  public void close() throws IOException {
    IOException primary = null;
    for (SsdFile s : shards) {
      try {
        s.checkpoint();
      } catch (IOException ex) {
        if (primary == null) primary = ex;
        else primary.addSuppressed(ex);
      }
      try {
        s.close();
      } catch (IOException ex) {
        if (primary == null) primary = ex;
        else primary.addSuppressed(ex);
      }
    }
    if (primary != null) throw primary;
  }
}
