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
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Multi-shard SSD tier. Mirrors velox {@code SsdCache}.
 *
 * <p>Each {@link SsdFile} shard owns a single regular file. Routing: {@code files[fileNum %
 * numShards]} — all of one file's pages live in a single SSD shard, which is what makes coalesced
 * reads from one file efficient (velox §4.1).
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls. Per-shard mutexes
 * serialize within a shard; cross-shard operations have no global ordering.
 */
public final class SsdCache implements AutoCloseable {

  /**
   * SsdCache configuration.
   *
   * <p><b>Note on {@code checkpointIntervalBytes}:</b> this is the <em>cache-wide</em> budget; each
   * shard auto-checkpoints after roughly {@code checkpointIntervalBytes / numShards} bytes have
   * been written to it (matches velox SsdCache.cpp:70). If the value is less than {@code numShards}
   * the per-shard interval floors to 0, which disables auto-checkpointing for every shard. A value
   * of {@code 0} also disables auto-checkpointing entirely. Explicit {@link SsdCache#checkpoint()}
   * always runs regardless of this setting.
   */
  public record Config(
      Path directory,
      String shardPrefix,
      int numShards,
      int regionsPerShard,
      int maxEntriesPerShard,
      long checkpointIntervalBytes,
      boolean checksumEnabled,
      boolean checksumReadVerificationEnabled) {
    public Config {
      Objects.requireNonNull(directory, "directory");
      Objects.requireNonNull(shardPrefix, "shardPrefix");
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
  }

  private final Config config;
  private final SsdFile[] shards;

  /** Constructs and opens the cache. Recovers existing shard files if present. */
  public SsdCache(Config config, StringIdMap fileIds) throws IOException {
    this.config = config;
    this.shards = new SsdFile[config.numShards];
    boolean[] opened = new boolean[config.numShards];
    // Velox SsdCache.cpp:70 — caller's interval is the cache-wide target; each shard auto-
    // checkpoints after roughly 1/numShards of that many bytes have been written. Plain floor
    // division: if the interval is smaller than numShards, per-shard floors to 0 which
    // disables checkpointing for that shard (matches velox isCheckpointEnabled() semantics).
    long perShardInterval = config.checkpointIntervalBytes / config.numShards;
    try {
      for (int i = 0; i < config.numShards; i++) {
        String name = config.shardPrefix + "-" + i;
        SsdFile.Config sc =
            new SsdFile.Config(
                config.directory.resolve(name + ".data"),
                config.directory.resolve(name + ".cpt"),
                config.directory.resolve(name + ".log"),
                config.directory.resolve(name + ".cpt.tmp"),
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
          } catch (IOException ignored) {
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

  /** Removes all entries belonging to {@code filesToRemove} from every shard. */
  public Set<Long> removeFileEntries(Set<Long> filesToRemove) {
    Objects.requireNonNull(filesToRemove, "filesToRemove");
    // Defensive copy at the public boundary — shards iterate this set under their own lock.
    Set<Long> targets = Set.copyOf(filesToRemove);
    Set<Long> retained = new HashSet<>();
    for (SsdFile s : shards) {
      retained.addAll(s.removeFileEntries(targets));
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

  /** Closes every shard. Best-effort — collects exceptions as suppressed. */
  @Override
  public void close() throws IOException {
    IOException primary = null;
    for (SsdFile s : shards) {
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
