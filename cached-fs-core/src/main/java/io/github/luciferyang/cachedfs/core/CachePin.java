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
package io.github.luciferyang.cachedfs.core;

/**
 * RAII pin over a {@link CacheEntry}. The pin keeps the entry from being evicted while it is held;
 * closing decrements the appropriate pin counter. Mirrors velox {@code CachePin}.
 *
 * <p>{@link #close()} is idempotent — calling it multiple times is safe and does not
 * double-decrement.
 *
 * <p><b>Thread safety:</b> a pin is owned by one logical caller. Hand-off between threads must be
 * explicit (e.g. via a synchronization barrier in the caller's code). {@link #entry()} is only
 * valid while the pin is open.
 */
public final class CachePin implements AutoCloseable {

  /** Empty pin singleton. {@link #close()} is a no-op. */
  private static final CachePin EMPTY = new CachePin(null, null, false);

  private final CacheShard shard;
  private CacheEntry entry;
  private final boolean exclusive;

  private CachePin(CacheShard shard, CacheEntry entry, boolean exclusive) {
    this.shard = shard;
    this.entry = entry;
    this.exclusive = exclusive;
  }

  static CachePin shared(CacheShard shard, CacheEntry entry) {
    return new CachePin(shard, entry, false);
  }

  static CachePin exclusive(CacheShard shard, CacheEntry entry) {
    return new CachePin(shard, entry, true);
  }

  /** Returns the singleton empty pin. {@link #close()} on it is a no-op. */
  public static CachePin empty() {
    return EMPTY;
  }

  /** True if this pin holds no entry. Check before {@link #isExclusive()}/{@link #isShared()}. */
  public boolean isEmpty() {
    return entry == null;
  }

  public boolean isExclusive() {
    return entry != null && exclusive;
  }

  public boolean isShared() {
    return entry != null && !exclusive;
  }

  /** Returns the underlying entry. Only valid while this pin is open. */
  public CacheEntry entry() {
    return entry;
  }

  /**
   * Transitions an exclusive pin to a shared one, completing any waiters. Must be called by the
   * thread that owns the pin. After this call this exclusive pin is detached (subsequent {@link
   * #close()} is a no-op) and a new shared pin is returned.
   *
   * @param ssdSavable whether this entry should be marked SSD-saveable (Phase 2 hookup)
   * @return a new shared pin replacing this exclusive pin
   * @throws IllegalStateException if this pin is not exclusive
   */
  public CachePin exclusiveToShared(boolean ssdSavable) {
    if (!isExclusive()) {
      throw new IllegalStateException("exclusiveToShared: not exclusive");
    }
    CacheEntry e = entry;
    // Defer the detach until completeExclusive returns successfully. If it throws, leave `entry`
    // attached so a try-with-resources `close()` routes through releaseFailedExclusive — preventing
    // an orphaned EXCLUSIVE entry from deadlocking all future findOrCreate waiters on its key.
    shard.completeExclusive(e, ssdSavable);
    this.entry = null;
    return shared(shard, e);
  }

  /**
   * Releases the pin. Idempotent — subsequent calls are no-ops. Closing an empty pin is a no-op.
   */
  @Override
  public void close() {
    if (entry == null) {
      return;
    }
    CacheEntry e = entry;
    entry = null;
    if (exclusive) {
      shard.releaseFailedExclusive(e);
    } else {
      shard.releaseShared(e);
    }
  }
}
