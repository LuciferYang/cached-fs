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
package io.github.luciferyang.cachedfs.core.id;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Process-wide bidirectional map between strings (typically file paths) and dense {@code long} ids
 * with per-id reference counts. Mirrors velox's {@code StringIdMap} (StringIdMap.h:27-105).
 *
 * <p>Reference counting is exposed via {@link StringIdLease} (RAII). As long as any cache entry,
 * SSD entry, or in-flight load holds a lease, the path↔id mapping is stable. {@link #recoverId} is
 * what allows the SSD checkpoint to replay original id assignments after a process restart.
 *
 * <p>Ids are allocated densely from {@code 1} upward (zero is reserved as a "no-id" sentinel),
 * which is what makes the SSD checkpoint sentinels {@code 0xfffffffffffffffe} / {@code 0xcbedf11e}
 * unambiguous (see velox §4.6).
 */
public final class StringIdMap {

  private final ReentrantLock lock = new ReentrantLock();
  private final Map<String, Entry> stringToEntry = new HashMap<>();
  private final Map<Long, Entry> idToEntry = new HashMap<>();
  private long lastId = 0;

  /** Visible for tests. */
  static final class Entry {
    final long id;
    final String name;
    int refs;

    Entry(long id, String name) {
      this.id = id;
      this.name = name;
      this.refs = 0;
    }
  }

  /**
   * Returns the existing id for {@code name} or mints a new one. Always increments the reference
   * count by one; the caller owns one reference.
   */
  public long makeId(String name) {
    Objects.requireNonNull(name, "name");
    lock.lock();
    try {
      Entry entry = stringToEntry.get(name);
      if (entry == null) {
        entry = new Entry(++lastId, name);
        stringToEntry.put(name, entry);
        idToEntry.put(entry.id, entry);
      }
      entry.refs++;
      return entry.id;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Replays an explicit {@code (id, name)} pair from durable storage (e.g. an SSD checkpoint).
   * Reuses an existing entry when the names match; throws if the id is already bound to a different
   * name. Increments the reference count by one. {@code id} must be positive.
   */
  public long recoverId(long id, String name) {
    if (id <= 0) {
      throw new IllegalArgumentException("id must be > 0");
    }
    Objects.requireNonNull(name, "name");
    lock.lock();
    try {
      Entry existing = idToEntry.get(id);
      if (existing != null) {
        if (!existing.name.equals(name)) {
          throw new IllegalStateException(
              "Recovered id " + id + " conflicts: was '" + existing.name + "', now '" + name + "'");
        }
        existing.refs++;
        return existing.id;
      }
      Entry byName = stringToEntry.get(name);
      if (byName != null) {
        throw new IllegalStateException(
            "Recovered name '" + name + "' already has id " + byName.id + ", asked for " + id);
      }
      Entry entry = new Entry(id, name);
      stringToEntry.put(name, entry);
      idToEntry.put(id, entry);
      entry.refs++;
      if (id > lastId) {
        lastId = id;
      }
      return id;
    } finally {
      lock.unlock();
    }
  }

  /** Adds another reference to an existing id. */
  public void addReference(long id) {
    lock.lock();
    try {
      Entry entry = idToEntry.get(id);
      if (entry == null) {
        throw new IllegalStateException("Unknown id: " + id);
      }
      entry.refs++;
    } finally {
      lock.unlock();
    }
  }

  /** Drops one reference; removes the entry when the count hits zero. */
  public void release(long id) {
    lock.lock();
    try {
      Entry entry = idToEntry.get(id);
      if (entry == null) {
        return;
      }
      if (--entry.refs <= 0) {
        idToEntry.remove(id);
        stringToEntry.remove(entry.name);
      }
    } finally {
      lock.unlock();
    }
  }

  /** Returns the string for {@code id}, or {@code null} if not bound. */
  public String stringForId(long id) {
    lock.lock();
    try {
      Entry entry = idToEntry.get(id);
      return entry == null ? null : entry.name;
    } finally {
      lock.unlock();
    }
  }

  /** Returns the count of currently bound ids. */
  public int size() {
    lock.lock();
    try {
      return idToEntry.size();
    } finally {
      lock.unlock();
    }
  }

  /** Visible for tests; the highest id ever issued (does not decrement on release). */
  long highWaterMark() {
    lock.lock();
    try {
      return lastId;
    } finally {
      lock.unlock();
    }
  }
}
