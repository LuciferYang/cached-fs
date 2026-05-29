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
package io.github.luciferyang.cachedfs.core.handle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Single-flight value factory backed by a count-based LRU cache. Mirrors velox {@code CachedFactory
 * + SimpleLRUCache}.
 *
 * <p>Concurrency model:
 *
 * <ul>
 *   <li>The first thread asking for a missing key inserts it into a {@code pending} set and runs
 *       the generator outside the cache lock.
 *   <li>Concurrent askers wait on a condition variable and pick up the freshly inserted entry on
 *       wake — no duplicate generation.
 *   <li>{@link CachedPtr} is RAII; closing the last pointer for a key allows it to be evicted by
 *       the LRU policy.
 * </ul>
 *
 * <p><b>LRU semantics:</b> {@link #generate} promotes the looked-up entry to most-recently-used.
 * {@link CachedPtr#close} (release) does NOT promote — matches velox's {@code SimpleLRUCache}.
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls.
 *
 * @param <K> cache key type (must have stable {@code equals}/{@code hashCode})
 * @param <V> cached value type
 */
public final class CachedFactory<K, V> {

  /**
   * RAII handle. {@link #close()} releases one pin on the underlying entry. Idempotent.
   *
   * <p>Static nested generic — holds an explicit reference to its owning factory, eliminating the
   * implicit-outer-class reference of a non-static inner class and giving cleaner generic spelling:
   * {@code CachedFactory.CachedPtr<K, V>}.
   */
  public static final class CachedPtr<K, V> implements AutoCloseable {
    private final CachedFactory<K, V> owner;
    private K key;
    private final V value;

    CachedPtr(CachedFactory<K, V> owner, K key, V value) {
      this.owner = owner;
      this.key = key;
      this.value = value;
    }

    /** Returns the underlying value. Must not be called after {@link #close()}. */
    public V value() {
      if (key == null) {
        throw new IllegalStateException("CachedPtr is closed");
      }
      return value;
    }

    /** True if this handle still holds a pin (i.e. not yet closed). */
    public boolean isOpen() {
      return key != null;
    }

    @Override
    public void close() {
      if (key != null) {
        owner.release(key);
        key = null;
      }
    }
  }

  private static final class Entry<V> {
    final V value;
    int pins;

    Entry(V value) {
      this.value = value;
      this.pins = 1;
    }
  }

  private final int capacity;
  private final Function<K, V> generator;

  /** Non-reordering map for {@code release()} lookups (never touches LRU order). */
  private final Map<K, Entry<V>> index = new HashMap<>();

  /** Insertion-ordered LRU; tail = most recently used. Reordered only by {@code generate}. */
  private final LinkedHashMap<K, Entry<V>> lru = new LinkedHashMap<>(16, 0.75f, true);

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition pendingCv = lock.newCondition();
  private final Map<K, Boolean> pending = new HashMap<>();

  public CachedFactory(int capacity, Function<K, V> generator) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be > 0: " + capacity);
    }
    this.capacity = capacity;
    this.generator = Objects.requireNonNull(generator, "generator");
  }

  /**
   * Returns a {@link CachedPtr} for {@code key}, generating the value if needed. Single-flight: at
   * most one generator call per key is in flight at a time. Promotes the entry to MRU.
   */
  public CachedPtr<K, V> generate(K key) {
    Objects.requireNonNull(key, "key");

    lock.lock();
    try {
      while (true) {
        Entry<V> existing = lru.get(key); // promotes to MRU
        if (existing != null) {
          existing.pins++;
          return new CachedPtr<>(this, key, existing.value);
        }
        if (pending.containsKey(key)) {
          try {
            pendingCv.await();
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for cached value", ex);
          }
          continue;
        }
        pending.put(key, Boolean.TRUE);
        break;
      }
    } finally {
      lock.unlock();
    }

    V generated;
    try {
      generated = generator.apply(key);
    } catch (RuntimeException | Error ex) {
      // Errors (e.g., OutOfMemoryError) must clear the `pending` entry and wake waiters too —
      // otherwise every thread blocked on `pendingCv` for this key deadlocks forever, since
      // no future signalAll() will fire for a key that stays in `pending`.
      lock.lock();
      try {
        pending.remove(key);
        pendingCv.signalAll();
      } finally {
        lock.unlock();
      }
      throw ex;
    }

    lock.lock();
    boolean pendingCleared = false;
    try {
      Entry<V> entry = new Entry<>(generated);
      lru.put(key, entry);
      index.put(key, entry);
      pending.remove(key);
      pendingCv.signalAll();
      pendingCleared = true;
      evictIfNeededLocked();
      return new CachedPtr<>(this, key, generated);
    } finally {
      if (!pendingCleared) {
        // HashMap.put or similar may throw (OOM during resize, etc.) before we cleared
        // `pending`. Without this cleanup the key would stay there forever, deadlocking
        // every thread waiting on pendingCv. The generated value is unrecoverable here
        // (V is not statically AutoCloseable), so it leaks until GC; the deadlock is the
        // critical thing to prevent.
        pending.remove(key);
        pendingCv.signalAll();
      }
      lock.unlock();
    }
  }

  private void release(K key) {
    lock.lock();
    try {
      // Use the non-reordering index map — release MUST NOT promote the entry to MRU.
      Entry<V> entry = index.get(key);
      if (entry == null) {
        return;
      }
      if (--entry.pins <= 0) {
        evictIfNeededLocked();
      }
    } finally {
      lock.unlock();
    }
  }

  private void evictIfNeededLocked() {
    if (lru.size() <= capacity) {
      return;
    }
    var it = lru.entrySet().iterator();
    while (lru.size() > capacity && it.hasNext()) {
      Map.Entry<K, Entry<V>> e = it.next();
      if (e.getValue().pins == 0) {
        it.remove();
        index.remove(e.getKey());
      }
    }
  }

  /** Visible for tests. */
  public int size() {
    lock.lock();
    try {
      return lru.size();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns {@code true} if {@code key} is currently cached. Read-only — does not bump LRU order or
   * pin the entry. Used by ops tooling (the JMX inspect endpoint) to ask "is this file open right
   * now?" without side effects.
   */
  public boolean containsKey(K key) {
    Objects.requireNonNull(key, "key");
    lock.lock();
    try {
      return index.containsKey(key);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns a snapshot of the currently-cached keys. Read-only — does not bump LRU order. Used by
   * the JMX purge endpoint to enumerate candidates before applying the regex match. The returned
   * list is a copy; callers may mutate it without affecting the cache.
   */
  public List<K> keys() {
    lock.lock();
    try {
      return new ArrayList<>(lru.keySet());
    } finally {
      lock.unlock();
    }
  }

  /**
   * Drains every entry (pinned or not) and returns the values so the caller can close them. Used
   * when the owning component is shutting down and must release every cached resource regardless of
   * pin state. In normal operation, pinned entries are released via {@link CachedPtr#close} and the
   * eviction policy retires cold entries.
   */
  public List<V> drain() {
    lock.lock();
    try {
      List<V> drained = new ArrayList<>(lru.size());
      for (Entry<V> e : lru.values()) {
        drained.add(e.value);
      }
      lru.clear();
      index.clear();
      return drained;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Drains only the entries whose key matches {@code predicate}, returning their values. Used by
   * partial-shutdown callers (one decorator closing while peers keep their entries). Waits for any
   * in-flight {@link #generate} calls on matching keys to settle BEFORE draining — otherwise a
   * generator that finishes after this call returns would re-insert a value the shutdown caller
   * already believed was gone. Bounded wait: generators always exit {@code pending} (success or
   * throw).
   *
   * <p><b>Pin semantics:</b> matches the {@link #drain} contract — pinned entries are drained too
   * (this is a shutdown primitive, not eviction). If a generator completes during the wait window,
   * its caller will receive a {@link CachedPtr} whose value is then yanked and closed by the
   * drainer; subsequent reads through that pointer fail at the underlying resource level (e.g.
   * IOException("Stream closed"), the same symptom Hadoop produces for any close-during-read race).
   * Callers performing a partial drain must understand that close-while-reading is fundamentally
   * racy at the inner-FS layer; the only guarantee here is no resource leak across the drain.
   */
  public List<V> drainMatching(Predicate<K> predicate) {
    Objects.requireNonNull(predicate, "predicate");
    lock.lock();
    try {
      while (anyPendingMatches(predicate)) {
        // Every generator completion (success or failure) calls signalAll on pendingCv, so we'll
        // wake on each one and re-check whether any matching key is still in flight.
        pendingCv.awaitUninterruptibly();
      }
      List<V> drained = new ArrayList<>();
      Iterator<Map.Entry<K, Entry<V>>> it = lru.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<K, Entry<V>> e = it.next();
        if (predicate.test(e.getKey())) {
          drained.add(e.getValue().value);
          it.remove();
          index.remove(e.getKey());
        }
      }
      return drained;
    } finally {
      lock.unlock();
    }
  }

  private boolean anyPendingMatches(Predicate<K> predicate) {
    for (K k : pending.keySet()) {
      if (predicate.test(k)) {
        return true;
      }
    }
    return false;
  }
}
