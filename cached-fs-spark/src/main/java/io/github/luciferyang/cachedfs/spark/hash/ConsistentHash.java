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
package io.github.luciferyang.cachedfs.spark.hash;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.codec.digest.MurmurHash3;

/**
 * Thread-safe consistent-hash ring. Each physical node spawns {@code replicate} virtual partitions
 * placed at MurmurHash3-derived slots; lookups find the first partition clockwise from the key's
 * hashed slot. Node add/remove operations only disturb keys near the affected partitions —
 * minimizing data movement in Spark's task-placement use case.
 *
 * <p>Adapted from {@code org.apache.gluten.hash.ConsistentHash}. Differences:
 *
 * <ul>
 *   <li>Uses {@link Node#key()} for hashing so two physically-distinct nodes can share the same
 *       hash identity if desired (we don't, but the abstraction is preserved).
 *   <li>Returns the iterator order rather than a {@link Set} when {@code count > 0} — the order is
 *       stable and matters for Spark's {@code preferredLocations[0]} being the primary.
 * </ul>
 *
 * @param <T> node type
 */
public final class ConsistentHash<T extends ConsistentHash.Node> {

  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final Map<T, Set<Partition<T>>> nodes = new HashMap<>();
  private final SortedMap<Long, Partition<T>> ring = new TreeMap<>();
  private final int replicate;
  private final Hasher hasher;

  public ConsistentHash(int replicate) {
    this(replicate, ConsistentHash::defaultHash);
  }

  public ConsistentHash(int replicate, Hasher hasher) {
    if (replicate <= 0) {
      throw new IllegalArgumentException("replicate must be positive, got " + replicate);
    }
    if (hasher == null) {
      throw new IllegalArgumentException("hasher must not be null");
    }
    this.replicate = replicate;
    this.hasher = hasher;
  }

  private static long defaultHash(String key, int seed) {
    byte[] data = key.getBytes();
    return MurmurHash3.hash32x86(data, 0, data.length, seed);
  }

  public boolean addNode(T node) {
    if (node == null) return false;
    lock.writeLock().lock();
    try {
      if (nodes.containsKey(node)) return false;
      Set<Partition<T>> parts = new HashSet<>(replicate);
      for (int i = 0; i < replicate; i++) {
        Partition<T> p = new Partition<>(node, i);
        long slot;
        int seed = 0;
        do {
          slot = hasher.hash(p.partitionKey(), seed++);
        } while (ring.containsKey(slot));
        p.slot = slot;
        ring.put(slot, p);
        parts.add(p);
      }
      nodes.put(node, parts);
      return true;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean removeNode(T node) {
    if (node == null) return false;
    lock.writeLock().lock();
    try {
      Set<Partition<T>> parts = nodes.remove(node);
      if (parts == null) return false;
      for (Partition<T> p : parts) {
        ring.remove(p.slot);
      }
      return true;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Returns up to {@code count} distinct nodes starting at the key's slot, walking clockwise. */
  public java.util.List<T> allocateNodes(String key, int count) {
    if (key == null || count <= 0) return java.util.List.of();
    lock.readLock().lock();
    try {
      if (nodes.isEmpty()) return java.util.List.of();
      if (count >= nodes.size()) {
        return new java.util.ArrayList<>(nodes.keySet());
      }
      long slot = hasher.hash(key, 0);
      java.util.LinkedHashSet<T> result = new java.util.LinkedHashSet<>();
      Iterator<Partition<T>> it = new AllocateIterator(slot);
      while (it.hasNext() && result.size() < count) {
        result.add(it.next().node);
      }
      return new java.util.ArrayList<>(result);
    } finally {
      lock.readLock().unlock();
    }
  }

  public int nodeCount() {
    lock.readLock().lock();
    try {
      return nodes.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean contains(T node) {
    lock.readLock().lock();
    try {
      return nodes.containsKey(node);
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Slot-iterator that wraps around the ring tail → head until exhausted. */
  private final class AllocateIterator implements Iterator<Partition<T>> {
    private final Iterator<Partition<T>> tail;
    private final Iterator<Partition<T>> head;

    AllocateIterator(long slot) {
      this.tail = ring.tailMap(slot).values().iterator();
      this.head = ring.headMap(slot).values().iterator();
    }

    @Override
    public boolean hasNext() {
      return tail.hasNext() || head.hasNext();
    }

    @Override
    public Partition<T> next() {
      return tail.hasNext() ? tail.next() : head.next();
    }
  }

  /** One virtual node on the ring. */
  public static final class Partition<T extends Node> {
    final T node;
    final int index;
    long slot;

    Partition(T node, int index) {
      this.node = node;
      this.index = index;
    }

    String partitionKey() {
      return node.key() + ":" + index;
    }
  }

  /** Hash-input contract for ring members. {@link #key()} drives slot placement. */
  public interface Node {
    String key();
  }

  /** Pluggable hash function. Default is MurmurHash3 x86 32-bit. */
  public interface Hasher {
    long hash(String key, int seed);
  }
}
