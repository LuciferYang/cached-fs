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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Multi-entry load grouped into a single IO. Mirrors velox {@code CoalescedLoad}.
 *
 * <p>State machine: {@code PLANNED → LOADING → LOADED | CANCELLED}. The {@link #loadOrFuture}
 * pattern guarantees only one thread runs {@link #loadData}; concurrent callers receive a {@link
 * LoadResult.Pending Pending} result with a future they may await.
 *
 * <p><b>Pin lifetime contract:</b> after a successful load, the produced shared pins are kept alive
 * by this {@code CoalescedLoad} until {@link #close()} is called. Consumers should obtain pins via
 * {@link #pins()}; closing the load releases all of them. This protects entries from eviction
 * between fill and consumption.
 *
 * <p>The destructor (here, {@link #close}) cancels via {@link State#CANCELLED} so any thread
 * waiting on a half-built load is unblocked even if the producer was destroyed mid-flight.
 *
 * <p><b>Thread safety:</b> all public methods are safe for concurrent calls. Subclasses' {@link
 * #loadData} runs without the load mutex held.
 */
public abstract class CoalescedLoad implements AutoCloseable {

  public enum State {
    PLANNED,
    LOADING,
    LOADED,
    CANCELLED
  }

  /** Sealed result of {@link #loadOrFuture(boolean, boolean)}. */
  public sealed interface LoadResult {
    /**
     * No further action available on this call: the load is either terminally finished (LOADED or
     * CANCELLED) or the caller passed {@code wait=false} while another thread was already filling —
     * the caller will not be notified of that fill's completion. Callers relying on the entries
     * being live MUST re-probe the cache rather than assume {@code pins()} reflects the in-flight
     * load.
     */
    record Done() implements LoadResult {}

    /** Load is still in progress. Caller awaits {@code future} to be notified of completion. */
    record Pending(CompletableFuture<Void> future) implements LoadResult {}
  }

  private final ReentrantLock mutex = new ReentrantLock();
  private State state = State.PLANNED;
  private CompletableFuture<Void> promise;
  private List<CachePin> sharedPins = List.of();

  private final List<RawFileCacheKey> keys;
  private final List<Integer> sizes;

  protected CoalescedLoad(List<RawFileCacheKey> keys, List<Integer> sizes) {
    Objects.requireNonNull(keys, "keys");
    Objects.requireNonNull(sizes, "sizes");
    if (keys.size() != sizes.size()) {
      throw new IllegalArgumentException("keys.size() != sizes.size()");
    }
    this.keys = List.copyOf(keys);
    this.sizes = List.copyOf(sizes);
  }

  /** Returns an immutable view of the keys this load covers. */
  protected final List<RawFileCacheKey> keys() {
    return keys;
  }

  /** Returns an immutable view of the per-key sizes. */
  protected final List<Integer> sizes() {
    return sizes;
  }

  public State state() {
    mutex.lock();
    try {
      return state;
    } finally {
      mutex.unlock();
    }
  }

  /** Cancels the load, transitioning to {@link State#CANCELLED} and waking any waiters. */
  public final void cancel() {
    setEndState(State.CANCELLED);
  }

  public abstract long size();

  public abstract boolean isSsdLoad();

  /**
   * Returns the shared pins produced by a successful load, in the same order as {@link #keys()}.
   * Empty if the load has not yet completed or was cancelled. The pins remain owned by this {@code
   * CoalescedLoad}; do NOT close them — call {@link #close()} on the load instead.
   */
  public final List<CachePin> pins() {
    mutex.lock();
    try {
      return sharedPins;
    } finally {
      mutex.unlock();
    }
  }

  /**
   * Drives the load. {@code wait==true} causes a {@link LoadResult.Pending Pending} return when
   * another thread is currently filling; {@code wait==false} returns {@link LoadResult.Done Done}
   * eagerly even if a fill is in flight (caller will not be informed of completion).
   *
   * @param wait whether to receive a future for an in-flight load
   * @param ssdSavable whether to mark loaded entries as SSD-saveable
   */
  public final LoadResult loadOrFuture(boolean wait, boolean ssdSavable) {
    boolean shouldLoad = false;
    mutex.lock();
    try {
      switch (state) {
        case CANCELLED, LOADED -> {
          return new LoadResult.Done();
        }
        case LOADING -> {
          if (!wait) return new LoadResult.Done();
          if (promise == null) {
            promise = new CompletableFuture<>();
          }
          return new LoadResult.Pending(promise);
        }
        case PLANNED -> {
          state = State.LOADING;
          shouldLoad = true;
        }
      }
    } finally {
      mutex.unlock();
    }
    if (shouldLoad) {
      // velox: a no-wait caller signals a prefetch (the caller doesn't intend to await this load
      // synchronously). Mirrors AsyncDataCache.cpp's `loadData(/*prefetch=*/wait==nullptr)`.
      runLoad(/* prefetch= */ !wait, ssdSavable);
    }
    return new LoadResult.Done();
  }

  private void runLoad(boolean prefetch, boolean ssdSavable) {
    List<CachePin> exclusivePins = null;
    List<CachePin> producedShared = null;
    int next = 0;
    try {
      exclusivePins = Objects.requireNonNull(loadData(prefetch), "loadData() returned null");
      producedShared = new ArrayList<>(exclusivePins.size());
      for (; next < exclusivePins.size(); next++) {
        CachePin pin = exclusivePins.get(next);
        CacheEntry e = pin.entry();
        if (e == null || !e.isExclusive()) {
          throw new IllegalStateException("loadData returned a non-exclusive pin");
        }
        producedShared.add(pin.exclusiveToShared(ssdSavable));
      }
    } catch (RuntimeException | Error primary) {
      // Errors (e.g., OutOfMemoryError) MUST run the same unwind path as RuntimeException —
      // otherwise unconverted exclusive pins remain held forever, leaving their CacheEntry
      // objects permanently EXCLUSIVE and deadlocking every future findOrCreate on those keys.
      if (exclusivePins != null) {
        for (int i = next; i < exclusivePins.size(); i++) {
          try {
            exclusivePins.get(i).close();
          } catch (RuntimeException | Error suppressed) {
            primary.addSuppressed(suppressed);
          }
        }
      }
      // Drop any successfully-converted shared pins: the entries are already published shared, so
      // dropping the pins makes them evictable. This is the best we can do — the caller saw a
      // failure, so there is no consumer.
      if (producedShared != null) {
        for (CachePin sp : producedShared) {
          try {
            sp.close();
          } catch (RuntimeException | Error suppressed) {
            primary.addSuppressed(suppressed);
          }
        }
      }
      setEndState(State.CANCELLED);
      throw primary;
    }
    // Publish shared pins AND transition to LOADED atomically under the load mutex so a
    // concurrent close() either sees PLANNED/LOADING (and cancels the load with no pins to
    // release) or sees LOADED (and observes the populated pins). Without this atomicity, a
    // close() interleaving between sharedPins-assignment and setEndState(LOADED) could leave
    // state=LOADED with pins already closed by close().
    CompletableFuture<Void> pendingPromise;
    boolean cancelled;
    mutex.lock();
    try {
      cancelled = state == State.CANCELLED;
      if (!cancelled) {
        sharedPins = Collections.unmodifiableList(producedShared);
        state = State.LOADED;
      }
      pendingPromise = promise;
      promise = null;
    } finally {
      mutex.unlock();
    }
    if (cancelled) {
      // close() was called concurrently — release our pins, do not publish.
      for (CachePin sp : producedShared) {
        try {
          sp.close();
        } catch (RuntimeException ignored) {
          // best-effort cleanup
        }
      }
    }
    if (pendingPromise != null) {
      pendingPromise.complete(null);
    }
  }

  /**
   * Implemented by subclasses to fill the entries. Must return one exclusive pin per key in {@link
   * #keys()}. Pins will be promoted to shared by the framework and held by this load.
   *
   * @param prefetch true if this load is a speculative prefetch
   */
  protected abstract List<CachePin> loadData(boolean prefetch);

  private void setEndState(State endState) {
    CompletableFuture<Void> p;
    mutex.lock();
    try {
      state = endState;
      p = promise;
      promise = null;
    } finally {
      mutex.unlock();
    }
    if (p != null) {
      p.complete(null);
    }
  }

  /**
   * Cancels any waiters and releases the shared pins so their entries become evictable. Safe to
   * call multiple times; idempotent. The state flip, pin drain, and promise drain all happen inside
   * one mutex section so a concurrent runLoad observes a single atomic transition (either it
   * publishes before close arrives and close drains, or it sees CANCELLED and closes its own pins).
   */
  @Override
  public void close() {
    CompletableFuture<Void> p;
    List<CachePin> toClose;
    mutex.lock();
    try {
      state = State.CANCELLED;
      p = promise;
      promise = null;
      toClose = sharedPins;
      sharedPins = List.of();
    } finally {
      mutex.unlock();
    }
    if (p != null) {
      p.complete(null);
    }
    for (CachePin pin : toClose) {
      pin.close();
    }
  }
}
