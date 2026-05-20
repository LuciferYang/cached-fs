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

import java.util.Objects;

/**
 * RAII handle that holds one reference on a {@link StringIdMap} entry. {@code 0} is the empty /
 * "no-id" sentinel; calling {@link #close()} on an empty lease is a no-op.
 *
 * <p>Mirrors velox's {@code StringIdLease} (StringIdMap.h:108-179). Use try-with-resources or call
 * {@link #close()} explicitly to release the reference.
 */
public final class StringIdLease implements AutoCloseable {

  /** Empty / "no-id" sentinel. */
  public static final long EMPTY_ID = 0L;

  private final StringIdMap map;
  // volatile so that on 32-bit JVMs the 64-bit read/write is atomic and visibility is
  // guaranteed even if a lease is shared across threads contrary to its single-owner contract.
  private volatile long id;

  /** Creates a lease by minting (or sharing) the id for {@code name}. */
  public StringIdLease(StringIdMap map, String name) {
    this.map = Objects.requireNonNull(map, "map");
    this.id = map.makeId(name);
  }

  /** Creates a lease over an already-known id (additional reference). */
  public StringIdLease(StringIdMap map, long existingId) {
    this.map = Objects.requireNonNull(map, "map");
    if (existingId == EMPTY_ID) {
      this.id = EMPTY_ID;
    } else {
      map.addReference(existingId);
      this.id = existingId;
    }
  }

  /** Constructs an empty lease (no underlying id). */
  public static StringIdLease empty(StringIdMap map) {
    return new StringIdLease(map, EMPTY_ID);
  }

  /** Returns the underlying id, or {@link #EMPTY_ID} for an empty lease. */
  public long id() {
    return id;
  }

  /** Returns {@code true} if this lease holds a real id. */
  public boolean hasValue() {
    return id != EMPTY_ID;
  }

  /**
   * Releases the reference. Safe to call multiple times AND safe under accidental concurrent close
   * (exception-handler double-close or contract violation): the synchronized guard ensures {@code
   * map.release} fires at most once per lease, preventing a sub-zero refcount that would let {@link
   * StringIdMap} reissue the id under a different name while a caller still believes the old
   * binding is alive.
   */
  @Override
  public synchronized void close() {
    if (id != EMPTY_ID) {
      map.release(id);
      id = EMPTY_ID;
    }
  }
}
