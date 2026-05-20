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

import java.util.concurrent.atomic.AtomicReference;

/**
 * RAII pin on a single SSD region. While any pin exists for a region, the region's bytes cannot be
 * overwritten or evicted (mirrors velox {@code SsdPin}).
 *
 * <p>Pins are obtained from {@link SsdFile#find(long, long)} on cache hit and held by the reader
 * until the bytes have been consumed (typically copied into a RAM-tier {@link
 * io.github.luciferyang.cachedfs.core.CacheEntry CacheEntry}). Closing the pin decrements the
 * region's pin counter.
 *
 * <p><b>Thread safety:</b> {@link #close()} is idempotent and safe to call from multiple threads
 * concurrently — only the first successful CAS releases the underlying region. All other accessors
 * are read-only and safe from any thread once the pin has been published.
 */
public final class SsdPin implements AutoCloseable {

  /** Empty pin singleton — closing it is a no-op. */
  private static final SsdPin EMPTY = new SsdPin(null, -1, null);

  private final SsdFile file;
  private final int regionIndex;
  // null sentinel marks "released"; AtomicReference makes close() race-safe.
  private final AtomicReference<SsdRun> runRef;

  SsdPin(SsdFile file, int regionIndex, SsdRun run) {
    this.file = file;
    this.regionIndex = regionIndex;
    this.runRef = new AtomicReference<>(run);
  }

  public static SsdPin empty() {
    return EMPTY;
  }

  /**
   * Returns {@code true} for the empty singleton AND for any pin that has been {@link #close()}d.
   * The two observationally-equivalent states share this flag.
   */
  public boolean isEmpty() {
    return runRef.get() == null;
  }

  /** The packed run; {@code null} once the pin has been closed (i.e. {@link #isEmpty()} flips). */
  public SsdRun run() {
    return runRef.get();
  }

  /**
   * The region index this pin protects. Returns {@code -1} for the empty singleton — callers should
   * guard with {@link #isEmpty()} before indexing into region-keyed arrays.
   */
  public int regionIndex() {
    return regionIndex;
  }

  /** Releases the pin. Idempotent and safe under concurrent close. */
  @Override
  public void close() {
    SsdRun current = runRef.getAndSet(null);
    if (current == null) {
      return;
    }
    file.unpinRegion(regionIndex);
  }
}
