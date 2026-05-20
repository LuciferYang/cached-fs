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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntToLongFunction;

/**
 * Generic coalesced-IO helper used by both the storage path and the SSD path. Mirrors velox {@code
 * CoalesceIo.h} (helper around {@code readPins}).
 *
 * <p>Caller must pre-sort items by offset. {@link #coalesce} groups them by gap budget and batch
 * cap, then invokes a user-supplied {@link Reader} once per coalesced range. The lambda is the only
 * place that touches the underlying file handle.
 */
public final class CoalesceIo {

  private CoalesceIo() {}

  /** Mutable counters returned to the caller for IoStatistics propagation. */
  public record CoalesceIoStats(int numIos, long payloadBytes, long extraBytes) {}

  /** User-supplied bytes-fetcher invoked once per coalesced range. */
  @FunctionalInterface
  public interface Reader {
    void read(long offset, List<ByteBuffer> buffers) throws IOException;
  }

  /**
   * Coalesces and fetches.
   *
   * @param numItems number of items (must be &gt; 0; pre-sorted by offset)
   * @param offsetOf returns the file offset for item {@code i}
   * @param sizeOf returns the byte size for item {@code i}
   * @param destinationsFor returns the writable {@link ByteBuffer} ranges for item {@code i}
   * @param maxGap maximum gap (bytes) to fold into a single range; bounded by {@code int} so the
   *     gap-filler {@link ByteBuffer#allocate(int)} is well-defined
   * @param maxBatch maximum number of items per range
   * @param reader user-supplied bytes-fetcher
   */
  public static CoalesceIoStats coalesce(
      int numItems,
      IntToLongFunction offsetOf,
      IntToLongFunction sizeOf,
      java.util.function.IntFunction<List<ByteBuffer>> destinationsFor,
      int maxGap,
      int maxBatch,
      Reader reader)
      throws IOException {
    if (numItems <= 0) return new CoalesceIoStats(0, 0L, 0L);
    int numIos = 0;
    long payloadBytes = 0L;
    long extraBytes = 0L;

    int begin = 0;
    while (begin < numItems) {
      int end = begin + 1;
      long startOff = offsetOf.applyAsLong(begin);
      long lastEnd = startOff + sizeOf.applyAsLong(begin);
      long batchPayload = sizeOf.applyAsLong(begin);
      while (end < numItems && (end - begin) < maxBatch) {
        long off = offsetOf.applyAsLong(end);
        long sz = sizeOf.applyAsLong(end);
        long gap = off - lastEnd;
        if (gap > maxGap || gap < 0) break;
        extraBytes += gap; // gap >= 0 here
        batchPayload += sz;
        lastEnd = off + sz;
        end++;
      }
      List<ByteBuffer> rangeBuffers = new ArrayList<>();
      // Insert filler buffers for gaps so the underlying preadv can write past them.
      long cursor = startOff;
      for (int i = begin; i < end; i++) {
        long off = offsetOf.applyAsLong(i);
        long sz = sizeOf.applyAsLong(i);
        long gap = off - cursor;
        if (gap > 0) {
          rangeBuffers.add(ByteBuffer.allocate((int) gap)); // throwaway
        }
        rangeBuffers.addAll(destinationsFor.apply(i));
        cursor = off + sz;
      }
      reader.read(startOff, rangeBuffers);
      numIos++;
      payloadBytes += batchPayload;
      begin = end;
    }
    return new CoalesceIoStats(numIos, payloadBytes, extraBytes);
  }
}
