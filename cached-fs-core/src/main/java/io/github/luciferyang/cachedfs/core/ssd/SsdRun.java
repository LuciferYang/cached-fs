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

/**
 * Packed {@code (offset, size)} pair for one SSD-resident cache entry. Mirrors velox {@code
 * SsdRun}.
 *
 * <p>Encoding: low <strong>23 bits</strong> = {@code size - 1} (so 0 means 1 byte and 0x7FFFFF
 * means 8 MiB max — matches default {@code loadQuantum}); high <strong>41 bits</strong> = file
 * offset. Separate {@code int32 checksum} is populated only when the {@code checksumEnabled} config
 * flag is on (CPT2 magic).
 *
 * <p><b>Thread safety:</b> immutable record; safe for unrestricted sharing.
 */
public record SsdRun(long fileBits, int checksum) {

  /** Bits used for the {@code size - 1} field. */
  public static final int SIZE_BITS = 23;

  /** Maximum encodable size: {@code 2^23 = 8 MiB} (matches default loadQuantum). */
  public static final int MAX_SIZE = 1 << SIZE_BITS;

  /** Mask covering the low 23 bits. */
  public static final long SIZE_MASK = (1L << SIZE_BITS) - 1;

  /** Maximum encodable offset: {@code 2^41 - 1} (high 41 bits of the packed {@code fileBits}). */
  public static final long MAX_OFFSET = (1L << (64 - SIZE_BITS)) - 1;

  /** Constructs an SsdRun with the given offset, size, and checksum. */
  public static SsdRun of(long offset, int size, int checksum) {
    if (size <= 0 || size > MAX_SIZE) {
      throw new IllegalArgumentException("size must be in (0, " + MAX_SIZE + "]: " + size);
    }
    if (offset < 0 || offset > MAX_OFFSET) {
      throw new IllegalArgumentException("offset must be in [0, " + MAX_OFFSET + "]: " + offset);
    }
    long fileBits = (offset << SIZE_BITS) | (long) (size - 1);
    return new SsdRun(fileBits, checksum);
  }

  public static SsdRun of(long offset, int size) {
    return of(offset, size, 0);
  }

  /** Decoded byte size of the entry (always {@code stored + 1}). */
  public int size() {
    return (int) (fileBits & SIZE_MASK) + 1;
  }

  /** File offset in the SSD shard file. */
  public long offset() {
    return fileBits >>> SIZE_BITS;
  }
}
