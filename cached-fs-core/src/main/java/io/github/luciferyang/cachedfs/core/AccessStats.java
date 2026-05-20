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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Per-entry access bookkeeping used by the eviction scorer. Mirrors velox {@code AccessStats}.
 *
 * <p>{@code lastUse} ticks at ≈0.5–2 ms resolution from {@link #now()}. {@code numUses} is the use
 * count since last reset. The retention score is
 *
 * <pre>{@code
 * score = (now - lastUse) / (1 + numUses)
 * }</pre>
 *
 * with the special case that {@code lastUse == 0} returns {@link Integer#MAX_VALUE} — the
 * "explicitly evictable" sentinel set by {@link #makeEvictable()}.
 *
 * <p><b>Thread safety:</b> {@link #score(int, long)} reads {@code lastUse} and {@code numUses}
 * non-atomically and may observe a torn snapshot under concurrent {@link #touch()}; this matches
 * velox's {@code tsan_atomic} (relaxed) intent — the score is heuristic. {@link #touch()} uses a
 * VarHandle add so increments are not lost under contention.
 */
public final class AccessStats {

  private static final VarHandle NUM_USES;
  private static final VarHandle LAST_USE;

  static {
    try {
      var lookup = MethodHandles.lookup();
      NUM_USES = lookup.findVarHandle(AccessStats.class, "numUses", int.class);
      LAST_USE = lookup.findVarHandle(AccessStats.class, "lastUse", int.class);
    } catch (NoSuchFieldException | IllegalAccessException ex) {
      throw new ExceptionInInitializerError(ex);
    }
  }

  @SuppressWarnings({"FieldMayBeFinal", "unused"}) // accessed via VarHandle
  private volatile int lastUse;

  @SuppressWarnings({"FieldMayBeFinal", "unused"}) // accessed via VarHandle
  private volatile int numUses;

  /** Wall-clock-ish tick approximating velox's {@code accessTime()}. */
  public static int now() {
    // System.nanoTime() in nanos; >>> 21 ≈ ~2 ms resolution. Same scale velox produces from
    // folly::hardware_timestamp() >> 21 for comparable scoring math.
    return (int) (System.nanoTime() >>> 21);
  }

  /**
   * Returns the retention score. Higher = less worth keeping. The {@code size} parameter is part of
   * the velox signature but currently unused; we keep it for symmetry / future use.
   */
  public int score(int now, @SuppressWarnings("unused") long size) {
    int lu = (int) LAST_USE.getVolatile(this);
    if (lu == 0) {
      return Integer.MAX_VALUE;
    }
    int uses = (int) NUM_USES.getVolatile(this);
    return (now - lu) / (1 + uses);
  }

  /** Updates last-use to now and atomically increments the use counter. */
  public void touch() {
    LAST_USE.setVolatile(this, now());
    NUM_USES.getAndAdd(this, 1);
  }

  /** Resets to "freshly reused" — last use = now, count = 0. */
  void reset() {
    LAST_USE.setVolatile(this, now());
    NUM_USES.setVolatile(this, 0);
  }

  /**
   * Marks the entry as immediately evictable. Sets {@code lastUse = 0} so {@link #score} returns
   * {@link Integer#MAX_VALUE}.
   */
  public void makeEvictable() {
    LAST_USE.setVolatile(this, 0);
    NUM_USES.setVolatile(this, 0);
  }

  /** Visible for tests. */
  int lastUse() {
    return (int) LAST_USE.getVolatile(this);
  }

  /** Visible for tests. */
  int numUses() {
    return (int) NUM_USES.getVolatile(this);
  }
}
