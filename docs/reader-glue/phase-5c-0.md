## Phase 5c.0 — `AsyncDataCache.pendingPrefetchBytes()` (precondition)

Lands first, no callers.

- `private final LongAdder pendingPrefetchBytes = new LongAdder()`.
- `public long pendingPrefetchBytes() { return pendingPrefetchBytes.sum(); }` — public for external observers and tests in different packages.
- Package-private (or protected): `void incrementPendingPrefetch(long bytes)` / `void decrementPendingPrefetch(long bytes)`. **Visibility:** since the only caller will be `cached-fs-hadoop`'s prefetch task and `cached-fs-hadoop` is a different package, we make these methods `public` too but javadoc them as "internal — call only from the cached-fs prefetch task". A future move to a sealed-friend pattern is possible if a `cached-fs-internal` annotation is introduced.
- **Integrity guard in decrement (v8.1: WARN-not-throw).** `decrementPendingPrefetch(bytes)` checks `pendingPrefetchBytes.sum() >= bytes` before applying. **Under contention**, LongAdder's `sum()` is non-atomic vs concurrent adds — a transient observation where `sum() < bytes` can legitimately appear even when the global invariant `sum >= 0` holds (e.g., during the read of `sum()` another thread's intervening add hasn't yet been observed). v8.1 therefore **does NOT throw IllegalStateException on guard failure** (the v7.x throw produced false positives under correct PrefetchTask concurrency). Instead: the guard logs WARN once-per-JVM (deduped via a static AtomicBoolean) with the offending bytes/sum values, **then still applies the decrement unconditionally** (`pendingPrefetchBytes.add(-bytes)`). LongAdder.add is correct under concurrency; the only correctness-bound is the global invariant `eventually(sum >= 0)`, which the PrefetchTask increment/decrement pairing already guarantees. Operators see the WARN and can investigate; observability is preserved (`pendingPrefetchBytes()` keeps returning the live counter rather than going permanently inconsistent).

  **Failure-mode contract.** WARN log + decrement-anyway gives best of both worlds: programming bugs (a third caller desyncing the counter) surface via the WARN log; correct PrefetchTask concurrency does NOT throw spurious ISE; the public `pendingPrefetchBytes()` observer never gets stuck reporting a stale value because the decrement always lands.

5c.0 does NOT specify increment/decrement call sites; that wiring lives entirely in 5c-proper.

Acceptance for 5c.0:
- Unit test: increment by `4×1024`, sum returns 4096; decrement by 1024, sum returns 3072.
- Public-API reach test in `cached-fs-hadoop`'s test tree: `CacheBootstrap.get().orElseThrow().ramCache().pendingPrefetchBytes()` returns 0 on a fresh bootstrap.
- **Contended decrement** (v8.1): 16 threads concurrently issue increment(N) and decrement(N) pairs for 1s; assert no exception thrown, no WARN logs emitted (the legitimate path doesn't trip the guard), final `sum() == 0`.
- **Guard WARN-not-throw** (v8.1): from a single thread, call `decrementPendingPrefetch(100)` on a fresh `AsyncDataCache` with `sum() == 0`; assert no exception thrown, one WARN log captured, `sum() == -100` (the decrement landed; counter is now negative which the WARN flagged).
- Existing test suite passes.
