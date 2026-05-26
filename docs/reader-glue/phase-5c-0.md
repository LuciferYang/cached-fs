## Phase 5c.0 — `AsyncDataCache.pendingPrefetchBytes()` (precondition)

Lands first, no callers.

- `private final LongAdder pendingPrefetchBytes = new LongAdder()`.
- `public long pendingPrefetchBytes() { return pendingPrefetchBytes.sum(); }` — public for external observers and tests in different packages.
- Package-private (or protected): `void incrementPendingPrefetch(long bytes)` / `void decrementPendingPrefetch(long bytes)`. **Visibility:** since the only caller will be `cached-fs-hadoop`'s prefetch task and `cached-fs-hadoop` is a different package, we make these methods `public` too but javadoc them as "internal — call only from the cached-fs prefetch task". A future move to a sealed-friend pattern is possible if a `cached-fs-internal` annotation is introduced.
- **Integrity guard in decrement.** `decrementPendingPrefetch(bytes)` asserts `pendingPrefetchBytes.sum() - bytes >= 0` before applying (`if (pendingPrefetchBytes.sum() < bytes) throw new IllegalStateException(...)`). LongAdder's `sum()` is non-atomic vs concurrent adds, so the guard is best-effort under contention — it catches gross misuse (negative-going counter from external callers desyncing) without claiming atomic consistency. The PrefetchTask runtime never triggers this under correct flow because each increment is paired with exactly one decrement in `finally`.

  **Failure-mode contract.** If the integrity guard fires, the IllegalStateException (1) escapes `PrefetchTask.run()`'s innermost decrement step, (2) is caught by `ThreadPoolExecutor`'s uncaught-exception handler (logged at WARN with the stack), (3) does NOT mask the consumer-side handoff because the outer finally has already completed the future and run `clearPendingPrefetchIf`. The `pendingPrefetchBytes` counter is left in an inconsistent state for the lifetime of the JVM — acceptable because the integrity-check trigger represents a programming bug, not a runtime condition; the inconsistency would only matter if a third caller external to PrefetchTask desyncs the counter.

5c.0 does NOT specify increment/decrement call sites; that wiring lives entirely in 5c-proper.

Acceptance for 5c.0:
- Unit test: increment by `4×1024`, sum returns 4096; decrement by 1024, sum returns 3072.
- Public-API reach test in `cached-fs-hadoop`'s test tree: `CacheBootstrap.get().orElseThrow().ramCache().pendingPrefetchBytes()` returns 0 on a fresh bootstrap.
- Existing test suite passes.

