# Reader Glue Port Plan — velox §5 (split index)

> **Status:** v8.0, 2026-05-26. Per-phase docs split from the v7.21 monolith at commit 8acdecd after 28 santa-method review rounds. Each phase is its own convergence target and lives in `docs/reader-glue/`.

The plan has been split into per-phase docs because the monolithic version was too large for holistic santa-method review to converge cleanly (focused rounds passed; holistic rounds always found new findings in unrelated sections). Each per-phase doc is small enough to converge independently.

## Files

- [`docs/reader-glue/overview.md`](reader-glue/overview.md) — Goal, existing inventory, Hadoop API context, velox §5 mapping, cross-cutting decisions.
- [`docs/reader-glue/phase-5a.md`](reader-glue/phase-5a.md) — Phase 5a-prework (ScanTracker concurrency refactor + TrackingId bit-width change + Murmur3 utility) and Phase 5a wiring (ScanTracker + IoStatistics + IOStatisticsSource bridge).
- [`docs/reader-glue/phase-5b.md`](reader-glue/phase-5b.md) — Multi-chunk coalescing via CoalesceIo + preadv with abort-and-restart on Waiting.
- [`docs/reader-glue/phase-5c-0.md`](reader-glue/phase-5c-0.md) — `AsyncDataCache.pendingPrefetchBytes()` precondition.
- [`docs/reader-glue/phase-5c.md`](reader-glue/phase-5c.md) — Async prefetch (executor, PrefetchTask, admission gate, sequential-CAS-loop state machine, regime-change reset, seqHwmRegimeResets counter).
- [`docs/reader-glue/followups.md`](reader-glue/followups.md) — Out of scope, estimate methodology, test infrastructure, open follow-ups, README integration, recommendation.

## Version history (master)

- **v7.21** (HEAD = 8acdecd, 2026-05-26): R28 holistic re-review HIGHs — releaseFailedExclusive cross-package, admissionGateFalseCount AtomicLong, heap-pressure-ttl config knob, state-machine text drift.
- **v7.20** (HEAD = 00f0f1b): explicit `compareAndSet` loop for exactly-once seqHwmRegimeResets.
- **v7.0–v7.19**: see git log; consolidated R5–R26 santa-method findings.

The per-phase docs above carry forward all v7.21 decisions verbatim. Each can be iterated independently going forward.
