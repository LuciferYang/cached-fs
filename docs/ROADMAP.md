# cached-fs Roadmap

Single source of truth for in-flight and deferred work across all modules. Update the **Status** column when a row's state changes; close the row by linking the commit/PR that shipped it. Items live here ONLY when they are concrete enough to act on — vague exploration belongs in `velox-file-read-cache.md` or a per-feature plan doc.

Status legend:
- 🟢 **Done** — shipped; link the commit/PR. Kept here for the historical roadmap; safe to prune after one release.
- 🟡 **In progress** — actively being worked. One owner; one row.
- 🔵 **Planned** — accepted into the roadmap with a sized plan and acceptance criteria.
- ⚪ **Deferred** — accepted into the backlog; not actionable until a stated precondition is met.
- ⚫ **Out of scope** — explicitly NOT planned; listed so future contributors don't re-propose them without context.

---

## Modules

| # | Item | Module | Status | Notes |
| --- | --- | --- | --- | --- |
| M1 | `cached-fs-metrics` — ship Micrometer/JMX bridges for `IoStatistics` + `AggregatedIoStatistics` + `ScanTracker` gauges. | `cached-fs-metrics` | 🟢 Done | Initial `CachedFsMeterBinder` with cumulative `FunctionCounter`s + supplier-driven gauges; 5 unit tests covering registration, live updates, reason-tag cardinality, and partial wiring. Latency counters NOT yet bound — needs latency getters on `AggregatedIoStatistics` (deferred). |
| M2 | Per-`(scanId, fileNum)` tracker keying — drop the 29-bit `fileNumNode` hash, key `ScanTracker.data` by raw `long fileNum`. Eliminates birthday-paradox file-vs-file density contamination above ~33k files per scan. | `cached-fs-core` + `cached-fs-hadoop` | 🟢 Done | Design at `docs/m2-design.md`. `TrackingId` deleted entirely (zero external callers); `ScanTracker` retyped to `ConcurrentMap<Long, …>`; `CacheEntry.trackingId` dead field also removed. Collision-regression test pins the fix. |
| M2.1 | Spark-side scanId production — wire `fs.cached.scan-id` per Spark stage/partition so concurrent queries on the same JVM no longer collapse to the `"default"` scanId. | `cached-fs-spark` | 🟢 Done | Shipped as `CachedFsScanIdPlugin` (Spark plugin). Wire with `spark.plugins=io.github.luciferyang.cachedfs.spark.CachedFsScanIdPlugin`; the executor plugin's `onTaskStart` opens a `withScanId("task-{stageId}-{partitionId}-{taskAttemptId}")` scope and closes it on `onTaskSucceeded/onTaskFailed`. Per-task-attempt isolation, retry-aware. 3 unit tests + 1 IT pin the wiring; IT verified on both Spark 4.0 and 4.1 profiles. |
| R3 | Cap per-`ScanTracker` inner map at 10k entries so the per-file keying from M2 doesn't blow memory on 50k-file scans. | `cached-fs-core` + `cached-fs-hadoop` | 🟢 Done | New `fs.cached.scan-tracker.max-entries-per-tracker` (default 10_000). Cap-overflow surfaces via `CacheBootstrap.scanTrackerEntriesRejected()` and the `cached_fs.scan_tracker.entries_rejected` gauge. Soft cap: race-window overshoot bounded by concurrent puts. |
| M3 | `cached-fs-cli` — Picocli-driven ops tool. Likely subcommands: `inspect <path>`, `stats --window`, `drain`, `purge --pattern`. Today the module is a pom-only stub. | `cached-fs-cli` | 🔵 Planned | Picocli 4.7.6 already in parent dependencyManagement. Acceptance criteria TBD; minimum is `inspect` + `stats`. |

## Reader-glue follow-ups (from `docs/reader-glue/followups.md`)

| # | Item | Status | Trigger condition |
| --- | --- | --- | --- |
| R1 | Replace `loadQuantum × threads × 4` admission denominator with a workload-measured value | ⚪ Deferred | Needs a workload microbenchmark first; no current benchmark harness in the repo. |
| R2 | Cap `bootstrap.scanTrackers` when `size() > 10_000` in any 24h window | ⚪ Deferred | Observability trigger: only act when production metrics show the gauge crosses the threshold. |
| R3 | Cap per-`ScanTracker` inner `TrackingData` map at 10k entries | 🟢 Done | Shipped alongside M2 (see Modules table above). |
| R4 | `IoStatistics` ring-buffer of recent N streams for debugging | ⚪ Deferred | Debugging convenience; not blocking any feature. |

## Known limitations (README `### Limitations`)

| # | Item | Status | Notes |
| --- | --- | --- | --- |
| L1 | `openFile(PathHandle)` / `open(PathHandle, int)` delegate to inner FS | ⚪ Deferred | `PathHandle.contentTag` is opaque; cache keying needs `PathHandle.contentHash` support. Unblocks Iceberg metadata-pointer reads. |

## Velox parity gaps (out of scope)

| # | Item | Status | Reason |
| --- | --- | --- | --- |
| V1 | Per-column tracking | ⚫ Out of scope | Consumer-side concern; the reader (Parquet/ORC) already does column-level pruning. |
| V2 | Adaptive next-quantum prefetch (velox §5.2.7) | ⚫ Out of scope | Dormant in velox itself; the static prefetch admission gate covers the common case. |
| V3 | Cross-file prefetch coordination | ⚫ Out of scope | No clear consumer demand. |
| V4 | `DirectBufferedInput` (no-cache path) | ⚫ Out of scope | `fs.cached.enabled=false` already provides the no-cache path. |

---

## How to use this doc

- When you START work on a row, flip its status to 🟡 and name the commit you expect to open the work in. Only ONE row per developer should be 🟡 at a time.
- When the work SHIPS, flip to 🟢 and link the merge commit.
- When a 🔵 / ⚪ row's preconditions change (a benchmark lands, a metrics gauge crosses its threshold), update the **Notes** column with the date and the new state.
- A 🟡 row that hasn't moved in two weeks should be moved back to 🔵 (something is blocking it; the blocker belongs in Notes).
- New rows MUST include an acceptance criterion. "Ship X" with no definition of done is not roadmap-ready.
