## Out of scope

- Per-column tracking; consumer-side concern.
- `DirectBufferedInput` (`fs.cached.enabled=false` already handles "no-cache").
- Adaptive next-quantum prefetch (velox §5.2.7); dormant.
- Cross-file prefetch coordination.

## Estimate methodology

A "commit" ≈ one bullet in the Changes list. TTL convergence took 19 commits over 11 santa-method rounds against a 7-bullet base — empirical multiplier ~2.7×. Reader glue adds concurrency surface, so use **2.7×–3.5×**:

| Phase | Bullets | Estimated commits incl. review |
| --- | --- | --- |
| 5a-prework | 1 | 2–4 |
| 5a wiring | 8 | 22–28 |
| 5b | 8 | 22–28 |
| 5c.0 pendingPrefetchBytes | 1 | 2–4 |
| 5c proper | 8 | 22–35 |

## Test infrastructure

- `PinLeakAssertions` helper in cached-fs-core's `test/` tree — `numExclusive == 0 && numShared == 0` after each test. Applied retroactively to 5a, plus 5b/5c.
- `PinLeakStressIT` in `cached-fs-hadoop/src/test/.../it/` (Failsafe, not Surefire — runs under `mvn verify`, ~30s).
- `CountingReadFile` + `ReadFileFactory` test seam (cached-fs-hadoop `test/`).

## Open follow-ups

- Replace `loadQuantumBytes × threads × 4` admission denominator with a measured value once a workload microbenchmark exists.
- Cap `bootstrap.scanTrackers` when metrics show `size() > 10_000` in any 24h window. Caffeine is **not** in the cached-fs dependency tree; use a `LinkedHashMap` wrapped with `synchronizedMap` and `removeEldestEntry` override, or a hand-rolled LRU. Adding Caffeine is a separate decision.
- Cap per-`ScanTracker` inner `TrackingData` map when any tracker's `data` map exceeds 10k entries. The `ScanTracker.size()` gauge (added in Phase 5a-prework and exposed as a Hadoop dynamic gauge via `CacheBootstrap.scanTrackerEntries()`) is the observable trigger condition. Partitioned-table scans touching 50k+ files accumulate `~40 bytes` per entry: 100 scans × 10k files ≈ 40 MB resident; 1k scans × 100k files ≈ 4 GB.
- `IoStatistics` ring-buffer of recent N streams for debugging.
- Per-`(scanId, fileNum)` tracker keying once Spark integration produces real workloads.

## README integration

- Phase 5a: `fs.cached.scan-id`, `fs.cached.scan-tracker.enabled`, `fs.cached.metrics.enabled` to config-reference table.
- Phase 5b: `fs.cached.coalesce.enabled`, `fs.cached.coalesce.max-gap-bytes`, `fs.cached.coalesce.max-chunks-per-group`, `fs.cached.coalesce.max-restarts`, `fs.cached.coalesce.always-on`.
- Phase 5c: 8 prefetch knobs (listed in §5c Configuration).
- Divergence-list bullets appended to `CacheBootstrap` (executor + close()), `AsyncDataCache` (pendingPrefetchBytes counter), `CachingInputStream` (5a/5b/5c reader behavior).

## Recommendation

Start with **Phase 5a-prework**. Then **Phase 5a wiring** as its own santa-method convergence. Phase 5b follows. Phase 5c.0 lands as a one-bullet commit before opening Phase 5c.
