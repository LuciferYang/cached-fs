# Reader Glue Port Plan — velox §5

> **Status:** draft v2, 2026-05-25. Plan-only; no code changes yet. Updated after round-1 review (commit 11786c7) — fixes inventory accuracy, decides previously-open questions, scopes the lifecycle and concurrency interactions, and replaces several invented APIs with what actually exists.

## Goal

Port velox's `CachedBufferedInput` + `ScanTracker` reader-side wiring (velox-file-read-cache.md §5) into cached-fs. Today the Hadoop decorator's `CachingInputStream` does per-chunk fetches via `findOrCreate`; it does not coalesce multi-chunk IO, prefetch, or feed `ScanTracker` / `IoStatistics`.

## Existing inventory (verified against tree at HEAD = 11786c7)

| Piece | Location | State |
| --- | --- | --- |
| `ScanTracker` | `cached-fs-core/.../tracker/ScanTracker.java` | Implemented; refs/reads via `recordReference(TrackingId, long)` / `recordRead(TrackingId, long)`; snapshot via `data(id)`. Single `ReentrantLock` over a `HashMap<TrackingId, MutableData>` — **JVM-wide serialization point under concurrent readers sharing one tracker**. **Not wired** into any reader. |
| `TrackingId` | same package | Record wrapping a single `int id`. Construction: `TrackingId.of(int node, int streamKind)` packs `(node << 5) \| streamKind`. There is no 2-arg constructor; `new TrackingId(0)` and `TrackingId.of(0, 0)` both yield id=0 (non-empty). `TrackingId.EMPTY = new TrackingId(-1)` and `ScanTracker.recordReference/recordRead` early-return on `isEmpty()`. |
| `TrackingData` | same package | Record exposing `referencedBytes`, `lastReferencedBytes`, `readBytes`, plus methods `readPct()` and `adjustedReadPct()`. **There is no `readDensity()` method.** Velox uses `readDensity = readBytes / (1 + referencedBytes)` (0..1) — if cached-fs needs it, we add a helper on `TrackingData`. |
| `IoStatistics` | `cached-fs-core/.../stats/IoStatistics.java` | Implemented; counters via `incRead(long)`, `incPrefetch(long)`, `incSsdRead(long)`, `incRamHit(long)`, `incRawOverreadBytes(long)`, latency methods. Each `inc*` increments both a count and a byte total (the public getters return byte totals). **Not wired** into any reader. |
| `CoalesceIo` | `cached-fs-core/.../CoalesceIo.java` | Gap-based grouping primitive over generic ranges. Stateless utility, ready to use. |
| `CoalescedLoad` | `cached-fs-core/.../CoalescedLoad.java` | **Abstract class** — subclass implements `loadData(boolean prefetch)`. The framework's `runLoad` performs the `exclusiveToShared` itself. The only concrete subclass today is `CoalescedLoadTest.TestLoad` (test-only). **No production subclass exists.** Phase 5b must write the Hadoop subclass (`HadoopCoalescedLoad`) — or bypass `CoalescedLoad` entirely and drive `CoalesceIo` + `preadv` directly. We pick the bypass path (see §5b decision). |
| `CachingInputStream` | `cached-fs-hadoop/.../CachingInputStream.java` | Per-chunk fetch only via `copyChunk` → `findOrCreate` → `Hit`/`Exclusive`/`Waiting` handling. Implements `Seekable` + `PositionedReadable`. **Single production constructor** — call sites: `CachedFileSystem.open()` only (verified via grep). |
| `HadoopReadFile` | `cached-fs-hadoop/.../HadoopReadFile.java` | `public final class HadoopReadFile implements ReadFile` — **cannot be spied directly** by Mockito; test seams must go through the `ReadFile` interface or count via `IoStatistics`. |
| `CacheBootstrap` | `cached-fs-hadoop/.../CacheBootstrap.java` | Per-JVM singleton. Holds `ramCache`, `ssdCache`, `stringIds`, `handleFactory`, `loadQuantumBytes`, `ttlController`, `openersByEndpoint`. **No `close()` method on the public surface**; teardown is `uninstallForTesting()` only — production code never tears down. `pendingPrefetchBytes()` does NOT exist on `AsyncDataCache`. |

## Hadoop API context

- `FSDataInputStream` is `public final` and wraps an inner `InputStream`. The decorator can't subclass it; the inner `CachingInputStream` is what we extend.
- Hadoop 3.3+ defines `org.apache.hadoop.fs.statistics.IOStatisticsSource` with `getIOStatistics()`. If `CachingInputStream` implements `IOStatisticsSource`, Hadoop's standard tooling (S3A, ABFS, Spark's IOStatisticsContext) picks the stats up automatically. **Decision (was open question #4):** implement `IOStatisticsSource` on `CachingInputStream` and bridge our `IoStatistics` to Hadoop's via a small adapter so the standard `FSDataInputStream.getIOStatistics()` cascade works.
- `CachedFileSystem.openFile()` / `open(PathHandle)` bypass the decorator today (documented in the Limitations section of README). Phase 5 does not change that.

## Velox §5 mapping → Java port

Velox's design is built around the Hive connector, which orchestrates many columns (streams) per file per stripe. Hadoop's `FSDataInputStream` is one stream per file; column-level orchestration lives in the consumer (Parquet/ORC reader). Implications:

- `ScanTracker`'s scope in velox is per-(TableScan, stream). The Hadoop layer has no column visibility, so the natural scope shrinks to **per-(scanId, file)** with `TrackingId = TrackingId.of(0, 0)` standing in for "the whole file as one virtual stream". Per-column tracking belongs to the consumer (out of scope).
- `CachedBufferedInput` orchestrates many streams in one buffered input. The Hadoop equivalent is one `CachingInputStream` per file. **Multi-chunk-within-a-single-file** is what we can coalesce; cross-file coalescing is intentionally out of scope (velox's reach doesn't extend there either — a `CachedBufferedInput` is constructed per `Reader`, hence per file).
- `prefetchPct_` (dormant in OSS velox §5.2.7 per `CacheInputStream.h:187`) we leave dormant. Revisit if observed sequential-scan miss rate exceeds 25%.

## Decisions (previously open questions)

1. **scanId source.** Phase 5a wires `fs.cached.scan-id` as a Hadoop `Configuration` key, plus a thread-local fallback `CacheBootstrap.currentScanId()` for callers (Spark `TaskContext.get().stageId()+attemptNumber`) that prefer to set it imperatively. Default scan id is the literal string `"default"`. If neither source is set, all readers share the default tracker. Decision driven by: config-key is universal across Hadoop tooling; thread-local enables Spark integration without forking Configuration per task.
2. **Coalesce gap default.** Phase 5b uses **`fs.cached.coalesce.max-gap-bytes` (default 512 KiB)** for the storage-side grouping in `CoalesceIo` — matches velox `io::ReaderOptions::maxCoalesceDistance_` default (`velox/common/io/Options.h:66, 211`). The fact that cached-fs's chunks are already `loadQuantum`-sized (8 MiB default) means in-file coalescing usually targets adjacent chunks (gap = 0); the 512 KiB knob exists for cases where a small intra-quantum hole exists between two read requests.
3. **Prefetch executor scope.** Phase 5c uses a **single shared executor on `CacheBootstrap`**. Rationale: bootstrap is already per-JVM; per-FS executors would multiply by the (now 4–5) endpoints in the multi-scheme registry; admission control (`pendingPrefetchBytes`) is global. Configurable via `fs.cached.prefetch.threads` (default `Runtime.getRuntime().availableProcessors()`).
4. **IoStatistics exposure.** `CachingInputStream implements IOStatisticsSource`. The cached-fs `IoStatistics` is adapted to Hadoop's `org.apache.hadoop.fs.statistics.IOStatistics` via a thin `IoStatisticsAdapter` in `cached-fs-hadoop`. `FSDataInputStream.getIOStatistics()` then propagates automatically.

## Phase 5a — ScanTracker + IoStatistics wiring (small, foundation)

Wire the existing primitives into `CachingInputStream`. No new IO patterns; same per-chunk fetch with observability + density signals.

### Pre-work (mandatory; no behavior change)

- **Reduce ScanTracker contention.** Replace the single `ReentrantLock` over `HashMap` with `ConcurrentHashMap<TrackingId, MutableData>` where each `MutableData` uses `LongAdder` for `referencedBytes` / `readBytes` and a `volatile long` for `lastReferencedBytes`. Existing semantics preserved (sum-on-read snapshot). Acceptance: existing `ScanTrackerTest` passes; new microbenchmark in `cached-fs-core/src/test/.../tracker/ScanTrackerContentionTest.java` shows >4× throughput under 8-thread shared-tracker contention vs the locked baseline.

### Changes (in landing order)

1. **`CacheBootstrap`** new field `ConcurrentMap<String, ScanTracker> scanTrackers`; method `ScanTracker trackerFor(String scanId)` lazy-creates with `loadQuantumBytes` (`scanId` defaults to `"default"` if null). Lifetime: trackers are bound to the bootstrap; no eviction in 5a — the map size is bounded by distinct scan-ids seen during the JVM lifetime. If churn becomes a problem (long-lived JVMs with many one-shot scans), we add a Caffeine-bounded cache in a follow-up. (Decision tracked in §Open follow-ups.)
2. **`CachedFsConfig`** add key `fs.cached.scan-id` (default `"default"`). README config-reference table picks it up.
3. **`CacheBootstrap.currentScanId()`** new thread-local accessor (`ThreadLocal<String>`), plus `withScanId(scanId, runnable)` helper. Used by Spark integration via `TaskContext.addTaskCompletionListener` (out of scope here; documented for downstream).
4. **`CachingInputStream`** constructor gains a `ScanTracker tracker` parameter and stores it alongside the existing fields. The class also gains:
   - `final TrackingId trackingId` — fixed at `TrackingId.of(0, 0)` since the Hadoop layer has no column metadata. Documented in javadoc as "Hadoop-tier file-level tracking; column-level tracking is the consumer's job".
   - `final IoStatistics ioStats` — new instance per stream.
5. **`CachedFileSystem.open`** resolves the scan id via `CacheBootstrap.currentScanId()` (preferred) → `conf.get(SCAN_ID, "default")` (fallback) → `"default"`. Looks up the tracker via `b.trackerFor(scanId)` and passes it to `CachingInputStream`.
6. **`CachingInputStream.readFullyFromCache`** records:
   - Once at the top: `tracker.recordReference(trackingId, length)` and `ioStats.incRead(length)`.
   - Once at the bottom: `tracker.recordRead(trackingId, length)`.
   - Per-chunk inside the existing switch, on `case FindResult.Hit`: `ioStats.incRamHit(copyLen)`. (No separate `cache.exists` probe — the hit signal is the sealed-result branch.)
7. **`CachingInputStream implements IOStatisticsSource`** — `IoStatisticsAdapter` exposes our internal counters under Hadoop-standard names (`stream_read_bytes`, `stream_read_cache_hit_bytes`, …). Lifecycle: a per-stream snapshot is aggregated into a process-wide `CacheBootstrap.aggregateIoStats` on `CachingInputStream.close()` so the per-stream IoStatistics survives stream close as a roll-up — addresses the "lost-on-close" gap.

### Off-switch

`fs.cached.scan-tracker.enabled` (default `true`). When false, `trackerFor` returns a singleton no-op tracker; `IoStatistics` still records (it's a counter-only struct, no shared lock). Lets operators bisect a regression without losing per-stream metrics.

### Acceptance tests

- **Multi-chunk-read test** (locks in the cross-chunk path). Configure `fs.cached.load-quantum-bytes=1048576` (1 MiB) in the test; read 8 MiB. Expect: `tracker.data(id).referencedBytes == 8 MiB`, `readBytes() == 8 MiB`, `readPct() == 100`. **Important:** the default `loadQuantum=8 MiB` would make this a single-chunk read; the test must override.
- **Warm re-read**: cold read 4 MiB into the cache, then re-read same range. Assert `ioStats.ramHitBytes() == 4 MiB` after the warm pass; `tracker.data(id).readPct() == 100` after both passes.
- **Concurrent readers**: 8 threads read disjoint 1 MiB windows of the same file under the same scan id. Assert `tracker.data(id).referencedBytes == 8 MiB` (accumulates), no deadlock, no `ConcurrentModificationException`.
- **Off-switch**: with `fs.cached.scan-tracker.enabled=false`, repeat the multi-chunk-read test; assert `tracker.data(id) == TrackingData.EMPTY` and `ioStats.readBytes() == 8 MiB` (stats keep working independently).
- **IOStatisticsSource bridge**: open the stream via `FileSystem.get(...).open(path)`, cast to `IOStatisticsSource`, query `getIOStatistics().counters()`. Expect the read-bytes counter to be present after a read.

### Risk

Low for behavior; **medium-low for concurrency** because of the ScanTracker pre-work. The pre-work is a self-contained refactor with the existing tests as the regression bar.

## Phase 5b — Multi-chunk coalescing (medium)

Cut IO syscalls for sequential reads. When a single positional read crosses N missed chunks, fill all N via one `ReadFile.preadv` instead of N. Bypass `CoalescedLoad` (it doesn't fit a single-file Hadoop reader cleanly — see inventory note) and drive `CoalesceIo` directly.

### Algorithm

1. Walk the chunk range `[startChunk, endChunk]`. For each chunk:
   - `findOrCreate(key, size, /*contiguous=*/false)`.
   - `case Hit` → push pin onto `hits` list.
   - `case Exclusive` → push pin onto `exclusives` list.
   - `case Waiting` → await the future, then re-call `findOrCreate` for this chunk only (treat the retry as a fresh lookup; the future's resolution means a sibling thread either promoted or failed — both produce a clean re-attempt). Do NOT hold any prior `Exclusive` pin across the await — the await blocks indefinitely on a peer thread's fill, and holding an `Exclusive` pin during that wait risks fairness issues if peers depend on cache pressure relief. **Discipline:** if a `Waiting` is encountered while `exclusives` is non-empty, abort the batch, release all currently-held pins (Hit pins close, Exclusive pins go through `releaseFailedExclusive` to wake any waiters), and restart the walk from `startChunk`. Bounded by a retry counter; if we restart more than 3 times, fall back to per-chunk `copyChunk` for this read.
2. Group `exclusives` into coalesced ranges via `CoalesceIo` with `maxGap = fs.cached.coalesce.max-gap-bytes`. Each group produces one `preadv(offset, buffers)` where `buffers` is the concatenation of every member's `dataRanges(chunkSize)` (in offset order).
3. For each `Exclusive` group: issue `handle.readFile().preadv(...)`. **Pin lifecycle on failure**: if `preadv` throws, iterate `exclusives` and call `releaseFailedExclusive` on each, accumulating any release-throw into a primary/suppressed chain; rethrow the original.
4. On success: promote each `Exclusive` to `Shared` via `exclusiveToShared(true)`. If `exclusiveToShared` throws, close that pin via `pin.close()` and add to a suppressed list; promote the rest; rethrow at the end.
5. Copy bytes out of every shared/hit pin in offset order, close them in finally.
6. **Group size cap.** `fs.cached.coalesce.max-chunks-per-group` (default 64). A single read crossing > cap chunks is split into multiple groups so peak pinned memory is bounded at `cap × loadQuantum` (default 64 × 8 MiB = 512 MiB). Caller can lower the cap if their cache is small.
7. **TTL interaction.** The TTL controller's `applyTTL` calls `removeFileEntries` on the RAM tier under the shard mutex; an entry currently held as `Exclusive` reports `numPins != 0` so it stays retained (see `CacheShard.removeFileEntries` line 183). No special handling needed — the existing pin-aware fail-soft is sufficient.
8. **Eviction interaction.** Each promoted-to-shared pin calls `setFirstUse(true)` so the clock sweep does not immediately reclaim them (matches existing single-chunk path).

### Off-switch

`fs.cached.coalesce.enabled` (default `true`). When false, `readFullyFromCache` calls the existing per-chunk `copyChunk` loop unchanged. Lets operators bisect coalescing regressions without touching the rest of the cache.

### Acceptance tests

- **One preadv per coalesced group**: Inject a counting `ReadFile` (test-only `CountingReadFile` wrapping a real `HadoopReadFile` — `HadoopReadFile` is `public final` so we wrap rather than spy). Set `loadQuantum=1 MiB`, `coalesce.max-gap-bytes=512 KiB`. Cold-read 4 MiB spanning 4 chunks. Assert `countingReadFile.preadvCalls == 1`.
- **Group cap respected**: `coalesce.max-chunks-per-group=2`, cold-read 4 MiB. Assert `preadvCalls == 2`.
- **Disabled fallback**: `coalesce.enabled=false`, same setup. Assert `preadvCalls == 4` (one per chunk, today's behavior).
- **Mixed Hit/Miss**: prefill chunks 0 and 2 (warm), cold-read 4 chunks. Assert two `preadv` calls for the miss groups (chunks 1 and 3 separately, since they're separated by hit chunks).
- **Waiting restart-and-fallback**: 2-thread test where T1 holds chunk 1 Exclusive (artificially blocked); T2 reads chunks 0..2. T2 must see Waiting on chunk 1, release its chunk-0 Exclusive, await, retry; verify no deadlock and ≤ 4 retries.
- **Pin-leak detector** (new test harness `PinLeakAssertions`): assert `cache.refreshStats().numShared == 0` and `numExclusive == 0` after each test method.
- **rawOverreadBytes accounting**: cold-read two chunks with a 512 KiB gap between them. Assert `ioStats.rawOverreadBytes() == 512 KiB`. Adjacent chunks: assert it stays 0.

### Risk

Medium. Multiple exclusive pins simultaneously held is the core lifecycle change; the abort-and-restart-on-Waiting policy keeps the implementation tractable. The pin-leak detector is the load-bearing test.

## Phase 5c — Async prefetch (large)

Issue async loads for the next loadQuantum-aligned chunk while the consumer reads the current one.

### Phase 5c.0 — `AsyncDataCache.pendingPrefetchBytes()` (precondition)

**Independent sub-phase**, lands first. Adds a process-wide counter for in-flight prefetch bytes so the admission gate can throttle.

- `AsyncDataCache` adds `private final LongAdder pendingPrefetchBytes` plus `long pendingPrefetchBytes()`.
- Caller side: increment by `chunkSize` BEFORE submitting a prefetch task; decrement either on `exclusiveToShared` success OR on `releaseFailedExclusive`. Both paths must hit the decrement (otherwise leak).
- Acceptance: a unit test submits 4 prefetches, blocks them on a latch, verifies `pendingPrefetchBytes() == 4 * chunkSize`, releases the latch, verifies it returns to 0. Failure path test: submit 1 prefetch, throw inside the load, verify the counter still returns to 0.

### Changes (Phase 5c proper)

1. **`CacheBootstrap`** new field `ExecutorService prefetchExecutor = Executors.newFixedThreadPool(fs.cached.prefetch.threads, daemonFactory)` where daemonFactory creates daemon threads so the executor doesn't block JVM exit. **Shutdown path**: a new `CacheBootstrap.close()` method (currently absent — Phase 5c introduces it):
   1. Mark the bootstrap as shutting down.
   2. `prefetchExecutor.shutdown()` + `awaitTermination(30s)`; on timeout, `shutdownNow()` (interrupts in-flight prefetches). In-flight `preadv` is best-effort — `releaseFailedExclusive` runs in a finally block inside the prefetch task, so even an interrupted task releases its pins.
   3. Drain the handle factory (`handleFactory.closeAll()`).
   4. Close `ssdCache` then `ramCache`.
   The existing `uninstallForTesting()` delegates to `close()` for test rigs.
2. **`CachingInputStream`** new field `volatile CompletableFuture<RawFileCacheKey> pendingPrefetchKey`. **Note: we deliberately do NOT hand a `CachePin` across threads**, in keeping with `CachePin`'s single-owner contract. Instead the prefetch task only ensures the entry is in cache; the consumer thread does a fresh `findOrCreate` on the prefetched key (which will hit) when it crosses into chunk N+1.
3. After each successful chunk read (consumer thread):
   - If `pendingPrefetchKey == null` AND `position >= chunkEnd - prefetchTriggerOffset` (configurable, see below) AND `tracker.data(id).adjustedReadPct() >= prefetchPctThreshold` AND `admissionGate()`: submit a prefetch task for chunk N+1.
4. **Prefetch task** (executor thread):
   1. `cache.pendingPrefetchBytes.add(chunkSize)`.
   2. `findOrCreate(nextKey, chunkSize, contiguous=false)`. If `Hit` or `Waiting` → cache already has it; decrement counter and return.
   3. If `Exclusive`: fill via `preadv`, then `exclusiveToShared(true)`. Pin lifecycle in a try/finally: on any throw `releaseFailedExclusive` and decrement counter.
   4. Complete the future with the key.
5. **Consumer thread on chunk N+1**: if `pendingPrefetchKey` is set, await it; then call `findOrCreate(key, …)` which now hits warm. Either way, on entering chunk N+1 the prefetch is consumed and reset to null. If the future completed exceptionally, log at DEBUG, discard, fall through to fresh `findOrCreate`.
6. **Admission gate** (`shouldPreload` equivalent):
   - Pass if `pendingPrefetchBytes() + chunkSize < totalRamBytes / 2`. `totalRamBytes` is computed once at bootstrap time as `options.maxWriteRatio * Runtime.getRuntime().maxMemory()` (placeholder until phase-2 SSD save-back lands proper accounting; documented as a deliberate approximation until then).
7. **Close**: `CachingInputStream.close()` sets a `volatile boolean closed` first, then if `pendingPrefetchKey != null`, calls `pendingPrefetchKey.cancel(false)`. The prefetch task checks `closed` in finally and decrements the counter regardless. No pin leak: the task always uses its own pin via the standard exclusive→shared dance; the consumer never holds it.
8. **Seek-away invalidation**: when `seek()` moves the position outside the prefetch's target chunk, cancel `pendingPrefetchKey` (same cancel-and-decrement path).

### Configuration knobs

- `fs.cached.prefetch.enabled` (default `false` — opt-in for the first release of phase 5c).
- `fs.cached.prefetch.threads` (default `Runtime.getRuntime().availableProcessors()`).
- `fs.cached.prefetch.trigger-tail-fraction` (default `0.25` — fire once `position` is in the last 25% of the current chunk). Tied to typical scan throughput / preadv latency; quoted-but-rounded from velox's heuristic in CacheInputStream.cpp:106-110. Operators can tune.
- `fs.cached.prefetch.read-density-threshold` (default `50` — `adjustedReadPct >= 50` matches `FLAGS_cache_prefetch_min_pct = 50` in velox `BufferedInput.h:244-258` / `CachedBufferedInput.cpp:116-118`).

### Acceptance tests

- **Sequential prefetch**: long sequential read (32 MiB at `loadQuantum=1 MiB`). Assert `ioStats.prefetchBytes() / ioStats.readBytes() >= 0.5` after warmup.
- **Random suppression**: 1000 random 4 KiB positional reads. Assert `prefetchBytes() / readBytes() < 0.05` (density gate keeps prefetch dormant).
- **Admission backpressure**: configure `totalRamBytes` such that admission cap is reached after 2 in-flight prefetches; verify subsequent prefetch attempts are no-ops (the admission counter does not over-increment).
- **Close cancels prefetch**: open stream, read enough to trigger prefetch, close stream before prefetch task runs. Assert `cache.pendingPrefetchBytes() == 0` and `cache.refreshStats().numExclusive == 0` within 1s.
- **Prefetch failure**: inject `preadv` throw on the next chunk. Assert consumer's read still succeeds (falls through to fresh `findOrCreate`), `pendingPrefetchBytes` returns to 0, no pin leak.
- **Seek-away invalidation**: trigger prefetch for chunk N+1, then seek to chunk N+10. Assert `pendingPrefetchKey == null` and `pendingPrefetchBytes` returned to 0.

### Risk

High. Async pins crossing threads (avoided via the key-not-pin design), executor lifecycle (now grounded in a real `close()` method), prefetch admission gate (grounded in a real counter). Worth its own santa-method convergence pass.

## Out of scope

- **Per-column tracking** (`TrackingId` for individual streams). Belongs in the consumer (Parquet/ORC reader); we expose `TrackingId.of(node, streamKind)` for future use but don't wire it.
- **`DirectBufferedInput`** (velox §5.3): "no-cache path" already exists via `fs.cached.enabled=false` toggle.
- **Adaptive next-quantum prefetch** (velox §5.2.7): dormant in OSS velox; revisit only if sequential-scan miss-rate observation justifies.
- **Cross-file prefetch coordination**: technically possible at the `CachedFileSystem` layer (e.g. Parquet footer-then-data patterns) but explicitly scoped out — velox itself doesn't cross `CachedBufferedInput` boundaries.

## Estimate methodology

A "commit" here ≈ one bullet in the Changes list. Santa-method convergence rounds add review-fix commits on top — TTL controller (MEMORY) took ~19 commits across 11 rounds, so a 1.5–2× multiplier on the bullet counts is realistic.

| Phase | Bullets | Estimated commits incl. review |
| --- | --- | --- |
| 5a pre-work (ScanTracker contention refactor) | 1 | 1–2 |
| 5a wiring | 7 | 8–14 |
| 5b | 8 | 10–16 |
| 5c.0 pendingPrefetchBytes | 1 | 2–3 |
| 5c proper | 8 | 12–20 |

## Test infrastructure

- `PinLeakAssertions` helper (new in cached-fs-core's `test/` tree): captures `cache.refreshStats()` before each test, asserts `numExclusive == 0 && numShared == 0` after. Used across 5b/5c.
- `CountingReadFile` wrapper (test-only in cached-fs-hadoop): wraps a real `ReadFile` and counts `preadv` invocations. Avoids the `HadoopReadFile is final` blocker.
- Integration tests reuse the existing MiniDFSCluster/Failsafe infrastructure (phase 4).

## Open follow-ups

- `scanTrackers` eviction: today the bootstrap holds them for JVM lifetime. If long-lived JVMs (Spark history servers) accumulate many one-shot scan ids, consider Caffeine with size+expiry. Defer until observed.
- `IoStatistics` per-stream → per-process aggregation strategy: the close-time roll-up in 5a is the minimum. Consider a ring buffer of recent N streams for debugging.
- README integration: each phase's config keys go into the existing `Configuration` table in README; the divergence-list bullets in `AsyncDataCache` / `CacheTTLController` get an additional entry per phase noting the reader-side changes.

## Recommendation

Start with **Phase 5a pre-work (ScanTracker contention refactor)** as the first commit — it's stand-alone and unblocks the rest. Then **Phase 5a wiring** as its own santa-method convergence. Phase 5b and 5c.0 + 5c follow as separate convergence passes once 5a lands.
