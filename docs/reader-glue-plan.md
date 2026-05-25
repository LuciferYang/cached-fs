# Reader Glue Port Plan — velox §5

> **Status:** draft v3, 2026-05-25. Plan-only; no code changes yet. Updated after round-2 review (commit 8abafc2). Fixes factual errors (FSDataInputStream not final, velox flag default 80 not 50, setFirstUse no-arg/package-private, IoStatisticsAdapter name table now derived from `StreamStatisticNames`), resolves the 5c.0 vs 5c-proper increment double-count, specifies the test-seam plumbing, replaces the impossible no-op subclass off-switch with a flag inside ScanTracker, and tightens lifecycle/concurrency invariants.

## Goal

Port velox's `CachedBufferedInput` + `ScanTracker` reader-side wiring (velox-file-read-cache.md §5) into cached-fs. Today the Hadoop decorator's `CachingInputStream` does per-chunk fetches via `findOrCreate`; it does not coalesce multi-chunk IO, prefetch, or feed `ScanTracker` / `IoStatistics`.

## Existing inventory (verified against HEAD = 8abafc2)

| Piece | Location | State |
| --- | --- | --- |
| `ScanTracker` | `cached-fs-core/.../tracker/ScanTracker.java` | `public final class`. Refs/reads via `recordReference(TrackingId, long)` / `recordRead(TrackingId, long)`; snapshot via `data(id)`. Single `ReentrantLock` over a `HashMap<TrackingId, MutableData>` — **JVM-wide serialization point under concurrent readers sharing one tracker**. **Not wired** into any reader. (Note: `final` modifier means an off-switch "return a no-op subclass" is infeasible — see §5a off-switch design.) |
| `TrackingId` | same package | Record wrapping a single `int id`. Construction: `TrackingId.of(int node, int streamKind)` packs `(node << 5) \| streamKind`. `new TrackingId(0)` and `TrackingId.of(0, 0)` both yield id=0 (non-empty). `TrackingId.EMPTY = new TrackingId(-1)` and `ScanTracker.recordReference/recordRead` early-return on `isEmpty()`. There IS a 2-arg static factory `TrackingId.of(int, int)`; the inventory note is "no 2-arg *constructor*", which is technically true for records but doesn't matter at the call site. |
| `TrackingData` | same package | Record exposing `referencedBytes`, `lastReferencedBytes`, `readBytes`, plus methods `readPct()` and `adjustedReadPct()`. **There is no `readDensity()` method.** Velox uses `readDensity = readBytes / (1 + referencedBytes)` (0..1) — if cached-fs needs it, we add a helper on `TrackingData` (phase 5c only). |
| `IoStatistics` | `cached-fs-core/.../stats/IoStatistics.java` | Counters via `incRead(long)`, `incPrefetch(long)`, `incSsdRead(long)`, `incRamHit(long)`, `incRawOverreadBytes(long)`, plus latency methods. Most `inc*` increment both an event-count and a byte-total; `incRawOverreadBytes` increments only a byte total (no count partner). Public getters expose both flavors (e.g. `read()` returns count, `readBytes()` returns bytes). **Not wired** into any reader. |
| `CoalesceIo` | `cached-fs-core/.../CoalesceIo.java` | Gap-based grouping primitive over generic ranges. Stateless utility, ready to use. |
| `CoalescedLoad` | `cached-fs-core/.../CoalescedLoad.java` | **Abstract class** — subclass implements `loadData(boolean prefetch)`. The framework's `runLoad` performs `exclusiveToShared` itself. The only concrete subclass today is `CoalescedLoadTest.TestLoad` (test-only). Phase 5b bypasses `CoalescedLoad` and drives `CoalesceIo` + `preadv` directly (see §5b decision). |
| `CachingInputStream` | `cached-fs-hadoop/.../CachingInputStream.java` | Per-chunk fetch only via `copyChunk` → `findOrCreate` → `Hit`/`Exclusive`/`Waiting`. Implements `Seekable` + `PositionedReadable`. Constructor is **package-private** (grep-verified: only call site is `CachedFileSystem.open()` in the same package). |
| `HadoopReadFile` | `cached-fs-hadoop/.../HadoopReadFile.java` | `public final class HadoopReadFile implements ReadFile`. Final → not Mockito-spy-able directly. Phase 5b's `CountingReadFile` test wrapper implements `ReadFile` and delegates to the real one; the test injects it via the new `HandleOpener.readFileFactory` seam (see §5b test plumbing). |
| `CacheBootstrap` | `cached-fs-hadoop/.../CacheBootstrap.java` | Per-JVM singleton. Holds `ramCache`, `ssdCache`, `stringIds`, `handleFactory`, `loadQuantumBytes`, `ttlController`, `openersByEndpoint`. **No `close()` method on the public surface** today; teardown is `uninstallForTesting()` only (production code never tears down). `AsyncDataCache` has no `pendingPrefetchBytes()` counter today. |

## Hadoop API context

- `FSDataInputStream` is `public class FSDataInputStream extends DataInputStream` — **NOT final**. The decorator could in principle subclass it; we choose not to because Hadoop's standard cascade for `IOStatisticsSource` already picks up the inner stream's stats when the inner stream implements the interface, so subclassing `FSDataInputStream` would buy us nothing and risk breaking instanceof checks downstream readers rely on.
- Hadoop 3.3+ defines `org.apache.hadoop.fs.statistics.IOStatisticsSource` with `getIOStatistics()`. When the inner `InputStream` of an `FSDataInputStream` implements `IOStatisticsSource`, Hadoop's standard tooling (S3A, ABFS, Spark's `IOStatisticsContext`) picks the stats up via `FSDataInputStream.getIOStatistics()` → delegate to wrapped stream.
- Counter names live in `org.apache.hadoop.fs.statistics.StreamStatisticNames`. The Phase 5a adapter MUST map our counters to the EXACT constants there. Verified mapping table:

  | cached-fs `IoStatistics` counter | Hadoop `StreamStatisticNames` constant |
  | --- | --- |
  | `read()` / `readBytes()` | `STREAM_READ_BYTES = "stream_read_bytes"` |
  | `ramHit()` / `ramHitBytes()` | `STREAM_READ_CACHE_HIT = "stream_read_cache_hit"` (counts, not bytes — see note) |
  | `prefetch()` / `prefetchBytes()` | `STREAM_READ_PREFETCH_OPERATIONS = "stream_read_prefetch_operations"` (count); `STREAM_READ_PREFETCH_BYTES_DISCARDED` for discarded |
  | `ssdRead()` / `ssdReadBytes()` | no standard Hadoop constant; expose under cached-fs-specific name `cachedfs_stream_ssd_read_bytes` |
  | `rawOverreadBytes()` | no standard Hadoop constant; expose under `cachedfs_stream_raw_overread_bytes` |

  Note: Hadoop's `STREAM_READ_CACHE_HIT` is an event-count name, not a byte-name. We publish BOTH (`stream_read_cache_hit` as count and a cached-fs-specific `cachedfs_stream_read_cache_hit_bytes`) to stay compatible with `IOStatisticsContext` aggregators while still exposing bytes for cached-fs metrics tooling. Tests assert both names appear.

- `CachedFileSystem.openFile()` / `open(PathHandle)` bypass the decorator today (Limitations section of README). Phase 5 does not change that.

## Velox §5 mapping → Java port

- `ScanTracker`'s scope in velox is per-(TableScan, stream). The Hadoop layer has no column visibility, so scope shrinks to **per-(scanId, file)** with `TrackingId.of(0, 0)` standing in for "the whole file as one virtual stream". Per-column tracking belongs to the consumer.
- `CachedBufferedInput` is per-`Reader` (i.e. per file) in velox. Cross-file coalescing is intentionally out of scope — velox itself doesn't cross `CachedBufferedInput` boundaries.
- `prefetchPct_` (dormant in OSS velox §5.2.7 per `CacheInputStream.h:187`) we leave dormant.

## Decisions (previously open questions)

1. **scanId source.** Phase 5a wires `fs.cached.scan-id` as a Hadoop `Configuration` key, plus a thread-local fallback `CacheBootstrap.currentScanId()` for callers (Spark `TaskContext.get().stageId() + "-" + attemptNumber`) that prefer to set it imperatively. **Precedence (lifted from §5a #5):** `currentScanId()` (thread-local) wins; falls back to `conf.getTrimmed(SCAN_ID)` (`""` normalized to `null`); finally defaults to the literal string `"default"`. The `trackerFor` chokepoint normalizes a null/empty string to `"default"` so callers can't bypass.
2. **Coalesce gap default.** `fs.cached.coalesce.max-gap-bytes` default is `min(512 KiB, loadQuantumBytes / 16)`. Rationale: velox's `kDefaultCoalesceDistance = 512 KiB` (at `velox/common/io/Options.h:66`) is the storage-side baseline; scaling by `loadQuantumBytes / 16` keeps the gap proportional when operators tune loadQuantum down (e.g. for small-file workloads with `loadQuantum=1 MiB`, the gap is 64 KiB, not the full 512 KiB).
3. **Prefetch executor scope.** **Single shared executor on `CacheBootstrap`** with a bounded work queue (cap 1024 tasks; `CallerRunsPolicy` rejection so a runaway tenant back-pressures the consumer thread instead of OOM-ing the queue). Configurable thread count: `fs.cached.prefetch.threads` default `Runtime.getRuntime().availableProcessors()`. Queue size config: `fs.cached.prefetch.queue` default 1024.
4. **IoStatistics exposure.** `CachingInputStream implements IOStatisticsSource`. Bridge via `IoStatisticsAdapter` (cached-fs-hadoop) using the name table in §Hadoop API context.

## Phase 5a — ScanTracker + IoStatistics wiring (small, foundation)

### Phase 5a-prework (mandatory; ships in its own commit before any 5a wiring)

**Goal:** make `ScanTracker` safe to share across N concurrent readers without contention or torn snapshots.

Refactor `ScanTracker` storage:

- Replace `private final ReentrantLock lock` + `HashMap<TrackingId, MutableData>` with `ConcurrentHashMap<TrackingId, AtomicReference<TrackingData>>`.
- `recordReference(id, bytes)` does an `updateAndGet(prev -> new TrackingData(prev.referencedBytes + bytes, /*lastReferencedBytes=*/bytes, prev.readBytes))`. `recordRead` analogous on `readBytes`. Atomic-CAS on an immutable record — single-writer commit per (id, event), no inter-field tearing for readers. Cost: one allocation per event, acceptable since refs/reads fire once per `readFullyFromCache` call (not per chunk — see §5a wiring step 6).
- `data(id)` returns `map.getOrDefault(id, REF_TO_EMPTY).get()`. Single volatile read, coherent across the three fields.
- **Off-switch primitive:** add `static final ScanTracker DISABLED` (a singleton instance whose `recordReference`/`recordRead` short-circuit because `id.isEmpty()` — we construct it with a flag and check at top of each method). Solves the "no-op subclass" infeasibility (the class is `final` and we keep it that way).

Acceptance for prework:
- Existing `ScanTrackerTest` passes unchanged.
- New unit test: 8 threads call `recordReference(id, 1)` 1M times each on a shared tracker; `data(id).referencedBytes()` must equal exactly 8 × 1M (no lost updates from CAS retries). Snapshot under concurrent writes never produces `lastReferencedBytes > referencedBytes` (run the test for 5s with a watchdog assertion).
- Microbenchmark in `cached-fs-core/src/test/.../tracker/ScanTrackerContentionBench.java` (JMH-style, written as a JUnit test that reports throughput; not part of CI gate, run on demand) showing the new design beats the locked baseline under 8-thread shared-tracker workload. We do NOT make CI gate on a perf threshold — too hardware-dependent — but we record the baseline.

### Phase 5a wiring (lands after pre-work)

Changes, in landing order (each its own commit so reviewers can size them):

1. **`CacheBootstrap.trackerFor(String scanId)`** (no other callers in this commit). New field `ConcurrentMap<String, ScanTracker> scanTrackers`. Normalizes null/empty to `"default"`. Eviction: none — tracker map size is bounded by distinct scan-ids seen during the JVM lifetime. Open-follow-up below documents a Caffeine bound if metrics show `scanTrackers.size() > 10000` in any 24h window.
2. **`CacheBootstrap.currentScanId()`** new `ThreadLocal<String>` field + `static String currentScanId()` getter + `static AutoCloseable withScanId(String scanId)` helper. **The helper is the only safe API**: it sets the value and `close()` REMOVES the slot (not just sets null) so a reused worker thread doesn't inherit a stale id. The plan REQUIRES integrators (Spark/MR adapters) to use `withScanId(...)` in a try-with-resources or task-completion listener. Direct `ThreadLocal.set()` callers must pair with `.remove()`; this is documented in the bootstrap javadoc with the warning "Stale ThreadLocal scanIds on pooled worker threads will cross-tenant-contaminate ScanTracker counters."
3. **`CachedFsConfig.SCAN_ID = "fs.cached.scan-id"`** added; README config-reference table picks it up; no behavior change yet.
4. **`CachingInputStream` constructor gains parameters** for `ScanTracker tracker`, `TrackingId trackingId`, `IoStatistics ioStats`. (The constructor is package-private with a single grep-verified call site — risk is low; the single call site updates in the same commit.) New fields stored. javadoc on `trackingId` says "Hadoop-tier file-level tracking; column-level tracking is the consumer's job."
5. **`CachedFileSystem.open()`** resolves scanId via the precedence in Decisions §1, looks up tracker via `b.trackerFor(scanId)`, constructs `IoStatistics ioStats = new IoStatistics()`, passes `(tracker, TrackingId.of(0, 0), ioStats)` to the `CachingInputStream` constructor.
6. **`CachingInputStream.readFullyFromCache`** records at the boundaries (NOT per-chunk, to keep tracker contention low):
   - Top: `tracker.recordReference(trackingId, length); ioStats.incRead(length);`.
   - Bottom: `tracker.recordRead(trackingId, length);`.
   - Per-chunk inside the existing switch, on `case FindResult.Hit`: `ioStats.incRamHit(copyLen);`. No separate `cache.exists` probe — the hit signal is the sealed-result branch.
7. **`IoStatisticsAdapter`** + `CachingInputStream implements IOStatisticsSource`. Adapter maps using the name table in §Hadoop API context. Adapter is a `final class` in cached-fs-hadoop.
8. **`CacheBootstrap.aggregateIoStats`** new field, type `IoStatistics`, but ALL its counters use `LongAdder` internally (different concrete class than the per-stream `IoStatistics`). Spec a new `IoStatistics.merge(IoStatistics other)` (in cached-fs-core) that snapshots the source counters once and adds them into `this` using `LongAdder.add` — commutative, thread-safe under concurrent close. Per-stream `IoStatistics` uses `AtomicLong` (unchanged); only the aggregator uses `LongAdder`. `CachingInputStream.close()` uses an `AtomicBoolean aggregated` to guard against double-merge if Hadoop calls close twice.

### Off-switch

`fs.cached.scan-tracker.enabled` (default `true`). When false, `trackerFor(...)` returns `ScanTracker.DISABLED`. `IoStatistics` continues to record (it's independent). The dedicated "Off-switch test" asserts:
- `ioStats.readBytes() == fullReadLength` (stats keep working independently).
- `tracker.data(id) == TrackingData.EMPTY` because `DISABLED.recordReference/recordRead` short-circuits.

### Acceptance tests

- **Multi-chunk-read test**: `fs.cached.load-quantum-bytes=1048576` (1 MiB), read 8 MiB. Assert `tracker.data(id).referencedBytes() == 8 MiB`, `readBytes() == 8 MiB`, `readPct() == 100`.
- **Warm re-read**: cold-read 4 MiB, then re-read same range. Assert `ioStats.ramHitBytes() == 4 MiB`.
- **Concurrent readers**: 8 threads read disjoint 1 MiB windows of the same file under same scan id. Assert `tracker.data(id).referencedBytes() == 8 MiB`, no `ConcurrentModificationException`. `adjustedReadPct()` is always non-negative throughout the test (sampled every 100 ms by a watchdog thread).
- **Off-switch**: as described above.
- **IOStatisticsSource bridge**: open via `FileSystem.get(...).open(path)`, cast to `IOStatisticsSource`, query `getIOStatistics().counters()`. Assert both `stream_read_bytes` and `cachedfs_stream_read_cache_hit_bytes` are present.
- **ThreadLocal cleanup invariant**: `try (var ignored = CacheBootstrap.withScanId("scan-A"))` then assert outside the block `CacheBootstrap.currentScanId() == null` (slot removed, not just nulled — verify via reflection or a watchdog test that runs many tasks on one pooled thread and confirms no leak).
- **Aggregator double-merge**: simulate two `close()` calls on the same `CachingInputStream`. Assert `bootstrap.aggregateIoStats.readBytes()` increments exactly once.
- **PinLeakAssertions** also applied here as a regression net (free).

### Risk

Pre-work: medium-low (refactor of a small, well-tested class). Wiring: low.

## Phase 5b — Multi-chunk coalescing (medium)

Cut IO syscalls for sequential reads. When a single positional read crosses N missed chunks, fill all N via one `ReadFile.preadv` instead of N. Drive `CoalesceIo` + `preadv` directly (bypass abstract `CoalescedLoad`).

### Algorithm

1. **Walk + classify.** For each chunk in `[startChunk, endChunk]`, call `findOrCreate(key, size, /*contiguous=*/false)`. Push the result onto a `List<Resolved>`:
   - `Hit` → `Resolved.hit(pin)`.
   - `Exclusive` → `Resolved.exclusive(pin)`.
   - `Waiting` → ABORT: release every pin held so far (Hit pins via `pin.close()`, Exclusive pins via `releaseFailedExclusive`, in **descending offset order** so any peer waiting on a low-offset chunk gets unblocked first), await the future, increment a per-call `restartCount`, restart the walk from `startChunk`. If `restartCount > fs.cached.coalesce.max-restarts` (default 3, configurable), fall back to per-chunk `copyChunk`. The retry counter is per-`readFullyFromCache` invocation; it does NOT persist across reads. The fallback path uses the existing per-chunk loop verbatim — that path's Waiting handling is unbounded and guaranteed to make progress (peer either promotes → Hit, or fails → releaseFailedExclusive → fresh findOrCreate → Exclusive).
   - **Partial-pin abort exception chain.** During abort, if any `pin.close()` or `releaseFailedExclusive` throws, accumulate via `Throwable.addSuppressed`; rethrow the original Waiting trigger (or, if abort itself was triggered by a Throwable in step 4 below, the original throwable). Symmetric with the preadv-throw path in step 4.
2. **Coalesce Exclusives.** Group consecutive `Resolved.exclusive(...)` entries via `CoalesceIo` with `maxGap = fs.cached.coalesce.max-gap-bytes`. Apply group cap `fs.cached.coalesce.max-chunks-per-group` — see §Group sizing.
3. **Issue IO per group.** For each Exclusive group, concatenate `dataRanges(chunkSize)` from every member in offset order. One `handle.readFile().preadv(groupStartOffset, buffers)` per group. `ioStats.incRawOverreadBytes(gapBytes)` per group, where `gapBytes` is the sum of gap bytes the coalescer absorbed (0 for adjacent chunks). The increment site is here in step 3 (referenced by §5b acceptance test).
4. **Promote.** For each Exclusive in the group, call `exclusiveToShared(/*ssdSavable=*/true)`. On any throw during steps 3 or 4: release every Exclusive that wasn't yet promoted via `releaseFailedExclusive` (descending offset), close every Hit/Shared pin already promoted (suppressed-exception chain), rethrow.
5. **Copy out.** For each `Resolved` (Hit pin, or promoted Exclusive→Shared), `copyOutOfEntry(pin.entry(), ...)`. Close pins in finally (descending offset).
6. **TTL interaction.** TTL's `applyTTL` calls `removeFileEntries` on the RAM tier under shard mutex; an entry held as `Exclusive` reports `numPins != 0` so it stays retained (`CacheShard.removeFileEntries` at line 187). No special handling needed.
7. **Eviction interaction.** `setFirstUse()` is already called on the miss path inside `CacheShard.findOrCreate` (line 113). The promoted Shared pins inherit the firstUse flag set during creation. The plan does NOT call `setFirstUse()` from the coalesce path — that method is no-arg, package-private to cached-fs-core, and would be redundant. (Earlier v2 incorrectly said `setFirstUse(true)`; method is `setFirstUse()`.)

### Group sizing

- `fs.cached.coalesce.max-chunks-per-group` default = `max(2, min(16, totalRamBytes / loadQuantumBytes / 16))`. With 1 GiB cache and `loadQuantum=8 MiB`, that's `1024 / 8 / 16 = 8` chunks ⇒ peak pin 64 MiB. With 256 MiB cache → 2 chunks (16 MiB peak). The cap auto-scales to the configured RAM so a small cache can't be exhausted by one read.
- The cap is the LESSER of the configured value and the auto-scaled value at FS init time.

### Off-switch

`fs.cached.coalesce.enabled` (default `true`). When false, `readFullyFromCache` calls the existing per-chunk `copyChunk` loop unchanged. The disabled path reads ONLY 5a-introduced fields (`tracker`, `trackingId`, `ioStats`); the 5b-introduced retry counter and `Resolved` list are method-local in the new code path and absent from the fallback. Documented in the off-switch's test as: "disabled path does not invoke `CoalesceIo` or hold more than one pin at a time."

### Test plumbing (CountingReadFile seam)

Today `CachedFileSystem.openHandleForKey` (around line 360 of `CachedFileSystem.java`) constructs `new HadoopReadFile(fs, p, key, size)` directly — no injection point. The seam adds:

- **`CacheBootstrap.HandleOpener` extension.** The opener interface gains an optional `ReadFile wrap(ReadFile inner)` default-method (returns `inner`). Tests register a wrapping opener via `CacheBootstrap.installOpener(endpoint, ...)` that returns `new CountingReadFile(inner)`.
- **`CountingReadFile`** lives in `cached-fs-hadoop/src/test/`, implements `ReadFile`, delegates everything to the inner real `HadoopReadFile`, increments an `AtomicLong preadvCalls` on each `preadv`.
- Acceptance: tests assert `preadvCalls` directly. No Mockito needed.

### Acceptance tests

- **One preadv per coalesced group**: `loadQuantum=1 MiB`, `coalesce.max-gap-bytes=512 KiB`. Cold-read 4 MiB spanning 4 chunks. Assert `countingReadFile.preadvCalls == 1`.
- **Group cap respected**: `coalesce.max-chunks-per-group=2`, cold-read 4 MiB. Assert `preadvCalls == 2`.
- **Disabled fallback**: `coalesce.enabled=false`. Assert `preadvCalls == 4`.
- **Mixed Hit/Miss**: prefill chunks 0 and 2 (warm), cold-read 4 chunks. Assert two `preadv` calls.
- **Waiting restart-and-fallback**: 2-thread test where T1 holds chunk 1 Exclusive (artificially blocked via a test seam in HadoopReadFile that latches one preadv); T2 reads chunks 0..2. Assert T2 either restarts and completes (`restartCount` in `[1,3]`) OR falls back to per-chunk (`restartCount == 3+1` and per-chunk path produces correct bytes). Either outcome is acceptance; the assertion is correctness + no deadlock + `PinLeakAssertions.assertNoLeak()`.
- **Pin-leak stress** (new harness `PinLeakStressTest`): 100 threads × 1000 random-offset reads × forced-Waiting injection rate 5%. Assert final `numShared == 0 && numExclusive == 0`. (CI duration ~ 30s.)
- **rawOverreadBytes accounting**: cold-read two chunks with a 512 KiB gap. Assert `ioStats.rawOverreadBytes() == 512 KiB`. Adjacent chunks: assert it stays 0.

### Risk

Medium. Multiple exclusive pins simultaneously held; abort-and-restart-on-Waiting policy keeps it tractable; the pin-leak stress is the load-bearing test.

## Phase 5c.0 — `AsyncDataCache.pendingPrefetchBytes()` (precondition, independent)

**Lands first, no callers yet.** Pure additive change to `AsyncDataCache`.

- New field `private final java.util.concurrent.atomic.LongAdder pendingPrefetchBytes = new LongAdder()`.
- New public method `long pendingPrefetchBytes() { return pendingPrefetchBytes.sum(); }`.
- New package-private methods `void incrementPendingPrefetch(long bytes)` / `void decrementPendingPrefetch(long bytes)` (sole increment/decrement sites; only callers will be in cached-fs-hadoop's prefetch path).

Acceptance:
- Unit test: increment by 4×1024, sum returns 4096; decrement by 1024, sum returns 3072; the AsyncDataCache builds and the existing test suite passes.
- The counter is NOT wired into anything in 5c.0; that wiring lives entirely in 5c-proper.

**This resolves the round-2 critical "double-increment" bug**: there is exactly ONE site that increments and ONE that decrements, both in `cached-fs-hadoop`'s prefetch task. 5c.0 only adds the counter; it does not specify or own any increment/decrement call site. See 5c step 4 below for the single increment+decrement pair (in the task, NOT before submit).

## Phase 5c — Async prefetch (large)

Issue async loads for the next loadQuantum-aligned chunk while the consumer reads the current one.

### Changes

1. **`CacheBootstrap`** new field `ExecutorService prefetchExecutor = new ThreadPoolExecutor(...)` with daemon `ThreadFactory` ("cached-fs-prefetch-N", daemon=true, uncaughtExceptionHandler logs at WARN). Bounded `ArrayBlockingQueue` of size `fs.cached.prefetch.queue` (default 1024). `RejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy()` (back-pressure to the caller thread).
2. **`CacheBootstrap.close()`** — new public method (introduced by this phase; the existing `uninstallForTesting()` delegates to it for tests):
   1. Atomically set `closed = true` (volatile or `AtomicBoolean`); subsequent `prefetchExecutor.submit` callers see this and skip submission.
   2. `prefetchExecutor.shutdown()` then `awaitTermination(timeout)` where timeout is `fs.cached.prefetch.shutdown-timeout-seconds` default 30. On timeout, `prefetchExecutor.shutdownNow()` AND a SECOND `awaitTermination(10s)`. If still not terminated, log at ERROR and proceed (pin leaks at this point are logged but tolerated — better than hanging indefinitely; tracking issue follow-up).
   3. After executor terminates: `handleFactory.closeAll()`; then `ssdCache.close()`; then `ramCache.close()`. (Pins held by abandoned tasks are leaked at this point; the executor.shutdownNow path already ensured the tasks were interrupted, and `releaseFailedExclusive` in the task's finally block runs even on interrupt before the cache is closed.)
3. **`CachingInputStream`** new field `volatile CompletableFuture<RawFileCacheKey> pendingPrefetch` (key-not-pin design avoids cross-thread `CachePin` handoff). Plus a `volatile boolean closed` field for the close-race interaction in §5c step 7.
   - **Single-mutator contract.** Plan declares: "`CachingInputStream` is not thread-safe; `read()/seek()/close()` may not be called concurrently from multiple threads. The implementation matches Hadoop's `FSDataInputStream` contract." (Document in class javadoc.) Cross-thread observability of `IOStatisticsSource.getIOStatistics()` is allowed; mutation of the read cursor is not.
4. **Consumer thread after each successful chunk-N read** (inside the existing main read loop):
   - If `pendingPrefetch == null` AND `position >= chunkNEnd - (loadQuantum * triggerTailFraction)` AND `tracker.data(trackingId).adjustedReadPct() >= prefetchPctThreshold` AND `admissionGate()`: submit the prefetch task for chunk N+1. The task is the SOLE site that increments/decrements `cache.pendingPrefetchBytes`.

   **Prefetch task body** (executor thread):
   ```text
   try {
     cache.incrementPendingPrefetch(chunkSize);  // sole increment
     try {
       FindResult r = cache.findOrCreate(nextKey, chunkSize, false);
       if (r instanceof FindResult.Hit hit) { hit.pin().close(); return nextKey; }
       if (r instanceof FindResult.Waiting w) { /* sibling fills; do not await; just return — see below */ return nextKey; }
       FindResult.Exclusive exc = (FindResult.Exclusive) r;
       CachePin excPin = exc.pin();
       try {
         fillExclusive(excPin, nextOffset, chunkSize);  // calls preadv
         excPin.exclusiveToShared(true).close();
       } catch (Throwable t) {
         releaseFailedExclusive(excPin); throw t;
       }
       ioStats.incPrefetch(chunkSize);
       return nextKey;
     } finally {
       cache.decrementPendingPrefetch(chunkSize);  // sole decrement, in finally
     }
   } catch (Throwable t) {
     // Future completes exceptionally; consumer's await on chunk N+1 will fall through to fresh findOrCreate.
     throw t;
   }
   ```
   The `Waiting` branch deliberately does NOT await — awaiting on the executor thread would block one of the bounded thread-pool workers indefinitely under contention. By returning the key, the consumer thread's subsequent `findOrCreate` either hits the now-warm entry or sees Waiting itself and uses the existing per-chunk Waiting handling (which IS allowed to block — it's the consumer thread). Net: prefetch under Waiting is a no-op rather than a warm-up.
   **TOCTOU on `pendingPrefetch`.** The submission uses `AtomicReferenceFieldUpdater` or `VarHandle.compareAndSet(this, null, future)` so two simultaneous submits cannot race even under a hypothetical multi-threaded consumer (defense in depth; the documented contract is single-threaded).
5. **Consumer thread on chunk N+1**: if `pendingPrefetch != null`, await its completion (or completion of replacement on retry). Whether the future resolved normally or exceptionally, fall through to fresh `findOrCreate` on the consumer thread. On normal completion, the entry is warm and `findOrCreate` hits. On exceptional completion, `findOrCreate` re-attempts the fill; the consumer never sees an error from the prefetch path (only at most a missed prefetch). After the await, reset `pendingPrefetch = null`. `ioStats.incPrefetch(chunkSize)` is set by the TASK (step 4), NOT by the consumer — so a future that errored doesn't double-count.

   **Eviction race acceptance.** Between prefetch completion and consumer await, the entry holds zero pins. If `TTL.applyTTL` or `clear()` evicts it in that window, the consumer's fresh `findOrCreate` is a miss → fills cleanly. This divergence from velox (which holds the pin across the handoff) is documented as a deliberate trade for the single-owner CachePin contract. New acceptance test: "TTL applyTTL between prefetch complete and consumer await — consumer still produces correct bytes."
6. **Admission gate** (`shouldPreload`-equivalent):
   - **Hard precondition:** `cache.pendingPrefetchBytes() + chunkSize <= prefetchBudgetBytes` where `prefetchBudgetBytes = fs.cached.prefetch.max-pending-bytes` (default `loadQuantumBytes * fs.cached.prefetch.threads * 4`). This is a deliberate distinct knob from `AsyncDataCache.Options.maxWriteRatio` (which is the RAM cache's write-staging budget, NOT a prefetch budget; v2 conflated these). The default ties prefetch headroom to thread count so a fully busy pool can have its working set in flight.
   - **Optional secondary signal:** if `MemoryMXBean.getHeapMemoryUsage()` shows heap > 90% used, deny even within budget. Wrapped behind `fs.cached.prefetch.heap-pressure-check.enabled` default `true`.
7. **`CachingInputStream.close()`** sets `closed = true` first, then `pendingPrefetch.cancel(false)` (if non-null). `cancel(false)` will not interrupt a running task; the task's finally block still decrements the counter. The consumer never touches the prefetch task's pin (single-owner contract preserved).
8. **`seek()` invalidation**: if `position` moves outside `[chunkN+1 start, chunkN+1 end)`, cancel `pendingPrefetch` the same way close does.

### Configuration knobs

- `fs.cached.prefetch.enabled` (default `false` — opt-in for the first release of 5c).
- `fs.cached.prefetch.threads` (default `Runtime.getRuntime().availableProcessors()`).
- `fs.cached.prefetch.queue` (default 1024).
- `fs.cached.prefetch.max-pending-bytes` (default `loadQuantumBytes * threads * 4`).
- `fs.cached.prefetch.trigger-tail-fraction` (default `0.25` — fire once position is in the last 25% of current chunk).
- `fs.cached.prefetch.read-density-threshold` (default 80 — matches velox `FLAGS_cache_prefetch_min_pct` at `velox/flag_definitions/flags.cpp:118-121`; the previous v2 value of 50 was incorrect).
- `fs.cached.prefetch.heap-pressure-check.enabled` (default `true`).
- `fs.cached.prefetch.shutdown-timeout-seconds` (default 30).

### Acceptance tests

- **Sequential prefetch** (prefetch.enabled=true): 32 MiB read at `loadQuantum=1 MiB`. Assert `ioStats.prefetchBytes() / ioStats.readBytes() >= 0.5` after warmup. The test MUST explicitly enable the flag (default off).
- **Random suppression**: 1000 random 4 KiB positional reads. Assert `prefetchBytes() / readBytes() < 0.05`.
- **Admission backpressure**: set `max-pending-bytes` tiny enough to admit only 2 in-flight; verify subsequent submits are no-ops AND the counter never exceeds the budget.
- **Close cancels prefetch**: open stream, trigger prefetch, close stream before task runs. Within 1s: `cache.pendingPrefetchBytes() == 0`, `numExclusive == 0`. `PinLeakAssertions.assertNoLeak()`.
- **Prefetch failure**: inject preadv throw on next chunk. Consumer's read still succeeds (fresh findOrCreate fallback), `pendingPrefetchBytes` returns to 0, no pin leak.
- **Seek-away invalidation**: trigger prefetch for N+1, seek to N+10. `pendingPrefetch == null` and counter returns to 0.
- **Eviction-during-handoff** (new for v3): inject a `TTL.applyTTL` between prefetch task completion and consumer's await. Assert consumer reads correct bytes (via fresh findOrCreate fallback).
- **CallerRunsPolicy under saturation**: configure thread pool size 1, queue 1, submit two prefetch tasks; the third submit's CallerRunsPolicy must run on the consumer thread and behave correctly (decrement counter, no pin leak).

### Risk

High. Async pins are avoided via key-not-pin design; executor lifecycle now grounded in a real `close()` method with a tested shutdown sequence; admission gate uses a dedicated budget rather than co-opting the RAM cache's write-staging ratio. Worth its own santa-method convergence pass.

## Out of scope

- Per-column tracking (`TrackingId.of(node, streamKind)` for individual columns) — consumer-side concern.
- `DirectBufferedInput` (velox §5.3) — already supported via `fs.cached.enabled=false`.
- Adaptive next-quantum prefetch (velox §5.2.7) — dormant in OSS velox; revisit only on observed sequential-scan miss-rate above 25%.
- Cross-file prefetch coordination — explicitly out of velox §5.

## Estimate methodology

A "commit" ≈ one bullet in the Changes list. TTL controller convergence (per MEMORY) took 19 commits across 11 santa-method rounds against an initial ~7-bullet base → empirical multiplier ~2.7×. Reader-glue phases involve more concurrency surface than TTL, so we expect 2.5–3.5× rather than 1.5–2×.

| Phase | Bullets | Estimated commits incl. review (2.7×) |
| --- | --- | --- |
| 5a-prework (ScanTracker contention refactor) | 1 | 2–4 |
| 5a wiring | 8 | 18–28 |
| 5b | 8 | 18–28 |
| 5c.0 pendingPrefetchBytes | 1 | 2–4 |
| 5c proper | 8 | 22–35 |

The 5c estimate includes santa-method's well-known affinity for async/lifecycle code (TTL ran 11 rounds; 5c may exceed).

## Test infrastructure

- `PinLeakAssertions` helper (new in cached-fs-core's `test/` tree): captures `cache.refreshStats()` and asserts `numExclusive == 0 && numShared == 0` after the test. Used 5a/5b/5c (cheap, applied retroactively to 5a as a regression net).
- `PinLeakStressTest` (new in cached-fs-hadoop's `test/`): N-thread chaos test with random seek + forced Waiting injection. Phase 5b acceptance includes this.
- `CountingReadFile` (cached-fs-hadoop `test/`): wraps a real `ReadFile`, counts `preadv` calls. Plumbed via the new `HandleOpener.wrap(ReadFile)` default-method seam. No Mockito needed.

## Open follow-ups

- `scanTrackers` eviction trigger: when metrics show `bootstrap.scanTrackers.size() > 10_000` within a 24h window, file follow-up to introduce a Caffeine-bounded cache. Until then, accept JVM-lifetime growth.
- `IoStatistics` per-stream → per-process aggregation strategy: the close-time roll-up in 5a is the minimum viable observability. Consider a ring buffer of recent N streams for debugging.
- Per-file ScanTracker scoping (instead of per-scan): if the global scan-id default produces noisy density signals in real Spark workloads, consider keying by `(scanId, fileNum)` — adds a `Map<Long, TrackingData>` per tracker.

## README integration

- Phase 5a adds: `fs.cached.scan-id`, `fs.cached.scan-tracker.enabled` to the existing config-reference table.
- Phase 5b adds: `fs.cached.coalesce.enabled`, `fs.cached.coalesce.max-gap-bytes`, `fs.cached.coalesce.max-chunks-per-group`, `fs.cached.coalesce.max-restarts`.
- Phase 5c adds: the 8 prefetch knobs listed in §5c Configuration.
- Each phase appends to the velox-divergence list in the appropriate class javadoc (`CachingInputStream` for 5a/5b, `CacheBootstrap` for 5c executor, `AsyncDataCache` for the prefetch counter).

## Recommendation

Start with **Phase 5a-prework** as a standalone commit. Then **Phase 5a wiring** as its own santa-method convergence. Phase 5b follows as a separate convergence. Phase 5c.0 lands as a one-bullet commit before opening Phase 5c.
