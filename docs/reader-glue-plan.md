# Reader Glue Port Plan — velox §5

> **Status:** draft v4, 2026-05-25 (HEAD = 7157fea). Plan-only; no code changes yet. Updated after round-3 review. Fixes: nonexistent Hadoop constant, AtomicReference init NPE, HandleOpener.wrap with no call site, first-stripe prefetch gate dead-zone, acceptance test that masked regressions, CallerRunsPolicy synchronous fallback, executor close pin-leak bound, ThreadLocal close-on-different-thread, IoStatistics merge type asymmetry.

## Goal

Port velox's `CachedBufferedInput` + `ScanTracker` reader-side wiring (velox-file-read-cache.md §5) into cached-fs. Today the Hadoop decorator's `CachingInputStream` does per-chunk fetches via `findOrCreate`; it does not coalesce multi-chunk IO, prefetch, or feed `ScanTracker` / `IoStatistics`.

## Existing inventory (verified against current HEAD)

| Piece | Location | State |
| --- | --- | --- |
| `ScanTracker` | `cached-fs-core/.../tracker/ScanTracker.java` | `public final class`. Refs/reads via `recordReference(TrackingId, long)` / `recordRead(TrackingId, long)`; snapshot via `data(id)`. Single `ReentrantLock` over `HashMap<TrackingId, MutableData>` — JVM-wide serialization point under concurrent readers. Not wired into any reader. Class is `final` — off-switch must be a flag inside, not a subclass. |
| `TrackingId` | same package | Record wrapping a single `int id`. `TrackingId.of(int node, int streamKind)` packs `(node << 5) \| streamKind`. `TrackingId.EMPTY = new TrackingId(-1)`. `recordReference/recordRead` early-return on `isEmpty()`. |
| `TrackingData` | same package | Immutable record `(long referencedBytes, long lastReferencedBytes, long readBytes)`. Methods: `readPct()` and `adjustedReadPct()` (the latter uses denominator `referencedBytes - lastReferencedBytes`). `TrackingData.EMPTY` constant exists. No `readDensity()` method. |
| `IoStatistics` | `cached-fs-core/.../stats/IoStatistics.java` | `public final class` with `private final AtomicLong` counters. `incRead(long)` / `incPrefetch(long)` / `incSsdRead(long)` / `incRamHit(long)` increment both an event-count and a byte-total. `incRawOverreadBytes(long)` increments only a byte total. Getters return both flavors (e.g. `read()` returns count, `readBytes()` returns bytes). Not wired into any reader. **Final class with no merge primitive today**. |
| `CoalesceIo` / `CoalescedLoad` | `cached-fs-core/.../` | `CoalesceIo` is a stateless gap-grouping utility. `CoalescedLoad` is `public abstract class` with only a test-time subclass; phase 5b bypasses it and drives `CoalesceIo` + `preadv` directly. |
| `CachingInputStream` | `cached-fs-hadoop/.../CachingInputStream.java` | Per-chunk fetch via `copyChunk` → `findOrCreate` → `Hit`/`Exclusive`/`Waiting`. Constructor is package-private; single call site `CachedFileSystem.open()` (grep-verified). |
| `HadoopReadFile` | `cached-fs-hadoop/.../HadoopReadFile.java` | `public final class HadoopReadFile implements ReadFile`. Final → not Mockito-spy-able. Phase 5b uses a `CountingReadFile` test wrapper that implements `ReadFile`; injection point is the `ReadFile` constructed inside `CachedFileSystem.openHandleForKey` (`new HadoopReadFile(fs, p, key, size)`). |
| `CacheBootstrap` | `cached-fs-hadoop/.../CacheBootstrap.java` | Per-JVM singleton. Holds `ramCache`, `ssdCache`, `stringIds`, `handleFactory`, `loadQuantumBytes`, `ttlController`, `openersByEndpoint`. No `close()` method today; teardown is `uninstallForTesting()` only. `HandleOpener` is a `@FunctionalInterface` (single abstract method `open(String key)`); default methods are legal but require a real production call site to take effect. |

`AsyncDataCache` is `public final class`. No `pendingPrefetchBytes()` counter today.

## Hadoop API context

- `FSDataInputStream` is `public class extends DataInputStream` (NOT final). We deliberately do not subclass it — Hadoop's standard `IOStatisticsSource` cascade picks up an inner `InputStream` that implements `IOStatisticsSource`, and subclassing `FSDataInputStream` would risk breaking `instanceof` checks in downstream readers.
- Hadoop 3.3+ exposes `org.apache.hadoop.fs.statistics.IOStatisticsSource.getIOStatistics()`. When the inner stream implements it, `FSDataInputStream.getIOStatistics()` delegates automatically.
- Counter names in `org.apache.hadoop.fs.statistics.StreamStatisticNames`. Verified mapping (only listed below the constants that actually exist in Hadoop trunk):

  | cached-fs `IoStatistics` counter | Real Hadoop constant or cached-fs name |
  | --- | --- |
  | `read()` (count) | `STREAM_READ_OPERATIONS = "stream_read_operations"` |
  | `readBytes()` | `STREAM_READ_BYTES = "stream_read_bytes"` |
  | `ramHit()` (count) | `STREAM_READ_CACHE_HIT = "stream_read_cache_hit"` |
  | `ramHitBytes()` | `cachedfs_stream_cache_hit_bytes` (no Hadoop standard for bytes) |
  | `prefetch()` (count) | `STREAM_READ_PREFETCH_OPERATIONS = "stream_read_prefetch_operations"` |
  | `prefetchBytes()` | `STREAM_READ_PREFETCHED_BYTES = "stream_read_prefetched_bytes"` |
  | `ssdRead()` / `ssdReadBytes()` | `cachedfs_stream_ssd_read_operations`, `cachedfs_stream_ssd_read_bytes` (no Hadoop standard) |
  | `rawOverreadBytes()` | `cachedfs_stream_raw_overread_bytes` (no Hadoop standard) |
  | `prefetch_evicted_before_use` (new in 5c) | `cachedfs_stream_prefetch_evicted_bytes` (new IoStatistics counter — see §5c) |

  **The previous draft cited a non-existent `STREAM_READ_PREFETCH_BYTES_DISCARDED`; removed.** The 5a acceptance test asserts every entry from this table is present in `getIOStatistics().counters()`.

- `CachedFileSystem.openFile()` / `open(PathHandle)` still bypass the decorator (README Limitations); phase 5 does not change that.

## Velox §5 mapping → Java port

- `ScanTracker`'s scope in velox is per-(TableScan, stream). Hadoop layer has no column visibility; scope shrinks to **per-(scanId, file)** with `TrackingId.of(0, 0)`. Per-column tracking belongs to the consumer.
- `CachedBufferedInput` is per-`Reader` (per file) in velox. Cross-file coalescing is out of scope.
- `prefetchPct_` dormant in OSS velox (`CacheInputStream.h:187`, `prefetchPct_{200}`). Leave dormant.

## Decisions

1. **scanId source.** `fs.cached.scan-id` Hadoop config key, plus a `ThreadLocal<String>` accessor `CacheBootstrap.currentScanId()`. **Precedence:** `currentScanId()` first, then `conf.getTrimmed(SCAN_ID)`, then `"default"`. **withScanId helper is single-threaded only** — it returns an `AutoCloseable` whose `close()` MUST run on the same thread that set it. The javadoc forbids wiring `withScanId` into `TaskContext.addTaskCompletionListener` (Spark) or any callback that can fire on a different thread; in that case the integrator must `try/finally` strictly within the task body, OR use the `Configuration` key (which doesn't touch the ThreadLocal). The Spark integration guidance section in the plan says exactly: "Do not wire `withScanId` into `TaskContext.addTaskCompletionListener`. Use try-with-resources inside `RDD.compute`/`mapPartitions`, or set `fs.cached.scan-id` on the per-task Hadoop `Configuration`."
2. **Coalesce gap default.** `fs.cached.coalesce.max-gap-bytes = min(512 KiB, loadQuantumBytes / 16)`. Velox baseline 512 KiB (`velox/common/io/Options.h:66`) scaled down when operators tune loadQuantum small.
3. **Prefetch executor scope.** Single shared executor on `CacheBootstrap`. **Rejection policy: `DiscardPolicy` + an explicit `IoStatistics.incPrefetchSkipped(reason)` counter.** Phase 5c originally proposed `CallerRunsPolicy`, but that would run the prefetch task synchronously on the consumer thread under saturation — strictly worse than no prefetch. Discard-and-count lets the consumer's normal per-chunk path proceed and surfaces saturation via metrics.
4. **IoStatistics exposure.** `CachingInputStream implements IOStatisticsSource`. Bridge via `IoStatisticsAdapter` using the verified name table above.
5. **First-stripe prefetch gate (new in v4).** Phase 5c admission gates on **`readPct() >= prefetchPctThreshold`**, not `adjustedReadPct()`. Velox's `adjustedReadPct` is computed against the most-recent-batch-excluded denominator and returns 0 on the first stripe (suppressing prefetch on unproven streams). In the Java port, where `recordReference` fires once per `readFullyFromCache` call (not per chunk), `adjustedReadPct` would stay 0 forever for a single `readFully(0, fullFile)` consumer. Use plain `readPct` so the gate opens after the first reference batch shows non-zero `readBytes`. **Acceptance test "Sequential prefetch >= 0.5 ratio" must specify the consumer access pattern** (default test: many 4 KiB `read()` calls; supplementary test: one `readFully(0, 32 MiB)` to verify the gate still opens after at least one reference event).

## Phase 5a — ScanTracker + IoStatistics wiring (small, foundation)

### Phase 5a-prework (mandatory; ships in its own commit before 5a wiring)

**Goal:** make `ScanTracker` safe to share under concurrent readers and provide a coherent snapshot.

Refactor `ScanTracker` storage:

- Replace `ReentrantLock`+`HashMap` with `private final ConcurrentMap<TrackingId, AtomicReference<TrackingData>> data`.
- `private AtomicReference<TrackingData> refFor(TrackingId id) { return data.computeIfAbsent(id, k -> new AtomicReference<>(TrackingData.EMPTY)); }` — the `TrackingData.EMPTY` initial value avoids the `updateAndGet(prev -> prev.referencedBytes + bytes)` NPE.
- `recordReference(id, bytes)`: if `id.isEmpty()` return; else `refFor(id).updateAndGet(prev -> new TrackingData(prev.referencedBytes() + bytes, bytes, prev.readBytes()))`.
- `recordRead(id, bytes)`: analogous, only `readBytes` changes.
- `data(id)`: `var ref = data.get(id); return ref == null ? TrackingData.EMPTY : ref.get();` — single volatile read, coherent across all three fields.
- **Off-switch primitive.** Add a `static final ScanTracker DISABLED = new ScanTracker("__disabled__", 0, true)` where the constructor's `boolean disabled` flag short-circuits `recordReference`/`recordRead` to no-ops. (`ScanTracker` remains `final`.)

Allocation overhead: at ~100k reads/sec → 200k 40-byte `TrackingData` allocations/sec → ~8 MB/s, comfortably within young-gen tolerance. Under CAS-retry contention this can double; document in the prework's microbenchmark.

Acceptance for prework:
- Existing `ScanTrackerTest` passes unchanged after the refactor.
- New unit test: 8 threads each call `recordReference(id, 1)` 1M times on a shared tracker; final `data(id).referencedBytes()` must equal exactly `8 × 1M` (no lost updates).
- Snapshot-monotonicity test: 8 writer threads + 1 reader thread polling `data(id).adjustedReadPct()` every 1ms for 5s — assert no negative value observed.
- Microbenchmark (JUnit, not CI-gated): record throughput vs the locked baseline as a regression-watch artifact.

### Phase 5a wiring

Commits in landing order:

1. **`CacheBootstrap.trackerFor(String scanId)`** — adds `private final ConcurrentMap<String, ScanTracker> scanTrackers`. Normalizes null/empty/whitespace to `"default"`. No callers in this commit.
2. **`CacheBootstrap.currentScanId()` + `withScanId(String)`** — `ThreadLocal<String>` accessor plus `AutoCloseable withScanId(scanId)` that on `close()` calls `threadLocal.remove()`. Javadoc warns: "Single-threaded use only. Do NOT pair with Spark `TaskContext.addTaskCompletionListener` — close may run on a different thread and leak the slot. Use try-with-resources inside the task body."
3. **`CachedFsConfig.SCAN_ID = "fs.cached.scan-id"`** — README config-reference picks up the key. No behavior change.
4. **`CachingInputStream` constructor signature change** — gains `ScanTracker tracker`, `TrackingId trackingId`, `IoStatistics ioStats`. Constructor is package-private with a single grep-verified call site (`CachedFileSystem.open()`). The same commit updates the call site so the build stays green.
5. **`CachedFileSystem.open()` plumbing** — resolves scanId via the Decisions §1 precedence, calls `b.trackerFor(scanId)`, instantiates `IoStatistics ioStats = new IoStatistics()`, passes `(tracker, TrackingId.of(0, 0), ioStats)` to `CachingInputStream`.
6. **`CachingInputStream.readFullyFromCache` recording**:
   - Top: `tracker.recordReference(trackingId, length); ioStats.incRead(length);`.
   - Bottom: `tracker.recordRead(trackingId, length);`.
   - Per-chunk inside the existing switch, on `case FindResult.Hit`: `ioStats.incRamHit(copyLen)`. (No separate `cache.exists` probe — sealed-result branch is the hit signal.)

   **Velox-faithful caveat:** `adjustedReadPct()` returns 0 on the very first reference batch because the denominator collapses. This suppresses prefetch on unproven streams. Decisions §5 picks `readPct()` over `adjustedReadPct()` for the prefetch gate to avoid the dead-zone on `readFully(0, fullFile)` consumers.
7. **`AggregatedIoStatistics`** new class in cached-fs-core (`stats/AggregatedIoStatistics.java`) — `public final class` with the same counter surface as `IoStatistics` but backed by `LongAdder` per counter. New `add(IoStatistics source)` method snapshots the source via its public byte/count getters (one volatile read per counter) and `LongAdder.add`s into self. Commutative and thread-safe under concurrent callers.

   Spec: `IoStatistics` stays unchanged (final, `AtomicLong` fields). `CacheBootstrap.aggregateIoStats` is of type `AggregatedIoStatistics`. `CachingInputStream.close()` uses an `AtomicBoolean aggregated` guard (set-once, never reset) so double-close is idempotent: `if (aggregated.compareAndSet(false, true)) bootstrap.aggregateIoStats.add(this.ioStats);`.
8. **`IoStatisticsAdapter` + `CachingInputStream implements IOStatisticsSource`** — adapter maps each cached-fs counter to either a real `StreamStatisticNames` constant or a `cachedfs_`-prefixed name per the table above. Adapter is `final class` in cached-fs-hadoop.

### Off-switch

`fs.cached.scan-tracker.enabled` (default `true`). When false: `trackerFor(...)` returns `ScanTracker.DISABLED`. `IoStatistics` continues to record independently. **Master metrics off-switch:** `fs.cached.metrics.enabled` (default `true`). When false: `CachingInputStream` is constructed with `IoStatistics.NO_OP` (new singleton in `IoStatistics` whose `inc*` methods return without touching state — keeps the public API while skipping the AtomicLong writes). With both flags off, the codepath is functionally identical to today's per-chunk fetch (verified by the off-switch acceptance test).

### Acceptance tests

- **Multi-chunk read**: `fs.cached.load-quantum-bytes=1048576` (1 MiB), read 8 MiB. Assert `tracker.data(id).referencedBytes() == 8 MiB`, `readBytes() == 8 MiB`, `readPct() == 100`.
- **Warm re-read**: cold-read 4 MiB, re-read same range. Assert `ioStats.ramHitBytes() == 4 MiB`.
- **Concurrent readers**: 8 threads, disjoint 1 MiB windows, same scanId. Assert `referencedBytes() == 8 MiB`, no CME, no deadlock, and a watchdog thread observes no negative `adjustedReadPct` over the test's 5s lifetime.
- **Off-switch matrix**: both flags off, repeat the multi-chunk read. Assert `tracker.data(id) == TrackingData.EMPTY`, `ioStats.readBytes() == 0` (NO_OP IoStatistics doesn't count), behavior matches today's per-chunk path.
- **IOStatisticsSource bridge**: cast inner stream to `IOStatisticsSource`. Assert every name from the §Hadoop API context table appears in `getIOStatistics().counters()` after a read.
- **ThreadLocal slot removal**: `try (var ignored = bootstrap.withScanId("scan-A")) { … }`. After the block, `bootstrap.currentScanId() == null`.
- **Double-close idempotence**: call `close()` twice on the same `CachingInputStream`. Assert `bootstrap.aggregateIoStats.readBytes()` increments exactly once.
- **PinLeakAssertions** regression net (free).

### Risk

Pre-work medium-low (refactor of a small class). Wiring low.

## Phase 5b — Multi-chunk coalescing (medium)

Cut IO syscalls for sequential reads. When a positional read crosses N missed chunks, fill via one `preadv`. Drive `CoalesceIo` + `preadv` directly.

### Algorithm

1. **Walk + classify.** For each chunk in `[startChunk, endChunk]`, call `findOrCreate(key, size, false)`. Build a `List<Resolved>`:
   - `Hit` → `Resolved.hit(pin)`.
   - `Exclusive` → `Resolved.exclusive(pin)`.
   - `Waiting` → **abort sub-routine**: release every pin held so far in ascending offset order (pins were acquired in ascending order; releasing in the same order matches stack discipline and is simpler to reason about than the v3 descending-order claim). Hit pins via `pin.close()`; Exclusive pins via `releaseFailedExclusive`. If any release throws, accumulate via `Throwable.addSuppressed` and continue releasing the rest; rethrow the original abort trigger only after every pin is released. Await the future. Increment a per-call `restartCount`; restart from `startChunk`. **Bound:** `fs.cached.coalesce.max-restarts` (default 3). On exceeding the bound, fall back to the per-chunk `copyChunk` path.
2. **Coalesce Exclusives.** Group consecutive `Resolved.exclusive(...)` via `CoalesceIo` with `maxGap = fs.cached.coalesce.max-gap-bytes`. Apply group cap (see §Group sizing).
3. **Issue IO per group.** Concatenate `dataRanges(chunkSize)` from every member; one `handle.readFile().preadv(groupStartOffset, buffers)` per group. `ioStats.incRawOverreadBytes(gapBytes)` is called HERE, where `gapBytes` is the bytes the coalescer absorbed (`groupSpan - sum(chunkSize)`); 0 for purely-adjacent chunks.
4. **Promote.** For each Exclusive in the group, `exclusiveToShared(true)`. On any throw in steps 3 or 4: release every still-Exclusive in ascending order via `releaseFailedExclusive`; close every already-promoted Shared pin and every Hit pin (suppressed-exception chain); rethrow.
5. **Copy out + close pins** in ascending offset order (finally block).

### Group sizing

`fs.cached.coalesce.max-chunks-per-group` default = `max(2, min(16, totalRamBytes / loadQuantumBytes / 16))`. With 256 MiB cache and `loadQuantum=8 MiB`, that's `max(2, min(16, 2)) = 2`. **When the auto-scaled cap is exactly 2 the coalesce path adds non-trivial overhead for marginal benefit** — the spec gates: when `cap == 2` AND `fs.cached.coalesce.always-on=false`, `coalesce.enabled` is treated as `false`. Operators with small caches who want coalescing anyway set `always-on=true`. The default flips coalescing off automatically on under-sized caches to avoid pessimization.

### Off-switch

`fs.cached.coalesce.enabled` (default `true`, but auto-disabled when cap==2 per Group sizing). Disabled path reads only 5a-introduced fields (`tracker`, `trackingId`, `ioStats`); the Resolved list and restart counter are method-local in the new code path, absent from the fallback.

### Test plumbing (CountingReadFile seam — concrete)

Today `CachedFileSystem.openHandleForKey` creates `new HadoopReadFile(fs, p, key, size)` directly (verified at the start of the method body around line 346). The seam plumbed in phase 5b:

- **`CachedFileSystem` gains a protected `ReadFile newReadFile(FileSystem fs, Path p, String key, long size)`** method that returns `new HadoopReadFile(...)` by default. Phase 5b's test class extends `CachedFileSystem` (allowed: `CachedFileSystem` is `public final` today — phase 5b removes the `final` modifier as part of the test-seam change, with a justification comment noting the seam intent).
- Test subclass `WrappingCachedFileSystem` overrides `newReadFile` to return `new CountingReadFile(super.newReadFile(...))`. `CountingReadFile` lives in `cached-fs-hadoop/src/test/`, implements `ReadFile`, delegates everything, increments an `AtomicLong preadvCalls` on `preadv`.
- **Alternative if removing `final` is unacceptable:** introduce a `ReadFileFactory` in `cached-fs-core` (`@FunctionalInterface` taking `(FileSystem, Path, String, long) -> ReadFile`), stored on `CacheBootstrap`, defaulting to `HadoopReadFile::new`. Test code overrides via a new `CacheBootstrap.setReadFileFactoryForTesting(...)` helper (test-only mutator, package-restricted or annotated). Plan picks this alternative if the `final`-removal raises objections in review. The HandleOpener.wrap default-method from v3 is dropped (had no production call site).

### Acceptance tests

- **One preadv per coalesced group** (non-contended cold read): `loadQuantum=1 MiB`, `max-gap=512 KiB`, cap=16. Read 4 MiB cold. Assert `preadvCalls == 1` AND `restartCount == 0`. (Both halves of the assertion are required — restartCount==0 ensures we exercised the coalesce path, not silent fallback.)
- **Group cap respected**: `max-chunks-per-group=2`. Read 4 MiB cold. Assert `preadvCalls == 2`.
- **Disabled fallback** (`coalesce.enabled=false`): assert `preadvCalls == 4`.
- **Mixed Hit/Miss**: prefill chunks 0 and 2. Cold-read 4 chunks. Assert exactly 2 `preadv` calls (for the miss groups).
- **Contended Waiting restart-and-progress** (2-thread test): T1 holds chunk 1 Exclusive via a latched ReadFile that blocks one preadv. T2 reads chunks 0..2 cold.
  - Assert: T2 completes, `PinLeakAssertions.assertNoLeak()`, AND either (a) T2 restarted ≤ 3 times then succeeded, OR (b) T2 fell back to per-chunk. Both are acceptable per design.
  - **Regression guard against silent-fallback**: a separate `coalesceFallbackRateUnderNoContention` test issues 100 cold non-contended reads back-to-back; asserts `restartCount == 0 && preadvCalls < chunkCount` for ALL reads. This catches a regression where Waiting handling spuriously fires (e.g. order-of-init bug treats every Hit as Waiting).
- **Pin-leak stress** (Failsafe IT, not Surefire unit test — duration ~30s): `PinLeakStressIT` in `cached-fs-hadoop/src/test/java/.../it/`. 100 threads × 1000 random-offset reads × forced-Waiting injection at 5% rate. Asserts final cache snapshot has `numShared==0 && numExclusive==0`.
- **`rawOverreadBytes`**: cold-read two chunks with a 512 KiB gap between their offsets. Assert `ioStats.rawOverreadBytes() == 512 KiB`. Adjacent chunks: 0.

### Risk

Medium. Multi-pin lifecycle + abort-and-restart; the no-fallback regression test plus pin-leak stress are the load-bearing checks.

## Phase 5c.0 — `AsyncDataCache.pendingPrefetchBytes()` (precondition)

Lands first, no callers.

- `private final LongAdder pendingPrefetchBytes = new LongAdder()`.
- `public long pendingPrefetchBytes() { return pendingPrefetchBytes.sum(); }` — public for external observers and tests in different packages.
- Package-private (or protected): `void incrementPendingPrefetch(long bytes)` / `void decrementPendingPrefetch(long bytes)`. **Visibility:** since the only caller will be `cached-fs-hadoop`'s prefetch task and `cached-fs-hadoop` is a different package, we make these methods `public` too but javadoc them as "internal — call only from the cached-fs prefetch task". A future move to a sealed-friend pattern is possible if a `cached-fs-internal` annotation is introduced.

5c.0 does NOT specify increment/decrement call sites; that wiring lives entirely in 5c-proper.

Acceptance for 5c.0:
- Unit test: increment by `4×1024`, sum returns 4096; decrement by 1024, sum returns 3072.
- Public-API reach test in `cached-fs-hadoop`'s test tree: `CacheBootstrap.get().orElseThrow().ramCache().pendingPrefetchBytes()` returns 0 on a fresh bootstrap.
- Existing test suite passes.

## Phase 5c — Async prefetch (large)

### Changes

1. **`CacheBootstrap` prefetch executor.** New field `ExecutorService prefetchExecutor`, constructed as `ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueSize), daemonFactory, new ThreadPoolExecutor.DiscardPolicy())`. Daemon ThreadFactory names threads `"cached-fs-prefetch-N"` with `daemon=true` and an uncaught-exception handler that logs at WARN. **Rejection policy is `DiscardPolicy`** (per Decisions §3) plus a wrapper that increments `ioStats.incPrefetchSkipped("queue_full")` before discarding. The wrapper is implemented as a `RejectedExecutionHandler` named `DiscardAndCountHandler`.
2. **`CacheBootstrap.close()`** (new method introduced by this phase; `uninstallForTesting()` delegates to it):
   1. `closed.set(true)` (new `AtomicBoolean` field). Subsequent `submit` callers consult this and skip.
   2. **Quiesce in-flight readers.** Phase 5c does NOT add reader-side close-coordination (out of scope — phase 4's `FileHandleFactory` drain already covers per-FS close ordering). Instead, **`close()` is documented as a process-level shutdown step** that the operator triggers only after all FileSystems have closed. If the operator calls `close()` while CachingInputStreams are still open, the result is a best-effort drain plus a bounded leak:
   3. `prefetchExecutor.shutdown()` (no new submissions accepted by JDK either). `awaitTermination(timeout)` where `timeout = fs.cached.prefetch.shutdown-timeout-seconds` (default 30).
   4. If timeout, `prefetchExecutor.shutdownNow()` AND a second `awaitTermination(10s)`. If STILL not terminated, log at ERROR and proceed. **Upper bound on leaked pins:** prefetch tasks each hold at most one chunk's exclusive pin at a time AND the executor is bounded to `threads` concurrent tasks AND the queue is capped at `queueSize`. The maximum leaked pin bytes is `(threads + queueSize) × loadQuantumBytes`. Document this bound; with default `threads=availableProcessors` (~16) and `queueSize=1024` and `loadQuantum=8 MiB`, peak leak ≈ `1040 × 8 MiB = 8.3 GiB` — large in absolute terms but bounded. Operators with tight budgets should drop `queueSize` (Phase 5c knob).
   5. After executor terminates: `handleFactory.closeAll()`, `ssdCache.close()`, `ramCache.close()`.
3. **`CachingInputStream` prefetch state.** New `volatile CompletableFuture<RawFileCacheKey> pendingPrefetch`. Documented contract: `read/seek/close` are not thread-safe (matches Hadoop `FSDataInputStream` contract). VarHandle-based `compareAndSet` on `pendingPrefetch` is defense-in-depth:
   ```java
   private static final VarHandle PENDING_VH =
       MethodHandles.lookup().findVarHandle(CachingInputStream.class,
                                            "pendingPrefetch",
                                            CompletableFuture.class);
   ```
   Submission: `if (PENDING_VH.compareAndSet(this, null, future)) submit(...); else future.cancel(false);` — the `else` branch handles the impossible-but-defended case of two concurrent submits.
4. **Consumer thread, post-chunk-N read**: if `pendingPrefetch == null` AND `position >= chunkNEnd - (loadQuantum * triggerTailFraction)` AND `tracker.data(trackingId).readPct() >= prefetchPctThreshold` AND `admissionGate()`: submit the prefetch task. (Per Decisions §5, gate on `readPct` not `adjustedReadPct` so the first stripe is not deadlocked.)

   **Prefetch task body** (executor thread; sole site that increments/decrements `pendingPrefetchBytes`):
   ```text
   cache.incrementPendingPrefetch(chunkSize);     // sole increment
   try {
     FindResult r = cache.findOrCreate(nextKey, chunkSize, false);
     if (r instanceof FindResult.Hit hit) { hit.pin().close(); return nextKey; }
     if (r instanceof FindResult.Waiting w) {
       // peer is filling; do NOT await on an executor thread — return and let
       // the consumer's later findOrCreate see Hit or Waiting itself.
       return nextKey;
     }
     // Exclusive: fill via preadv, then promote to shared and close.
     CachePin excPin = ((FindResult.Exclusive) r).pin();
     try {
       fillExclusive(excPin, nextOffset, chunkSize);   // preadv
       excPin.exclusiveToShared(true).close();
     } catch (Throwable t) {
       releaseFailedExclusive(excPin); throw t;
     }
     ioStats.incPrefetch(chunkSize);
     return nextKey;
   } finally {
     cache.decrementPendingPrefetch(chunkSize);   // sole decrement, in finally
   }
   ```
   This balances increment and decrement on every path including throws and the CallerRunsPolicy-removed branch (we now use DiscardPolicy + counter, so this path doesn't apply).

   **DiscardPolicy + counter on submit-side rejection:** if `submit` is rejected (queue full), the DiscardAndCountHandler increments `incPrefetchSkipped("queue_full")` and returns. The consumer's pre-submit increment of `pendingPrefetchBytes` MUST therefore NOT happen — the wrapper is what runs (it gets the discarded Runnable in `rejectedExecution(...)`). The pre-submit increment moves INSIDE the Runnable, ensuring symmetry with the decrement-in-finally regardless of whether the task runs.

5. **Consumer thread on chunk N+1**: if `pendingPrefetch != null`, `await` the future. Regardless of normal or exceptional completion, fall through to fresh `findOrCreate`. After await, set `pendingPrefetch = null`. `ioStats.incPrefetch(chunkSize)` is set by the TASK only — a discarded or failed task never bumps the counter.

   **Eviction-during-handoff acceptance:** new test injects `TTL.applyTTL(0)` between task completion and consumer await. Consumer's fresh `findOrCreate` is a miss → fills cleanly. `IoStatistics.prefetch_evicted_before_use` counter is incremented (new in 5c — added in 5c.0 actually, or in 5c, see below).

6. **Admission gate.** `cache.pendingPrefetchBytes() + chunkSize <= fs.cached.prefetch.max-pending-bytes`. Default `loadQuantumBytes × threads × 4` (factor of 4 is a starting heuristic — see open follow-up; cited as "rounded from a workload microbenchmark TBD"). Optional secondary signal: heap pressure — but **cached per 100 ms** to avoid the per-prefetch `MemoryMXBean` cost (~µs per call):
   ```java
   private static final long HEAP_PRESSURE_TTL_NS = 100_000_000L;
   private volatile long heapPressureLastCheckedNs;
   private volatile boolean heapPressureActive;
   private boolean heapPressureGate() {
     long now = System.nanoTime();
     if (now - heapPressureLastCheckedNs > HEAP_PRESSURE_TTL_NS) {
       heapPressureLastCheckedNs = now;
       MemoryUsage u = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
       heapPressureActive = u.getUsed() * 10 > u.getMax() * 9;  // >90% used
     }
     return heapPressureActive;
   }
   ```
   Configurable via `fs.cached.prefetch.heap-pressure-check.enabled` (default `true`) and `fs.cached.prefetch.heap-pressure-ttl-ms` (default 100).

7. **`CachingInputStream.close()`**: set `closed = true`, then `pendingPrefetch.cancel(false)` if non-null. `cancel(false)` does not interrupt running tasks; the task's finally block decrements the counter regardless. The consumer never holds the prefetch task's pin.
8. **`seek()` invalidation**: if `position` moves outside `[chunkN+1 start, chunkN+1 end)`, cancel `pendingPrefetch`.

### 5c new IoStatistics counters

Phase 5c adds two counters to `IoStatistics`: `prefetchSkipped(reason)` and `prefetchEvictedBeforeUse()`. Both are byte-only (no event count partner) since the use cases are about volume. Adapter exposes under `cachedfs_stream_prefetch_skipped_bytes` and `cachedfs_stream_prefetch_evicted_bytes`.

### Configuration knobs

- `fs.cached.prefetch.enabled` (default `false` — opt-in).
- `fs.cached.prefetch.threads` (default `Runtime.getRuntime().availableProcessors()`).
- `fs.cached.prefetch.queue` (default 1024).
- `fs.cached.prefetch.max-pending-bytes` (default `loadQuantumBytes * threads * 4`; reasoned as "~4 chunks per thread of headroom"; see Open Follow-ups for a measurement task).
- `fs.cached.prefetch.trigger-tail-fraction` (default `0.25`).
- `fs.cached.prefetch.read-density-threshold` (default `80` — matches velox `FLAGS_cache_prefetch_min_pct` at `velox/flag_definitions/flags.cpp:118-121`).
- `fs.cached.prefetch.heap-pressure-check.enabled` (default `true`).
- `fs.cached.prefetch.heap-pressure-ttl-ms` (default 100).
- `fs.cached.prefetch.shutdown-timeout-seconds` (default 30).

### Acceptance tests

- **Sequential prefetch with many small reads**: 32 MiB consumed via 8000 × 4 KiB `read()` calls. `prefetch.enabled=true`, `loadQuantum=1 MiB`. Assert `prefetchBytes() / readBytes() >= 0.5`.
- **Sequential prefetch with one large readFully**: same 32 MiB consumed via one `readFully(0, 32 MiB)`. Assert `prefetchBytes()` shows positive flow after at least one recordReference batch (i.e. prefetch is not permanently dead-zoned).
- **Random suppression**: 1000 random 4 KiB positional reads, scattered. Assert `prefetchBytes() / readBytes() < 0.05`.
- **Admission backpressure**: configure `max-pending-bytes` to admit only 2 in-flight. Verify subsequent submits go to `DiscardAndCountHandler` and `prefetchSkipped("queue_full")` counter increases; pending counter never exceeds budget.
- **Close cancels prefetch**: open stream, trigger prefetch, close stream. Within 1s: `pendingPrefetchBytes() == 0`, `numExclusive == 0`, `PinLeakAssertions.assertNoLeak()`.
- **Prefetch failure**: inject preadv throw. Consumer read still succeeds via fresh findOrCreate. Counter returns to 0.
- **Seek-away invalidation**: trigger prefetch for N+1, seek to N+10. Counter returns to 0.
- **Eviction-during-handoff**: TTL evicts the prefetched entry between task completion and consumer await. Consumer reads correct bytes; `prefetchEvictedBeforeUse` counter increments.
- **CallerRunsPolicy not used** (regression test): the `prefetchExecutor`'s `getRejectedExecutionHandler()` must be `DiscardAndCountHandler`, not `CallerRunsPolicy`.
- **Heap-pressure caching**: under-90%-used heap → ~10K admission checks/sec produce ≤ 1 `MemoryMXBean.getHeapMemoryUsage()` call per 100 ms.

### Risk

High. Async pins are avoided via key-not-pin design; executor lifecycle has a documented (bounded) leak path; admission gate uses a dedicated budget. Worth its own santa-method convergence pass.

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
- Cap `bootstrap.scanTrackers` when metrics show `size() > 10_000` in any 24h window (Caffeine-bounded cache).
- `IoStatistics` ring-buffer of recent N streams for debugging.
- Per-`(scanId, fileNum)` tracker keying once Spark integration produces real workloads.

## README integration

- Phase 5a: `fs.cached.scan-id`, `fs.cached.scan-tracker.enabled`, `fs.cached.metrics.enabled` to config-reference table.
- Phase 5b: `fs.cached.coalesce.enabled`, `fs.cached.coalesce.max-gap-bytes`, `fs.cached.coalesce.max-chunks-per-group`, `fs.cached.coalesce.max-restarts`, `fs.cached.coalesce.always-on`.
- Phase 5c: 8 prefetch knobs (listed in §5c Configuration).
- Divergence-list bullets appended to `CacheBootstrap` (executor + close()), `AsyncDataCache` (pendingPrefetchBytes counter), `CachingInputStream` (5a/5b/5c reader behavior).

## Recommendation

Start with **Phase 5a-prework**. Then **Phase 5a wiring** as its own santa-method convergence. Phase 5b follows. Phase 5c.0 lands as a one-bullet commit before opening Phase 5c.
