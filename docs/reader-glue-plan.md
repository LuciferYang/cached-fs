# Reader Glue Port Plan — velox §5

> **Status:** draft v7.2, 2026-05-25 (HEAD = 52d7715). Plan-only; no code changes yet. Round-7 review fixes: (a) corrected the TrackingId collision math (previous claim "1% threshold at ~75k files" was wrong by ~23×; the actual 1% threshold at 2^29 buckets is ~3.3k files, so collisions are the norm not the exception at production scan sizes) and re-argued tolerability on different grounds (per-pair density mixing, not per-scan); (b) dropped the unimplementable "exposed via bootstrap.aggregateIoStats" gauge wording — `AggregatedIoStatistics` is a typed-counter class with no key/value bag, and `ScanTracker.size()` is a gauge that decreases on eviction; gauge now wired through a new `CacheBootstrap.scanTrackerEntries()` accessor and routed to IoStatisticsAdapter as a Hadoop dynamic gauge; (c) called out the TrackingId class-javadoc update as part of the prework commit; (d) clarified that `setReadFileFactoryForTesting` is not thread-safe and tests using it must run under `@Execution(SAME_THREAD)` (Surefire default is one fork sequential, NOT fork-per-class — wording fixed); (e) submit-vs-execute terminology drift in §4 corrected; (f) heap-pressure refresh now guards on a single AtomicLong CAS so the "≤ 10/sec total MBean calls" bound is hard, not amortized; (g) decrement-counter integrity-check added.

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
5. **First-stripe prefetch gate.** Phase 5c admission gates on **`readPct() >= prefetchPctThreshold`**, not `adjustedReadPct()`. Rationale: `adjustedReadPct` returns 0 on the first reference batch (denominator collapses), which would dead-zone `readFully(0, fullFile)` consumers. `readPct` is cumulative-since-scan-start, so it never returns 0 once any byte is read; the trade-off is that a stream which transitions from sequential to random after warm-up keeps showing high `readPct`. Mitigations: (a) the per-(scanId, fileNumHash) tracker key (see §Decisions §6) limits this contamination to one file, (b) per-chunk consumer cost is cheap (one cache miss) when the speculation turns out wrong, (c) the new acceptance test `staleDensityAfterSeekAwayDoesNotExplodePrefetch` (in §5c) caps the wasted prefetch ratio at `<= 1.5×` the sequential baseline. Long-term, an EWMA-windowed `readPct` over the last K reference batches is on the open-follow-ups list.
6. **TrackingId scoping.** Phase 5a-prework shrinks `TrackingId`'s streamKind from 5 bits to 2 bits (only one streamKind value is used at the Hadoop layer), gaining 3 bits → **29-bit node space** (~536M buckets). This is the v7 change relative to v6's documented 26-bit node. Plan wires `TrackingId.of(fileNumNode, 0)` where:
   ```java
   long h = fileNum ^ (fileNum >>> 32);
   int fileNumNode = (int)(h & ((1L << 29) - 1));   // mask to 29 bits to satisfy TrackingId.of's range check
   ```
   `TrackingId.of(int node, int streamKind)` after the bit-width change validates `0 <= node < (1 << 29)` — the mask is mandatory.

   With 2^29 (~5.37×10^8) buckets, the birthday-paradox 1% collision threshold is **~3.3k files** (`k = sqrt(0.02·N) = sqrt(0.02·5.37e8) ≈ 3,277`). At the median Spark scan scale of 10k–50k files, P(at least one pair collides) ≈ 11%–89%; at 100k files it is effectively 1.0. **Collisions are the norm, not the exception** at production sizes — the previous "rare collision" framing was wrong.

   **Why this is still acceptable.** Tolerability does NOT rest on collisions being rare; it rests on the per-pair locality of the impact. A collision causes two unrelated files to share one `TrackingData` entry, mixing the density signal **only for those two files**. With ~10 expected collisions at 100k files (the expected count is `~k(k-1)/(2N) ≈ k²/(2N)`, e.g., 100k² / 2·5.37e8 ≈ 9.3 collisions), ~20 files (~0.02%) see mixed-density prefetch signals. This is bounded — adding more files at the same N does increase the absolute collision count linearly in `k²/N`, but the impacted fraction stays at most `k/N` (~0.02% at 100k, ~0.1% at 500k).

   **Aggregate prefetch-precision impact.** The `staleDensityAfterSeekAwayDoesNotExplodePrefetch` acceptance test caps wasted prefetch at 1.5× the random baseline for a stale-signal scenario. Collision-induced mixing is structurally similar (a colliding-pair tracker has weighted-averaged density rather than per-file density) and is bounded by the same test for the colliding pair. Files not involved in a collision (the overwhelming majority) see exactly the density they should.

   **Backward compatibility note.** Bit-width change to `TrackingId` is binary-incompatible if any future code persists `TrackingId.id` (e.g., to an SSD scoring snapshot). Today no such persistence exists; document the requirement: any future serialized state including `TrackingId` must carry a version byte to allow re-interpretation.

## Phase 5a — ScanTracker + IoStatistics wiring (small, foundation)

### Phase 5a-prework (mandatory; ships in its own commit before 5a wiring)

**Goal:** make `ScanTracker` safe to share under concurrent readers and provide a coherent snapshot.

Refactor `ScanTracker` storage:

- Replace `ReentrantLock`+`HashMap` with `private final ConcurrentMap<TrackingId, AtomicReference<TrackingData>> data = new ConcurrentHashMap<>();`.
- `private AtomicReference<TrackingData> refFor(TrackingId id) { return data.computeIfAbsent(id, k -> new AtomicReference<>(TrackingData.EMPTY)); }` — the `TrackingData.EMPTY` initial value avoids the `updateAndGet(prev -> prev.referencedBytes + bytes)` NPE.
- `recordReference(id, bytes)`: if `id.isEmpty()` return; else `refFor(id).updateAndGet(prev -> new TrackingData(prev.referencedBytes() + bytes, bytes, prev.readBytes()))`.
- `recordRead(id, bytes)`: analogous, only `readBytes` changes.
- `data(id)`: `var ref = data.get(id); return ref == null ? TrackingData.EMPTY : ref.get();` — single volatile read, coherent across all three fields.
- **Off-switch primitive.** Add a `static final ScanTracker DISABLED = new ScanTracker("__disabled__", 0, true)` where the constructor's `boolean disabled` flag short-circuits `recordReference`/`recordRead` to no-ops. ScanTracker stays `final`. New constructors:
  ```java
  public ScanTracker(String scanId, int loadQuantum) { this(scanId, loadQuantum, /*disabled=*/false); }
  private ScanTracker(String scanId, int loadQuantum, boolean disabled) { ... }
  ```
  Existing call sites at `ScanTrackerTest:29/39/49/63/88` keep working unchanged.
- **TrackingId bit-width change** (v7). Shrink `STREAM_KIND_BITS` from 5 to 2 in the packed representation; node range widens from 26-bit to 29-bit automatically since `NODE_MAX = 1 << (Integer.SIZE - STREAM_KIND_BITS - 1)` is derived. `TrackingId.of(int node, int streamKind)` validation tightens to `streamKind < 4` and `node < (1<<29)`. Existing call sites all use `streamKind == 0`, so the change is non-breaking for the cached-fs tree. **Also updates** the TrackingId class-level javadoc ("node in the high 27 bits and stream kind in the low 5" → "node in the high 29 bits and stream kind in the low 2") and the `NODE_MAX` derivation comment (currently `2^(31 - 5) = 2^26` → `2^(31 - 2) = 2^29`). Adds a 1-line ScanTracker pre-work acceptance test asserting `TrackingId.of((1<<29)-1, 0).id() == ((1<<29)-1) << 2`.
- **`ScanTracker.size()` gauge.** Add `public int size() { return data.size(); }` (O(1) on `ConcurrentHashMap`). Wired to `CacheBootstrap.scanTrackerEntries()` (sum of `size()` across all active trackers) which Phase 5a then routes to `IoStatisticsAdapter` as a Hadoop dynamic gauge. NOT placed on `IoStatistics`/`AggregatedIoStatistics`: those are typed-counter classes; `ScanTracker.size()` is a gauge that can decrease (on tracker eviction by the open-follow-up LRU) and so mismatches the counter data model. Acceptance test: `bootstrap.scanTrackerEntries() == 0` on fresh bootstrap; increments by 1 after first `recordReference` on a new (scanId, file) pair.

Allocation overhead: at ~100k reads/sec → 200k 40-byte `TrackingData` allocations/sec → ~8 MB/s, comfortably within young-gen tolerance. Under CAS-retry contention this can double; document in the prework's microbenchmark.

Acceptance for prework:
- Existing `ScanTrackerTest` passes unchanged after the refactor.
- New unit test: 8 threads each call `recordReference(id, 1)` 1M times on a shared tracker; final `data(id).referencedBytes()` must equal exactly `8 × 1M` (no lost updates).
- Snapshot-monotonicity test: 8 writer threads + 1 reader thread polling `data(id).adjustedReadPct()` every 1ms for 5s — assert no negative value observed.
- Microbenchmark (JUnit, not CI-gated): record throughput vs the locked baseline as a regression-watch artifact.

### Phase 5a wiring

Commits in landing order:

1. **`CacheBootstrap.trackerFor(String scanId)`** — adds `private final ConcurrentMap<String, ScanTracker> scanTrackers`. Normalizes null/empty/whitespace to `"default"`. `computeIfAbsent(normalized, k -> new ScanTracker(k, this.loadQuantumBytes))` — uses the bootstrap's `loadQuantumBytes` field (already exposed via `loadQuantumBytes()`; the value is dropped by `ScanTracker` per velox parity but the constructor still requires it). No callers in this commit.
2. **`CacheBootstrap.currentScanId()` + `withScanId(String)`** — `ThreadLocal<String>` accessor plus `AutoCloseable withScanId(scanId)` that on `close()` calls `threadLocal.remove()`. Javadoc warns: "Single-threaded use only. Do NOT pair with Spark `TaskContext.addTaskCompletionListener` — close may run on a different thread and leak the slot. Use try-with-resources inside the task body."
3. **`CachedFsConfig.SCAN_ID = "fs.cached.scan-id"`** — README config-reference picks up the key. No behavior change.
4. **`CachingInputStream` constructor signature change** — gains `ScanTracker tracker`, `TrackingId trackingId`, `IoStatistics ioStats`. Constructor is package-private with a single grep-verified call site (`CachedFileSystem.open()`). The same commit updates the call site so the build stays green.
5. **`CachedFileSystem.open()` plumbing** — resolves scanId via the Decisions §1 precedence, calls `b.trackerFor(scanId)`, instantiates `IoStatistics ioStats = metricsEnabled ? new IoStatistics() : IoStatistics.NO_OP`, derives `TrackingId trackingId = TrackingId.of(fileNumHash(handle.fileNum()), 0)` per Decisions §6, passes `(tracker, trackingId, ioStats)` to `CachingInputStream`.
6. **`CachingInputStream.readFullyFromCache` recording**:
   - Top: `tracker.recordReference(trackingId, length); ioStats.incRead(length);`.
   - Bottom: `tracker.recordRead(trackingId, length);`.
   - Per-chunk inside the existing switch, on `case FindResult.Hit`: `ioStats.incRamHit(copyLen)`. (No separate `cache.exists` probe — sealed-result branch is the hit signal.)

   **Velox-faithful caveat:** `adjustedReadPct()` returns 0 on the very first reference batch because the denominator collapses. This suppresses prefetch on unproven streams. Decisions §5 picks `readPct()` over `adjustedReadPct()` for the prefetch gate to avoid the dead-zone on `readFully(0, fullFile)` consumers.
7. **`AggregatedIoStatistics`** new class in cached-fs-core (`stats/AggregatedIoStatistics.java`) — `public final class` with the same counter surface as `IoStatistics` but backed by `LongAdder` per counter. New `add(IoStatistics source)` method snapshots the source via its public byte/count getters (one volatile read per counter) and `LongAdder.add`s into self. Commutative and thread-safe under concurrent callers.

   **Snapshot invariant** (added in v5): `add(source)` does NOT snapshot the source atomically across counters — each public getter is a separate `AtomicLong.get()` volatile read. Caller MUST ensure the source is quiescent during `add`. The Phase 5a use case satisfies this: `CachingInputStream.close()` is the only `add` call site, and Hadoop's `FSDataInputStream` contract makes `close` non-thread-safe vs. reads. javadoc on `add` states the invariant.

   Spec: `IoStatistics` stays `final` with `AtomicLong` fields plus a `private final boolean disabled` flag (see Off-switch). `CacheBootstrap.aggregateIoStats` is of type `AggregatedIoStatistics`. `CachingInputStream.close()` uses an `AtomicBoolean aggregated` guard (set-once, never reset) so double-close is idempotent: `if (aggregated.compareAndSet(false, true)) bootstrap.aggregateIoStats.add(this.ioStats);`.
8. **`IoStatisticsAdapter` + `CachingInputStream implements IOStatisticsSource`** — adapter maps each cached-fs counter to either a real `StreamStatisticNames` constant or a `cachedfs_`-prefixed name per the table above. Adapter is `final class` in cached-fs-hadoop.

### Off-switch

`fs.cached.scan-tracker.enabled` (default `true`). When false: `trackerFor(...)` returns `ScanTracker.DISABLED`. `IoStatistics` continues to record independently.

**Master metrics off-switch:** `fs.cached.metrics.enabled` (default `true`). When false: `CachingInputStream` is constructed with `IoStatistics.NO_OP`.

`IoStatistics` stays `final`. The off-switch is implemented via a `private final boolean disabled` field on `IoStatistics`. All `inc*` methods short-circuit at the top when `disabled == true`, mirroring the `ScanTracker.DISABLED` pattern from 5a-prework. Getters still return the underlying `AtomicLong.get()` (always 0 when disabled). Constructors:
```java
public IoStatistics() { this(false); }                 // preserves existing call sites
private IoStatistics(boolean disabled) { this.disabled = disabled; }
public static final IoStatistics NO_OP = new IoStatistics(true);
```
The public no-arg constructor delegating to the new private boolean overload preserves the existing `new IoStatistics()` call sites (`IoStatisticsTest`). Same pattern applied to `ScanTracker`: `public ScanTracker(String scanId, int loadQuantum) { this(scanId, loadQuantum, false); }` plus the new private 3-arg constructor for `DISABLED`.

With both flags off, all `inc*` calls are one volatile load + early return; `AggregatedIoStatistics.add(NO_OP)` then adds zeros (NO_OP's getters return 0). The codepath is observably equivalent to today's per-chunk fetch (verified by the off-switch acceptance test).

**NO_OP shared-instance test isolation.** Because `NO_OP` is JVM-wide, if a regression removes the short-circuit, parallel tests could each touch NO_OP and the "expect 0" assertion gets cross-test pollution. Acceptance test asserts via per-test snapshot delta: `long before = NO_OP.readBytes(); doRead(); assertEquals(before, NO_OP.readBytes());` — robust against pollution and still catches the regression.

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

Today `CachedFileSystem.openHandleForKey` creates `new HadoopReadFile(fs, p, key, size)` directly at line 346 of `CachedFileSystem.java`. `CachedFileSystem` is `public final` and the codebase's established style preserves that (see multi-scheme opener registry: extensibility via factories on `CacheBootstrap`, not via subclassing).

**Primary seam: `ReadFileFactory` on `CacheBootstrap`.**

- New `@FunctionalInterface ReadFileFactory` in `cached-fs-core`: `ReadFile create(FileSystem fs, Path p, String key, long size)`.
- `CacheBootstrap` adds `private volatile ReadFileFactory readFileFactory = HadoopReadFile::new` and `ReadFileFactory readFileFactory()` accessor.
- `CachedFileSystem.openHandleForKey` (line 346) changes from `new HadoopReadFile(fs, p, key, size)` to `b.readFileFactory().create(fs, p, key, size)`. The `b` reference is already in scope at line 348-351 where `b.stringIds()` is called for `StringIdMap`; no extra lookup needed.
- Test-only mutator: `CacheBootstrap.setReadFileFactoryForTesting(ReadFileFactory factory)` — **package-private** (the codebase has no `@VisibleForTesting` annotation; tests in `cached-fs-hadoop/src/test/java/io/github/luciferyang/cachedfs/hadoop` are in the same package as `CacheBootstrap`, so package-private access works). Canonical implementation:
  ```java
  AutoCloseable setReadFileFactoryForTesting(ReadFileFactory factory) {
    ReadFileFactory prior = this.readFileFactory;
    this.readFileFactory = factory;
    return () -> this.readFileFactory = prior;
  }
  ```
  Used as `try (var ignored = bootstrap.setReadFileFactoryForTesting(testFactory)) { ... }` so a test crash inside the try-block restores the prior factory via try-with-resources. The swap is **not thread-safe**: concurrent callers can race on `prior` and lose the original factory. Repository's Surefire default is `forkCount=1, reuseForks=true` with one fork running classes sequentially (NOT fork-per-class), so the no-parallel default already enforces single-threaded execution. Tests using this seam must additionally carry `@Execution(SAME_THREAD)` so they remain safe if a future CI run enables `-Dparallel=classes` or `-DthreadCount=N`.
- `CountingReadFile` lives in `cached-fs-hadoop/src/test/`, implements `ReadFile`, delegates everything, increments an `AtomicLong preadvCalls` on `preadv`.

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
- **Integrity guard in decrement.** `decrementPendingPrefetch(bytes)` asserts `pendingPrefetchBytes.sum() - bytes >= 0` before applying (`if (pendingPrefetchBytes.sum() < bytes) throw new IllegalStateException(...)`). LongAdder's `sum()` is non-atomic vs concurrent adds, so the guard is best-effort under contention — it catches gross misuse (negative-going counter from external callers desyncing) without claiming atomic consistency. The PrefetchTask runtime never triggers this under correct flow because each increment is paired with exactly one decrement in `finally`.

5c.0 does NOT specify increment/decrement call sites; that wiring lives entirely in 5c-proper.

Acceptance for 5c.0:
- Unit test: increment by `4×1024`, sum returns 4096; decrement by 1024, sum returns 3072.
- Public-API reach test in `cached-fs-hadoop`'s test tree: `CacheBootstrap.get().orElseThrow().ramCache().pendingPrefetchBytes()` returns 0 on a fresh bootstrap.
- Existing test suite passes.

## Phase 5c — Async prefetch (large)

### Changes

1. **`CacheBootstrap` prefetch executor.** New field `ExecutorService prefetchExecutor`, constructed as `ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueSize), daemonFactory, new DiscardAndCountHandler())`. Daemon ThreadFactory names threads `"cached-fs-prefetch-N"` with `daemon=true` and an uncaught-exception handler that logs at WARN.

   **`PrefetchTask` is a named class** (NOT a lambda) so the rejection handler can downcast and recover state. Fields: `(CachingInputStream owner, IoStatistics ioStats, AsyncDataCache cache, long chunkSize, RawFileCacheKey nextKey, long nextOffset, CompletableFuture<RawFileCacheKey> future)`. `PrefetchTask.run()` is the body shown in step 4 below. PrefetchTask owns the future; on every exit path (success, exception, discard) the future is completed exactly once.

   **Submission via `execute()`, not `submit()`.** `ExecutorService.submit(Runnable)` wraps the runnable in a `java.util.concurrent.FutureTask` before enqueueing, so the rejection handler sees a `FutureTask` (not our `PrefetchTask`) and the `r instanceof PrefetchTask` downcast fails silently. Phase 5c uses `prefetchExecutor.execute(prefetchTask)` so the raw `PrefetchTask` reaches the handler.

   **Synchronous-on-submit-thread invariant (load-bearing).** Per `ThreadPoolExecutor` JDK contract, `RejectedExecutionHandler.rejectedExecution(...)` is invoked synchronously on the calling thread of `execute()`. Phase 5c relies on this: the handler writes `task.owner.lastRejectionNanos` and calls `task.owner.clearPendingPrefetchIf(...)` on what is also the consumer thread (since submission only happens on the consumer thread per §step 4). Same-thread writer + reader means no cross-thread visibility concerns. A future change that wraps `execute()` in another executor (e.g., a queue aggregator) would invalidate this invariant; document near the field declaration.

   **`DiscardAndCountHandler implements RejectedExecutionHandler`**: on `rejectedExecution(Runnable r, ThreadPoolExecutor executor)`:
   - If `r instanceof PrefetchTask task`: increment `task.ioStats.incPrefetchSkipped("queue_full")`; complete the future exceptionally so the consumer doesn't deadlock: `task.future.completeExceptionally(new RejectedExecutionException("prefetch queue full"))`; reset the owner's `pendingPrefetch` slot via the co-located helper `task.owner.clearPendingPrefetchIf(task.future)` — a new package-private method on `CachingInputStream` that does `PENDING_VH.compareAndSet(this, expected, null)` internally. This keeps `PENDING_VH` package-private to `CachingInputStream` (avoids cross-class private access).
   - Set the owner's `lastRejectionNanos = System.nanoTime()` for the per-stream backoff in §step 4.
   - The byte budget counter `pendingPrefetchBytes` is NOT touched by the handler — the increment only runs inside `PrefetchTask.run()` (after the rejection branch this code never executes), so no decrement is needed and no leak occurs.
2. **`CacheBootstrap.close()`** (new method introduced by this phase; `uninstallForTesting()` delegates to it):
   1. `closed.set(true)` (new `AtomicBoolean` field). Subsequent `submit` callers consult this and skip.
   2. **Quiesce in-flight readers.** Phase 5c does NOT add reader-side close-coordination (out of scope — phase 4's `FileHandleFactory` drain already covers per-FS close ordering). Instead, **`close()` is documented as a process-level shutdown step** that the operator triggers only after all FileSystems have closed. If the operator calls `close()` while CachingInputStreams are still open, the result is a best-effort drain plus a bounded leak:
   3. `prefetchExecutor.shutdown()` (no new submissions accepted by JDK either). `awaitTermination(timeout)` where `timeout = fs.cached.prefetch.shutdown-timeout-seconds` (default 30).
   4. If timeout, `prefetchExecutor.shutdownNow()` AND a second `awaitTermination(10s)`. If STILL not terminated, log at ERROR and proceed. **Upper bound on leaked pins:** prefetch tasks each hold at most one chunk's exclusive pin at a time AND the executor is bounded to `threads` concurrent tasks AND the queue is capped at `queueSize`. The maximum leaked pin bytes is `(threads + queueSize) × loadQuantumBytes`. Document this bound; with default `threads=availableProcessors` (~16) and `queueSize=1024` and `loadQuantum=8 MiB`, peak leak ≈ `1040 × 8 MiB = 8.3 GiB` — large in absolute terms but bounded. Operators with tight budgets should drop `queueSize` (Phase 5c knob).
   5. After executor terminates: `handleFactory.closeAll()`, `ssdCache.close()`, `ramCache.close()`.
3. **`CachingInputStream` prefetch state.** New fields:
   - `private volatile CompletableFuture<RawFileCacheKey> pendingPrefetch` — the in-flight prefetch handle (key, not pin). VarHandle CAS is used to coordinate submission vs. rejection-handler reset.
   - `volatile long lastRejectionNanos = Long.MIN_VALUE / 2` — **package-private** so sibling `DiscardAndCountHandler` in the same package can write it. Initialized far in the past so the first prefetch attempt is never back-pressured. Volatile is defense-in-depth: per the JDK invariant in §step 1, both the write (in the handler) and the read (in the admission gate, §step 4) execute on the same consumer thread, but `volatile` removes the assumption that no future refactor will move submission off-thread.
   - `private volatile boolean closed` — see §step 7 for the close-race interaction.
   ```java
   static final VarHandle PENDING_VH =
       MethodHandles.lookup().findVarHandle(CachingInputStream.class,
                                            "pendingPrefetch",
                                            CompletableFuture.class);
   ```
   `PENDING_VH` is **package-private** (no modifier) so sibling classes `DiscardAndCountHandler` and `PrefetchTask` reach the slot through the co-located helper `void clearPendingPrefetchIf(CompletableFuture<?> expected)` rather than calling `PENDING_VH.compareAndSet(...)` directly across class boundaries. The helper body is `PENDING_VH.compareAndSet(this, expected, null)` — if a peer already replaced the slot (e.g., another `seek()` cycle), the CAS is a no-op and the helper returns silently.

   Submission: `if (PENDING_VH.compareAndSet(this, null, future)) prefetchExecutor.execute(prefetchTask); else future.cancel(false);` — the `else` branch handles the impossible-but-defended case of two concurrent submits. Always `execute(...)` (never `submit(...)`); the rejection-handler downcast depends on the raw Runnable reaching the handler unwrapped by `FutureTask`.

   Documented contract: `read/seek/close` are not thread-safe (matches Hadoop `FSDataInputStream` contract). VarHandle-based `compareAndSet` on `pendingPrefetch` is defense-in-depth.
4. **Consumer thread, post-chunk-N read**: if `pendingPrefetch == null` AND `position >= chunkNEnd - (loadQuantum * triggerTailFraction)` AND `tracker.data(trackingId).readPct() >= prefetchPctThreshold` AND `admissionGate()` AND `System.nanoTime() - lastRejectionNanos >= REJECTION_BACKOFF_NS` (per-stream backoff, default 100 ms via `fs.cached.prefetch.rejection-backoff-ms`): submit the prefetch task via `prefetchExecutor.execute(task)`. The backoff prevents a hot resubmit loop under sustained queue saturation: after a rejection the stream waits 100 ms before re-attempting prefetch submission, while the consumer's normal per-chunk path proceeds at its native rate.

   **`PrefetchTask.run()` body** (executor thread; sole site that increments/decrements `pendingPrefetchBytes`; sole site that completes the future from the run path):
   ```text
   cache.incrementPendingPrefetch(chunkSize);     // sole increment in run-path
   Throwable failure = null;
   try {
     FindResult r = cache.findOrCreate(nextKey, chunkSize, false);
     if (r instanceof FindResult.Hit hit) {
       hit.pin().close();
       return;  // future.complete in finally
     }
     if (r instanceof FindResult.Waiting w) {
       // peer is filling; do NOT await on an executor thread.
       return;  // future.complete in finally
     }
     CachePin excPin = ((FindResult.Exclusive) r).pin();
     try {
       fillExclusive(excPin, nextOffset, chunkSize);     // preadv
       excPin.exclusiveToShared(true).close();
     } catch (Throwable t) {
       releaseFailedExclusive(excPin);
       throw t;
     }
     ioStats.incPrefetch(chunkSize);
   } catch (Throwable t) {
     failure = t;
   } finally {
     // Complete the future FIRST so a throw in either subsequent step cannot
     // strand the consumer's await. CompletableFuture.complete is documented
     // no-throw. Then clear the CAS slot; finally decrement the counter.
     // The two later steps wrap each other in try/finally so the decrement
     // runs even on the (theoretical) clearPendingPrefetchIf throw.
     if (failure != null) future.completeExceptionally(failure);
     else future.complete(nextKey);
     try {
       owner.clearPendingPrefetchIf(future);
     } finally {
       cache.decrementPendingPrefetch(chunkSize);
     }
   }
   ```
   Properties:
   - Counter symmetric across all paths (Hit/Waiting/Exclusive-success/Exclusive-throw): one increment in run-path, one decrement in finally.
   - Future completed exactly once via finally, regardless of path.
   - On discard the task never runs → counter never touched; the rejection handler completes the future + resets the CAS slot.
   - Admission gate's pre-submit check `pendingPrefetchBytes() + chunkSize <= budget` is non-atomic vs. the in-task increment. **Overshoot bound:** up to `(active consumer threads) × chunkSize` over budget under concurrent admission. Acceptable; doesn't leak.

5. **Consumer thread on chunk N+1**: if `pendingPrefetch != null`, `await` the future. Regardless of normal or exceptional completion, fall through to fresh `findOrCreate`. After await, set `pendingPrefetch = null`. `ioStats.incPrefetch(chunkSize)` is set by the TASK only — a discarded or failed task never bumps the counter.

   **Eviction-during-handoff acceptance:** new test injects `TTL.applyTTL(0)` between task completion and consumer await. Consumer's fresh `findOrCreate` is a miss → fills cleanly. `IoStatistics.prefetch_evicted_before_use` counter is incremented (new in 5c — added in 5c.0 actually, or in 5c, see below).

6. **Admission gate.** `cache.pendingPrefetchBytes() + chunkSize <= fs.cached.prefetch.max-pending-bytes`. Default `loadQuantumBytes × threads × 4` (factor of 4 is a starting heuristic; see open follow-up for a measurement task). Optional secondary signal: heap pressure — but **cached per 100 ms** on `CacheBootstrap` (single instance per JVM, so one MemoryMXBean call per 100 ms across all streams):
   ```java
   // Fields on CacheBootstrap (shared by all streams):
   private static final long HEAP_PRESSURE_TTL_NS = 100_000_000L;
   private final AtomicLong heapPressureLastCheckedNs = new AtomicLong(Long.MIN_VALUE / 2);
   private volatile boolean heapPressureActive;
   public boolean isHeapPressureHigh() {
     long now = System.nanoTime();
     long prev = heapPressureLastCheckedNs.get();
     // Single-CAS-winner pattern: only one thread per TTL window refreshes,
     // even under N concurrent admission-gate callers. Hard bound on MBean calls.
     if (now - prev > HEAP_PRESSURE_TTL_NS && heapPressureLastCheckedNs.compareAndSet(prev, now)) {
       MemoryUsage u = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
       heapPressureActive = u.getUsed() * 10 > u.getMax() * 9;  // >90% used
     }
     return heapPressureActive;
   }
   ```
   `CachingInputStream` admission gate calls `bootstrap.isHeapPressureHigh()`; the single-CAS-winner pattern gives a **hard** bound — at most one MemoryMXBean call per TTL window across the entire JVM, so ≤ 10 calls/sec at the default 100 ms TTL regardless of concurrent stream count. (The previous double-volatile pattern was an amortized bound that could leak up to N concurrent MBean calls on a saturating burst.)
   Configurable via `fs.cached.prefetch.heap-pressure-check.enabled` (default `true`) and `fs.cached.prefetch.heap-pressure-ttl-ms` (default 100).

7. **`CachingInputStream.close()`**: set `closed = true`, then `pendingPrefetch.cancel(false)` if non-null. `cancel(false)` does not interrupt running tasks; the task's finally block decrements the counter regardless. The consumer never holds the prefetch task's pin.
8. **`seek()` invalidation**: if `position` moves outside `[chunkN+1 start, chunkN+1 end)`, cancel `pendingPrefetch`.

### 5c new IoStatistics counters

Phase 5c adds two counters to `IoStatistics`: `prefetchSkipped(reason)` and `prefetchEvictedBeforeUse()`. Both are byte-only (no event count partner) since the use cases are about volume. Adapter exposes under `cachedfs_stream_prefetch_skipped_bytes` and `cachedfs_stream_prefetch_evicted_bytes`.

### Configuration knobs

- `fs.cached.prefetch.enabled` (default `false` — opt-in).
- `fs.cached.prefetch.threads` (default `Runtime.getRuntime().availableProcessors()`).
- `fs.cached.prefetch.queue` (default 64; backpressure-sized, not throughput-sized — `DiscardAndCountHandler` is the steady-state safety valve. Reduces worst-case pin-leak window at close from 8.3 GiB to ~640 MiB with default threads/loadQuantum).
- `fs.cached.prefetch.max-pending-bytes` (default `loadQuantumBytes * threads * 4`; reasoned as "~4 chunks per thread of headroom"; see Open Follow-ups for a measurement task).
- `fs.cached.prefetch.trigger-tail-fraction` (default `0.25`).
- `fs.cached.prefetch.read-density-threshold` (default `80` — matches velox `FLAGS_cache_prefetch_min_pct` at `velox/flag_definitions/flags.cpp:118-121`).
- `fs.cached.prefetch.heap-pressure-check.enabled` (default `true`).
- `fs.cached.prefetch.heap-pressure-ttl-ms` (default 100).
- `fs.cached.prefetch.shutdown-timeout-seconds` (default 30).
- `fs.cached.prefetch.rejection-backoff-ms` (default 100 — per-stream wait between a `DiscardAndCountHandler` rejection and the next prefetch submission attempt; prevents a hot resubmit loop under sustained queue saturation).

### Acceptance tests

- **Sequential prefetch with many small reads**: 32 MiB consumed via 8000 × 4 KiB `read()` calls. `prefetch.enabled=true`, `loadQuantum=1 MiB`. Assert `prefetchBytes() / readBytes() >= 0.5`.
- **Sequential prefetch with one large readFully**: same 32 MiB consumed via one `readFully(0, 32 MiB)`. Assert `prefetchBytes()` shows positive flow after at least one recordReference batch (i.e. prefetch is not permanently dead-zoned).
- **Stale density after seek-away does not explode prefetch**: read 16 MiB sequentially (warms readPct to ~100), then issue 1000 random 4 KiB reads scattered across a 256 MiB file. Assert `prefetchBytes() / readBytes()` over the random phase `<= 1.5×` the same metric measured on a fresh stream that does only the random phase (i.e., stale density doesn't double the speculation rate). This is the v5 regression guard for the `readPct`-stays-cumulative trade-off.
- **Discarded submit does not deadlock consumer**: configure `prefetch.queue=1` and saturate it; the next consumer read after a rejected submit must complete within 100 ms (not block on `pendingPrefetch.await()`). Asserts `DiscardAndCountHandler` correctly completes the future + resets the CAS slot.
- **Rejection backoff prevents resubmit hot-loop**: under sustained saturation (queue full for 1s), assert the number of submitted prefetch tasks per consumer chunk advance is ≤ 1 + (elapsed_ms / rejection-backoff-ms). With `rejection-backoff-ms=100`, that's ≤ 11 submits in 1s rather than thousands.
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
