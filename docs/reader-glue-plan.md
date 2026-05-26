# Reader Glue Port Plan — velox §5

> **Status:** draft v7.13, 2026-05-26 (HEAD = d52ecb3). Plan-only; no code changes yet. v7.13 closes Round-19 docs drift: field javadoc on the HWM AtomicLong now matches v7.12's all-CAS unification (previously said "sequential uses plain set"; v7.12 unified all read-path writers under updateAndGet). Status banner now tracks the current v-prefix. Earlier round-15-onward fixes preserved: (A) **admission-gate invariant equation corrected**: the `prefetchSkipped("queue_full")` term was incorrectly included — `queue_full` fires in `DiscardAndCountHandler` AFTER the admission-gate has already PASSED (the submit branch), so it never increments `admissionGateFalseCount`. Equation now reads `admissionGateFalseCount == prefetchSkipped("budget") + prefetchSkipped("heap_pressure") + prefetchEligibleSuppressedBytes/chunkSize`. (B) **`cancel(false)` + slot clear**: seek-invalidation and close now BOTH (i) call `pendingPrefetch.cancel(false)` AND (ii) call `clearPendingPrefetchIf(future)` to release the CAS slot synchronously — without (ii) the cancelled future stays pinned in the slot until the executor's run-finally clears it, silently suppressing all subsequent prefetch submissions in that window. (C) **`fileNumHash` proper hash**: today `FileHandle.fileNum()` is a sequential int from `StringIdMap`; the XOR-fold to 29 bits is a no-op (high 32 bits always 0). v7.9 changes `fileNumHash(long fileNum)` to apply `Murmur3.fmix32((int) fileNum)` THEN mask to 29 bits — uniformly distributes sequential IDs across the bucket space and restores the collision math from §Decisions §6 to its intended meaning. (D) **`fillExclusive` and consumer state machine** properly defined: `CachingInputStream` adds `private long lastReadEndOffset` and `private long currentChunkEnd` updated by `Seekable.seek()` and the per-chunk fill path (NOT by `PositionedReadable.read(long, …)` which is stateless); `fillExclusive(CachePin, long offset, long size)` extracted as a package-private static helper that takes the `ReadFile` so `PrefetchTask` can invoke it via the `readFile` field added to its constructor. (E) **Queue default reconciled** to `queueSize=64` everywhere (was claimed `1024` at line 304); peak-leak bound updated to `(16+64) × 8 MiB ≈ 640 MiB`. (F) **`RejectedExecutionException` at submit site**: consumer wraps `prefetchExecutor.execute(task)` in `try { … } catch (RejectedExecutionException e) { /* same recovery as DiscardAndCountHandler */ }` to handle post-shutdown submission races. (G) **`default` scanId hotspot**: documented as a known JVM-wide collision concentration with operator guidance to wire `fs.cached.scan-id` even on small workloads. Round-12/13/14 polish preserved: (a) **`staleScanIdRecoveries` moved from `IoStatistics` (per-stream) to `AggregatedIoStatistics` (bootstrap-level)**. The bump-site is `CacheBootstrap.withScanId`, which runs before any `CachingInputStream` exists; there is no per-stream IoStatistics to bump. The counter now lives on `bootstrap.aggregateIoStats` with a new direct `incStaleScanIdRecoveries()` method, and `AggregatedIoStatistics.add(IoStatistics)` does NOT merge it (it's not a per-stream signal). Adapter exposes via the existing bootstrap-level dynamic-IOStatistics path using `withLongFunctionCounter`. (b) **`releaseCurrentScanId()` added to the Phase 5a wiring commits list** with a concrete body, idempotency contract, and a dedicated intentional-nesting acceptance test. (c) **Bump-site code snippet fixed**: braces now parse correctly; `admissionGate()` returns `AdmissionResult` (not boolean); the snippet branches read `AdmissionResult adm = admissionGate(); if (readPct >= threshold && adm.admit()) … else if (readPct < threshold) … else …`. (d) **`AdmissionResult` flyweight pattern specified**: three static-final singletons (`ADMIT`, `BUDGET_REJECT`, `HEAP_REJECT`); no allocation in the hot read path. (e) **Bump-site invariant acceptance test mechanism specified**: a package-private debug counter `admissionGateFalseCount` on `CachingInputStream` is incremented at every false-branch and read by the test. (f) **Stale-slot recovery WARN rate-limited**: first 16 occurrences per JVM at WARN, subsequent at DEBUG; counter ticks regardless so operators see the full rate. (g) **Orphan-mutation triggers** now enumerate stale-slot recovery as a third trigger alongside explicit `withScanId.close()` and TCL-fired `removeScanTracker`. (h) Fixed-reason map for `prefetchSkipped` logs a deduped WARN on first unknown reason so contributor bugs are visible rather than silently absorbed by the `"other"` bucket.

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

   With 2^29 (~5.37×10^8) buckets, the birthday-paradox 1% collision threshold is **~3.3k files** (`k = sqrt(0.02·N) = sqrt(0.02·5.37e8) ≈ 3,277`). At the median Spark scan scale of 10k–50k files, P(at least one pair collides) ≈ 9%–90% (`1 - exp(-k²/(2N))`); at 100k files it is effectively 1.0. **Collisions are the norm, not the exception** at production sizes — the previous "rare collision" framing was wrong.

   **Why this is still acceptable.** Tolerability does NOT rest on collisions being rare; it rests on the per-pair locality of the impact. A collision causes two unrelated files to share one `TrackingData` entry, mixing the density signal **only for those two files**. With ~10 expected collisions at 100k files (the expected count is `~k(k-1)/(2N) ≈ k²/(2N)`, e.g., 100k² / 2·5.37e8 ≈ 9.3 collisions), ~20 files (~0.02%) see mixed-density prefetch signals. This is bounded — adding more files at the same N does increase the absolute collision count linearly in `k²/N`, but the impacted fraction stays at most `k/N` (~0.02% at 100k, ~0.1% at 500k).

   **Aggregate prefetch-precision impact (upper-tail only).** The `staleDensityAfterSeekAwayDoesNotExplodePrefetch` acceptance test caps wasted prefetch at 1.5× the random baseline for a stale-signal scenario. Collision-induced mixing is structurally similar **only on the upper tail** (false-positive prefetch on a cold file paired with a hot file): the colliding-pair tracker has weighted-averaged density rather than per-file density; for the cold member, the mixed density is above its true density, so the test's upper-tail bound (wasted prefetch ≤ 1.5× random baseline) applies. Files not involved in a collision (the overwhelming majority) see exactly the density they should.

   **Lower tail (missed prefetch) is NOT bounded by the existing test.** For the hot member of a collision pair, mixed density is below true density — prefetch may be suppressed below the threshold and the hot file loses its prefetch acceleration silently. At 100k files (~10 collisions, ~10 hot files possibly affected) and at 500k files (~233 collisions, ~233 hot files possibly affected), this is bounded in count but not in performance impact.

   **Mitigation (Phase 5c, not 5a):** Phase 5c adds an `IoStatistics.prefetchEligibleSuppressedBytes` counter that ticks when the admission gate's density predicate is the sole reason a chunk is dropped (i.e., `pendingPrefetch == null` AND `position in trigger-tail-fraction` AND `backoff elapsed` AND `readPct < threshold`). See §Phase 5c step 4 for the exact bump site. The counter is a **union signal**: it ticks for (a) genuine low-density streams correctly suppressed and (b) collision-induced false-negative suppression on hot files. Operators cannot disambiguate from this counter alone; the two **observable** signals available for attribution are: (1) the ratio `prefetchEligibleSuppressedBytes / readBytes` compared against a known-baseline workload — a step-change rise at constant workload suggests collision pressure; (2) correlation with `scanTrackerMaxEntries()` growth (high = more collision pressure). Both signals are already exposed via the dynamic gauge / IoStatistics surface. **Per-scan readPct distribution was considered as a third signal but is intentionally NOT exposed in Phase 5a/5c** — it would require a histogram surface on ScanTracker; deferred to the open-follow-ups list. A future per-tracker collision-counter would refine attribution; also tracked there.

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
- **`ScanTracker.size()` gauge.** Add `public int size() { return data.size(); }` (O(1) approx on `ConcurrentHashMap` — non-atomic but lock-free; acceptable for a gauge per Hadoop/JMX/Prometheus semantics). Implementation note: this method depends on the new `ConcurrentMap<TrackingId, AtomicReference<TrackingData>> data` field introduced earlier in this prework; the gauge cannot ship before the data refactor lands.

  Wired to two bootstrap-level accessors that Phase 5a then routes through `IoStatisticsAdapter`:
  - `CacheBootstrap.scanTrackerEntries()`: sum of `size()` across all active trackers (lazy on read; inconsistent under concurrent `recordReference` is acceptable for a gauge).
  - `CacheBootstrap.scanTrackerMaxEntries()`: max of `size()` across all active trackers. **Required** for the per-tracker LRU follow-up trigger — `entries` (sum) cannot distinguish "one tracker has 100k entries" from "100 trackers have 1k entries each".

  Both are exposed via `IoStatisticsAdapter`'s `gauges()` map as **live-view suppliers** (not a snapshot map). The adapter implements `org.apache.hadoop.fs.statistics.IOStatistics` and returns a `Map<String, Long>` from `gauges()` whose `get(...)` delegates to `bootstrap.scanTrackerEntries()` / `scanTrackerMaxEntries()` on each call — built on Hadoop's `org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.dynamicIOStatistics(...)` via `DynamicIOStatisticsBuilder.withLongFunctionGauge(String name, ToLongFunction<String> eval)` registered once per gauge name (HADOOP-17450, hadoop-common 3.3.1+; present at our pinned 3.4.1). The lambda receives the gauge name as input: `k -> bootstrap.scanTrackerEntries()`. **Note:** the `impl` sub-package is conventionally less stable than the top-level `org.apache.hadoop.fs.statistics` (annotated `@InterfaceStability.Unstable` at the package level in some Hadoop minors). The API has been stable across 3.3.1–3.5.0 in practice; a defensive IT exercises `IOStatisticsBinding.dynamicIOStatistics()` so a future Hadoop bump fails in CI rather than runtime.

  NOT placed on `IoStatistics`/`AggregatedIoStatistics`: those are typed-counter classes; `ScanTracker.size()` is a gauge that can decrease (on tracker eviction by `withScanId.close()` and the open-follow-up LRU) and so mismatches the counter data model.

  Acceptance tests: (a) `bootstrap.scanTrackerEntries() == 0` and `scanTrackerMaxEntries() == 0` on fresh bootstrap; (b) increment by 1 after first `recordReference` on a new (scanId, file) pair; (c) `scanTrackerEntries()` returns to 0 after the matching `withScanId.close()`.

Allocation overhead: at ~100k reads/sec → 200k 40-byte `TrackingData` allocations/sec → ~8 MB/s, comfortably within young-gen tolerance. Under CAS-retry contention this can double; document in the prework's microbenchmark.

Acceptance for prework:
- Existing `ScanTrackerTest` passes unchanged after the refactor.
- New unit test: 8 threads each call `recordReference(id, 1)` 1M times on a shared tracker; final `data(id).referencedBytes()` must equal exactly `8 × 1M` (no lost updates).
- Snapshot-monotonicity test: 8 writer threads + 1 reader thread polling `data(id).adjustedReadPct()` every 1ms for 5s — assert no negative value observed.
- Microbenchmark (JUnit, not CI-gated): record throughput vs the locked baseline as a regression-watch artifact.

### Phase 5a wiring

Commits in landing order:

1. **`CacheBootstrap.trackerFor(String scanId)`** — adds `private final ConcurrentMap<String, ScanTracker> scanTrackers`. Normalizes null/empty/whitespace to `"default"`. `computeIfAbsent(normalized, k -> new ScanTracker(k, this.loadQuantumBytes))` — uses the bootstrap's `loadQuantumBytes` field (already exposed via `loadQuantumBytes()`; the value is dropped by `ScanTracker` per velox parity but the constructor still requires it). No callers in this commit.
2. **`CacheBootstrap.currentScanId()` + `withScanId(String)`** — `ThreadLocal<String>` accessor plus `AutoCloseable withScanId(scanId)` that on `close()` (a) calls `threadLocal.remove()` AND (b) calls `removeScanTracker(scanId)` to evict the tracker entry from `bootstrap.scanTrackers` so the inner `TrackingData` map (potentially MB-scale on partitioned scans) is GC-eligible. Without this, long-lived JVMs (Spark driver, YARN NM with the FileSystem cache) accumulate trackers indefinitely.

   **Stale-slot recovery, NOT nested-rejection.** On entry, `withScanId` checks `threadLocal.get()`. The handling depends on the cause:
   - **Genuinely nested call on the same thread (rare; only happens when an outer try-with-resources is still active):** indistinguishable from the stale-slot case at runtime — the slot is set. The plan treats this as the recovery case below; integrators with intentional nesting must produce distinct scanIds *and* manually call `bootstrap.releaseCurrentScanId()` before the inner `withScanId`, which clears the slot without removing the outer tracker.
   - **Stale slot from a crashed prior task (common on Spark executor thread reuse):** the prior task entered `withScanId(...)` and crashed before `close()` ran. Behavior: log WARN ("stale scanId slot 'A' detected; auto-recovering — likely prior task crashed between withScanId and close") *for the first 16 occurrences per JVM, subsequent at DEBUG*, then call `removeScanTracker(staleScanId)` to evict the orphaned tracker, then set the slot to the new scanId. This **restores the benign-leak property of the dangling-tracker design** and prevents Spark thread-reuse cascade failures. The `AggregatedIoStatistics.staleScanIdRecoveries` counter (on `bootstrap.aggregateIoStats`; see §Phase 5a wiring step 7) ticks each recovery regardless of log level so operators see the full rate via the metrics surface.
   - **Configuration-key path:** `withScanId` is not used; no slot is set; no recovery needed.

   Stale-slot recovery is a **third trigger for the orphan-mutation case** alongside (a) explicit `withScanId.close()` before stream close and (b) TCL-fired `removeScanTracker` — see the dangling-tracker section below for the perf cost (~8 MB/s young-gen churn per still-alive orphan stream).

   Single-thread invariant on `close()`: the AutoCloseable's `close()` MUST run on the same thread that set the slot (Spark `TaskContext.addTaskCompletionListener` is forbidden because it can run on a different thread and either no-op the wrong slot or leak a wrong-thread orphan).

   **`releaseCurrentScanId()`** (new public method on `CacheBootstrap`): `public void releaseCurrentScanId() { threadLocal.remove(); }` — idempotent (no-op on missing slot), tracker map untouched. Use case: intentional nested scoping on the same thread. Pattern: `try (var outer = bootstrap.withScanId("outer")) { bootstrap.releaseCurrentScanId(); try (var inner = bootstrap.withScanId("inner")) { /* … */ } }`. Outer's `close()` still calls `removeScanTracker("outer")` (eviction is by scanId, not by slot identity), so the outer tracker evicts normally. Inner's `close()` evicts inner. No WARN, no `staleScanIdRecoveries` bump on the inner entry because `releaseCurrentScanId()` already cleared the slot.

   Also adds a public `void removeScanTracker(String scanId)` for integrators that resolve scanId via the `Configuration` key (no try-with-resources path). **Idempotent**: calling on a missing or already-removed scanId is a no-op (no exception). Javadoc warns: "Single-threaded use only. Do NOT pair with Spark `TaskContext.addTaskCompletionListener` — close may run on a different thread and leak the slot. Use try-with-resources inside the task body. **`removeScanTracker` itself IS the right `TaskCompletionListener` payload** for Configuration-key integrators — it is not ThreadLocal-bound and runs correctly on any thread." **Contract:** the tracker lives from first `trackerFor(scanId)` until matching `withScanId.close()` OR explicit `removeScanTracker(scanId)`.

   **Dangling-tracker / concurrent-remove race.** Both are benign for correctness but carry a perf cost worth quantifying:
   - **Orphan-mutation:** an in-flight `CachingInputStream` opened with scanId X retains a strong reference to the now-evicted ScanTracker after `withScanId(X).close()` (or after a `removeScanTracker(X)` from a TCL that fires before stream close). Its continued `recordReference`/`recordRead` calls mutate the orphan tracker; mutations are simply ignored by readers (admission gate, gauge, future `trackerFor` calls) and the tracker becomes GC-eligible after the stream closes. **Perf cost:** each orphan mutation is a `ConcurrentHashMap.computeIfAbsent` (lock-free but allocation-prone on miss) plus a CAS-loop `updateAndGet` that allocates a 40-byte `TrackingData`. At ~100k reads/sec per stream, an orphan-stream wastes ~8 MB/s of young-gen churn. **Integrator guidance:** close all CachingInputStreams in the task BEFORE calling `removeScanTracker` (or before the matching `withScanId.close()`). For Configuration-key + TCL integrators this means closing streams in the user code body before TCL fires; the orphan-cost path only matters if streams escape that body.
   - **Concurrent-remove vs trackerFor:** if thread A's `removeScanTracker(X)` interleaves with thread B's `computeIfAbsent` inside `trackerFor(X)`, B may create a freshly-allocated ScanTracker that immediately becomes an orphan from A's perspective. Same benign properties; same orphan-mutation cost if B then mutates.

   **ScanTrackerNeverRemovedTest** (in the §5a acceptance-tests list): `bootstrap.trackerFor("orphan"); assertThat(bootstrap.scanTrackerEntries()).isEqualTo(1); /* simulate forgotten close: no withScanId path, no removeScanTracker call */ Thread.sleep(100); assertThat(bootstrap.scanTrackerEntries()).isEqualTo(1); bootstrap.removeScanTracker("orphan"); assertThat(bootstrap.scanTrackerEntries()).isEqualTo(0);`. This test verifies the failure mode is **observable** via the gauge — it does NOT guarantee the system prevents leaks at runtime. Real production leaks accumulate silently between gauge-watch intervals. The open-follow-up LRU is defense-in-depth; a phantom-reference reaper that fires `removeScanTracker` when the scanId String becomes weakly reachable is one alternative path tracked as open follow-up.
3. **`CachedFsConfig.SCAN_ID = "fs.cached.scan-id"`** — README config-reference picks up the key. No behavior change.
4. **`CachingInputStream` constructor signature change** — gains `ScanTracker tracker`, `TrackingId trackingId`, `IoStatistics ioStats`. Constructor is package-private with a single grep-verified call site (`CachedFileSystem.open()`). The same commit updates the call site so the build stays green.
5. **`CachedFileSystem.open()` plumbing** — resolves scanId via the Decisions §1 precedence, calls `b.trackerFor(scanId)`, instantiates `IoStatistics ioStats = metricsEnabled ? new IoStatistics() : IoStatistics.NO_OP`, derives `TrackingId trackingId = TrackingId.of(fileNumHash(handle.fileNum()), 0)` per Decisions §6, passes `(tracker, trackingId, ioStats)` to `CachingInputStream`.

   **`Murmur3` utility class (v7.10).** New file `cached-fs-core/src/main/java/io/github/luciferyang/cachedfs/core/util/Murmur3.java`:
   ```java
   public final class Murmur3 {
     private Murmur3() {}
     /** MurmurHash3 32-bit finalizer; uniformly distributes ints across the 32-bit space. */
     public static int fmix32(int h) {
       h ^= h >>> 16;
       h *= 0x85ebca6b;
       h ^= h >>> 13;
       h *= 0xc2b2ae35;
       h ^= h >>> 16;
       return h;
     }
   }
   ```
   Six lines, no dependencies. Unit test asserts known fixed-point values **verified against Apache Commons Codec / Guava `Hashing.murmur3_32().hashInt(i).asInt()`**: `fmix32(0) == 0x00000000`, `fmix32(1) == 0x514e28b7`, `fmix32(2) == 0x30f4c306`. (The constant `0xe6546b64` referenced in earlier plan drafts is the body-mix additive used inside the main MurmurHash3 loop — NOT an fmix32 output; do not use as a fixed-point literal.)

   **`fileNumHash` implementation:** `static int fileNumHash(long fileNum) { return Murmur3.fmix32((int) fileNum) & ((1<<29) - 1); }`. Today `FileHandle.fileNum()` comes from `StringIdMap` which assigns sequential ints from 0 — a raw XOR-fold would produce zero collisions until id ≥ 2^29 (degenerate distribution; the birthday-paradox analysis in §Decisions §6 would not apply). `Murmur3.fmix32` uniformly distributes sequential ids across the 32-bit space; masking to 29 bits then satisfies `TrackingId.of`'s range constraint. Acceptance test: hash the first 1M sequential ids; assert the bucket-population stddev is within 1.5× of the theoretical √(N/B) for uniform distribution (N=10^6, B=2^29 → ~0.0019 mean per bucket; stddev ~0.043).
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
- **ScanTrackerNeverRemoved (observability-only)**: `bootstrap.trackerFor("orphan")`; `long before = bootstrap.scanTrackerEntries(); long after = bootstrap.scanTrackerEntries();` assert `before == after == 1` (proving the leak is detectable but not auto-recovered without an explicit close/remove call — back-to-back reads avoid the wall-clock cost of a sleep); then `bootstrap.removeScanTracker("orphan")` and assert `scanTrackerEntries() == 0`. Test verifies the gauge surfaces the failure mode — it does not guarantee runtime leak prevention.
- **Stale-slot recovery (NOT rejection)**: `bootstrap.withScanId("A")` (no close, simulate a prior task that crashed); on the same thread call `bootstrap.withScanId("B")`. Assert: no exception; WARN log emitted with a "stale scanId slot" message; `bootstrap.scanTrackerEntries()` reflects only "B"'s tracker (the "A" tracker was auto-removed); `bootstrap.aggregateIoStats.staleScanIdRecoveries() == 1`. The "B" close then evicts B normally.
- **Intentional nesting via releaseCurrentScanId**: `try (var outer = bootstrap.withScanId("outer")) { assertThat(bootstrap.currentScanId()).isEqualTo("outer"); bootstrap.releaseCurrentScanId(); assertThat(bootstrap.currentScanId()).isNull(); try (var inner = bootstrap.withScanId("inner")) { assertThat(bootstrap.currentScanId()).isEqualTo("inner"); /* inner work */ } } assertThat(bootstrap.aggregateIoStats.staleScanIdRecoveries()).isEqualTo(0); assertThat(bootstrap.scanTrackerEntries()).isEqualTo(0); // both evicted
- **releaseCurrentScanId idempotency**: call `bootstrap.releaseCurrentScanId()` on a thread with no active slot; assert no exception, `bootstrap.currentScanId() == null`. Call twice in a row; second call is a no-op.
- **removeScanTracker idempotency**: call `removeScanTracker("ghost")` on a never-created scanId; assert no exception, no change to `scanTrackerEntries()`. Call `removeScanTracker(scanId)` twice in a row; assert second call is a no-op.
- **withScanId close evicts tracker**: `try (var ig = bootstrap.withScanId("ephemeral")) { bootstrap.trackerFor("ephemeral").recordReference(TrackingId.of(0,0), 4096); assertThat(bootstrap.scanTrackerEntries()).isEqualTo(1); } assertThat(bootstrap.scanTrackerEntries()).isEqualTo(0);`.
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

  **Failure-mode contract.** If the integrity guard fires, the IllegalStateException (1) escapes `PrefetchTask.run()`'s innermost decrement step, (2) is caught by `ThreadPoolExecutor`'s uncaught-exception handler (logged at WARN with the stack), (3) does NOT mask the consumer-side handoff because the outer finally has already completed the future and run `clearPendingPrefetchIf`. The `pendingPrefetchBytes` counter is left in an inconsistent state for the lifetime of the JVM — acceptable because the integrity-check trigger represents a programming bug, not a runtime condition; the inconsistency would only matter if a third caller external to PrefetchTask desyncs the counter.

5c.0 does NOT specify increment/decrement call sites; that wiring lives entirely in 5c-proper.

Acceptance for 5c.0:
- Unit test: increment by `4×1024`, sum returns 4096; decrement by 1024, sum returns 3072.
- Public-API reach test in `cached-fs-hadoop`'s test tree: `CacheBootstrap.get().orElseThrow().ramCache().pendingPrefetchBytes()` returns 0 on a fresh bootstrap.
- Existing test suite passes.

## Phase 5c — Async prefetch (large)

### Class layout decision

`PrefetchTask` and `DiscardAndCountHandler` are **sibling top-level package-private classes** in `io.github.luciferyang.cachedfs.hadoop` (NOT nested static classes of `CachingInputStream`). Trade-off:

- **Why sibling (chosen):** `DiscardAndCountHandlerTest` and `PrefetchTaskTest` exercise the handler/task in isolation — constructing a real `CachingInputStream` requires a live `CacheBootstrap`, `AsyncDataCache`, `ReadFile`, scanId, and IoStatistics; that's prohibitive for handler-only unit tests. As siblings, the test instantiates a mock `CachingInputStream` (just enough for `clearPendingPrefetchIf` and `lastRejectionNanos`) and verifies the handler's exact behavior on queue-full.
- **Why not nested:** if PrefetchTask/DiscardAndCountHandler were `private static` nested classes, `PENDING_VH` / `lastRejectionNanos` / `clearPendingPrefetchIf` could all revert to `private` for tighter encapsulation. Test surface would have to test the rejection path indirectly via the public stream API.
- **Surface trade:** the package-private widening is contained to `io.github.luciferyang.cachedfs.hadoop`; no other package can reach these symbols. Javadoc on each marks them `@apiNote internal`.
- **Within-package invariant.** PrefetchTask and DiscardAndCountHandler are the **only legitimate writers** of `PENDING_VH`, `lastRejectionNanos`, and `clearPendingPrefetchIf`. Future classes added to this package MUST respect this — direct CAS on PENDING_VH from a third class would break the submission single-owner contract; writing lastRejectionNanos from a third class would break the per-stream backoff invariant. Enforcement is by convention; no runtime guard.

### Changes

1. **`CacheBootstrap` prefetch executor.** New field `ExecutorService prefetchExecutor`, constructed as `ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueSize), daemonFactory, new DiscardAndCountHandler())`. Daemon ThreadFactory names threads `"cached-fs-prefetch-N"` with `daemon=true` and an uncaught-exception handler that logs at WARN.

   **`PrefetchTask` is a named class** (NOT a lambda) so the rejection handler can downcast and recover state. Fields: `(CachingInputStream owner, IoStatistics ioStats, AsyncDataCache cache, ReadFile readFile, int chunkSize, RawFileCacheKey nextKey, long nextOffset, CompletableFuture<RawFileCacheKey> future)`. **chunkSize is `int`** to match the existing `CachingInputStream.fillExclusive` signature (cache chunks are bounded; the chunk-size config validator clamps to `[64 KiB, Integer.MAX_VALUE]`). The `readFile` field is the same `ReadFile` instance that backs the owner's per-chunk fill path.

   **`fillExclusive` extraction (v7.9):** the existing `private void fillExclusive(CachePin exclusivePin, long chunkStart, int chunkSize)` on `CachingInputStream` is promoted to a **package-private static** with signature `static void fillExclusive(ReadFile readFile, CachePin exclusivePin, long chunkStart, int chunkSize)`. The body's only `this` reference is `handle.readFile()`, which becomes the new `readFile` parameter. **Call-site update:** `CachingInputStream.copyChunk` (currently calling `this.fillExclusive(pin, start, size)`) is rewritten to `fillExclusive(handle.readFile(), pin, start, size)` in the same commit. `PrefetchTask.run()` invokes the same static via `CachingInputStream.fillExclusive(readFile, excPin, nextOffset, chunkSize)`, eliminating the need for either a cross-class private helper or duplicating the preadv logic.

   `PrefetchTask.run()` is the body shown in step 4 below. PrefetchTask owns the future; on every exit path (success, exception, discard) the future is completed exactly once.

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
   4. If timeout, `prefetchExecutor.shutdownNow()` AND a second `awaitTermination(10s)`. If STILL not terminated, log at ERROR and proceed. **Upper bound on leaked pins:** prefetch tasks each hold at most one chunk's exclusive pin at a time AND the executor is bounded to `threads` concurrent tasks AND the queue is capped at `queueSize`. The maximum leaked pin bytes is `(threads + queueSize) × loadQuantumBytes`. Document this bound; with default `threads=availableProcessors` (~16) and `queueSize=64` and `loadQuantum=8 MiB`, peak leak ≈ `80 × 8 MiB = 640 MiB` — bounded and operator-tunable. Operators with tighter budgets should drop `queueSize` further (Phase 5c knob `fs.cached.prefetch.queue-size`).
   5. After executor terminates: `handleFactory.closeAll()`, `ssdCache.close()`, `ramCache.close()`.
3. **`CachingInputStream` prefetch state.** New fields:
   - `private volatile CompletableFuture<RawFileCacheKey> pendingPrefetch` — the in-flight prefetch handle (key, not pin). VarHandle CAS is used to coordinate submission vs. rejection-handler reset.
   - `volatile long lastRejectionNanos` — **package-private** so sibling `DiscardAndCountHandler` in the same package can write it. Initialized in the **constructor** as `System.nanoTime() - REJECTION_BACKOFF_NS - 1` so the first prefetch attempt is never back-pressured AND the overflow edge that a static `Long.MIN_VALUE/2` sentinel introduces (146-year-uptime regime; JVMs that happen to start with `nanoTime()` near `Long.MAX_VALUE`) is eliminated. Volatile is defense-in-depth: per the JDK invariant in §step 1, both the write (in the handler) and the read (in the admission gate, §step 4) execute on the same consumer thread, but `volatile` removes the assumption that no future refactor will move submission off-thread.
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
4. **Consumer thread, post-chunk-N read**: if `pendingPrefetch == null` AND `position >= chunkNEnd - (loadQuantum * triggerTailFraction)` AND `tracker.data(trackingId).readPct() >= prefetchPctThreshold` AND `admissionGate()` AND `System.nanoTime() - lastRejectionNanos >= REJECTION_BACKOFF_NS` (per-stream backoff, default 100 ms via `fs.cached.prefetch.rejection-backoff-ms`): submit the prefetch task via `prefetchExecutor.execute(task)`, wrapped in `try { … } catch (RejectedExecutionException e) { /* same recovery as DiscardAndCountHandler: complete future exceptionally, bump prefetchSkipped("queue_full"), clearPendingPrefetchIf, set lastRejectionNanos */ }` to handle the post-shutdown race where `closed.set(true)` won the visibility race to `closed.get()` but the executor moved to SHUTDOWN between then and the `execute()` call. The backoff prevents a hot resubmit loop under sustained queue saturation: after a rejection the stream waits 100 ms before re-attempting prefetch submission, while the consumer's normal per-chunk path proceeds at its native rate.

   **Consumer state machine (v7.12).** The trigger predicate requires `chunkNEnd` and a notion of "post-chunk-N read." `CachingInputStream` maintains:
   - `private final AtomicLong sequentialReadHighWater = new AtomicLong(-1L)` — sentinel `-1L` means "no read observed yet; prefetch disabled until armed." **AtomicLong (not plain `long`)** because `PositionedReadable.read(long, byte[], int, int)` is contractually thread-safe per Hadoop's `PositionedReadable` javadoc AND can run concurrently with sequential `read()` operations on the same stream (PositionedReadable explicitly does not modify the current position, so the Hadoop contract permits the mix). Every update site MUST tolerate concurrent callers; **all three sites use `updateAndGet` with monotone-advance semantics — no plain `set()` anywhere** (a plain set would unconditionally clobber a higher CAS-advanced value from a concurrent peer, violating monotonicity).
   - **Updated via CAS** in three sites:
     1. **Sequential read path** (`InputStream.read()` / `Seekable.read()` after per-chunk fill): `seqHWM.updateAndGet(prev -> (prev == -1L) ? (position + chunkSize) : Math.max(prev, position + chunkSize))`. Monotone advance — never regresses HWM, even if a concurrent positional thread has already advanced it past `position + chunkSize`.
     2. **PositionedReadable.read(long pos, byte[] b, int off, int len)**: `seqHWM.updateAndGet(prev -> (prev == -1L) ? (pos + len) : (pos == prev) ? (pos + len) : prev)`. Bootstrap from the `-1L` sentinel happens on the FIRST positional call regardless of `pos` (so a `readFully(0, …)` first call arms the state machine). Subsequent positional calls advance the HWM only when the call is contiguous (pos == prev HWM); scattered calls leave it unchanged.
     3. **Seekable.seek(long)**: `seqHWM.set(-1L)` — `set` is acceptable here because `Seekable.seek` is contractually NOT thread-safe vs concurrent seeks/reads on the same instance (Hadoop `Seekable` javadoc); the integrator guarantees serial execution against seek. Field javadoc reiterates this asymmetry.
   - `currentChunkEnd` is **derived inline** from `seqHWM.get()` rounded up to the nearest chunk boundary inside the trigger predicate — NOT a separate field (eliminates the v7.10 "two writers, two fields" race). The same local `hwm` is reused for both the gate check and the chunk-boundary derivation; no double-read.

   **`Seekable.getPos()` reads `position`** (the Hadoop byte-cursor contract is unchanged from current impl). The HWM is a prefetch-only internal field with no external observers.

   Trigger predicate amended (gated on the HWM being armed):
   ```
   long hwm = seqHWM.get();
   if (pendingPrefetch == null && hwm > 0
       && position >= ((hwm + chunkSize - 1) / chunkSize) * chunkSize - (loadQuantum * triggerTailFraction)
       && /* readPct + admissionGate + backoff … */) { … }
   ```
   Acceptance tests: (a) 1000 scattered `readFully(pos, …)` calls (pos values randomly distributed) produce `prefetchBytes == 0`; (b) 1000 contiguous `readFully(pos, …)` calls starting at `pos=0` with `pos += chunkSize` each iteration trigger prefetch on the same cadence as a sequential `read(…)` loop, AND final `seqHWM.get() == 1000 * chunkSize` (proves the state machine actually advanced, not just bootstrapped-and-froze); (c) 8 threads each issuing `readFully(threadIdx*chunkSize, …)` then `readFully((threadIdx+1)*chunkSize, …)` (interleaved contiguous patterns) — no HWM corruption, no negative HWM, monotonicity holds, AND final HWM advances to at least `8*chunkSize` (proving CAS-advance isn't dead-zoned); (d) **mixed sequential+positional**: one thread runs `InputStream.read()` loop while another thread runs `readFully(pos, …)` loop against the SAME `CachingInputStream`; both contiguous, interleaved at random; assert no HWM regression observable via a probe thread sampling `seqHWM.get()` every 10ms, AND final HWM equals the higher of the two threads' final position-plus-length.

   Field javadoc: `/** Prefetch-only high-water-mark. All read-path writers (sequential and positional) use updateAndGet with monotone-advance semantics — Hadoop permits mixed sequential + positional concurrent access on one stream, and plain set would clobber a higher CAS-advanced peer value. Only Seekable.seek uses set(-1L) as a reset, valid because Seekable.seek is contractually single-threaded per Hadoop. Sentinel -1L disables prefetch arming. */`.

   `triggerTailFraction` is a per-bootstrap config `fs.cached.prefetch.trigger-tail-fraction` (default `0.5` — submit when the consumer is past the chunk midpoint), exposed as a static read once at bootstrap install.

   **Bump-site for `prefetchEligibleSuppressedBytes`** (the lower-tail observability counter from §Decisions §6):
   ```java
   if (pendingPrefetch == null
       && position >= chunkNEnd - (loadQuantum * triggerTailFraction)
       && System.nanoTime() - lastRejectionNanos >= REJECTION_BACKOFF_NS) {
     // Position-eligible AND not in backoff: one of {submit, density-suppressed,
     // admission-skipped} fires. Pre-evaluate the gate so we can capture its
     // reason on the admission-skip branch.
     double readPct = tracker.data(trackingId).readPct();
     AdmissionResult adm = admissionGate();
     if (readPct >= prefetchPctThreshold && adm.admit()) {
       prefetchExecutor.execute(prefetchTask);                 // submit branch
     } else if (readPct < prefetchPctThreshold) {
       ioStats.incPrefetchEligibleSuppressed(chunkSize);       // density-suppressed
       admissionGateFalseCount++;                              // package-private debug counter (see acceptance test below)
     } else {  // readPct >= threshold AND !adm.admit()
       ioStats.incPrefetchSkipped(adm.reason(), chunkSize);    // "budget" | "heap_pressure"
       admissionGateFalseCount++;
     }
   }
   ```
   The position guard, backoff check, and `pendingPrefetch != null` guard all happen BEFORE the branch chain — so each counter measures exactly one disjoint cause. `admissionGate()` returns a typed `AdmissionResult` (see record spec below) so the rejection reason is captured exactly once at the call site, eliminating the boolean+sideband pattern.

   **`AdmissionResult` flyweight.** The record has three static-final singletons:
   ```java
   record AdmissionResult(boolean admit, String reason) {
     static final AdmissionResult ADMIT       = new AdmissionResult(true,  "");
     static final AdmissionResult BUDGET_REJECT  = new AdmissionResult(false, "budget");
     static final AdmissionResult HEAP_REJECT    = new AdmissionResult(false, "heap_pressure");
   }
   ```
   `admissionGate()` returns one of the three — no allocation per evaluation, eliminating the ~3 MB/s young-gen churn that a per-call new-record would incur in the hot read path.

   **Bump-site invariant + acceptance test mechanism.** The debug counter `volatile long admissionGateFalseCount` on `CachingInputStream` (package-private, declared `volatile` so the JUnit-thread accessor read has a happens-before edge against the consumer-thread writes — production correctness doesn't need this because admission-gate reads/writes are single-threaded, but the acceptance test runs on a separate thread and needs the visibility guarantee, default 0) is incremented at every false-branch above. The field carries a co-located javadoc reiterating the single-writer invariant. Acceptance test reads this counter post-run via a package-private accessor `long admissionGateFalseCountForTesting()` and asserts the invariant **in bytes** (all terms have matching units):

   ```
   admissionGateFalseCount * chunkSize == prefetchSkipped("budget") + prefetchSkipped("heap_pressure") + prefetchEligibleSuppressedBytes
   ```

   The `"queue_full"` bucket is EXCLUDED because it fires in `DiscardAndCountHandler` AFTER the admission gate has already passed (see §step 3); `queue_full` does NOT correspond to a gate-false branch. The test must `.join()` the consumer thread (or await a latch released after the last admission-gate evaluation) before reading `admissionGateFalseCount` to ensure the volatile read happens-after the last consumer write. This codifies the invariant that every admission-gate-false branch bumps exactly one dedicated byte counter by exactly `chunkSize`.

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
       CachingInputStream.fillExclusive(readFile, excPin, nextOffset, chunkSize);  // preadv via PrefetchTask's readFile field
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
   // Initialized in the constructor as `System.nanoTime() - HEAP_PRESSURE_TTL_NS - 1`
   // so the first call always refreshes AND the static-sentinel overflow edge
   // (when nanoTime() happens to start near Long.MAX_VALUE / 2) is eliminated.
   private final AtomicLong heapPressureLastCheckedNs;
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

7. **`CachingInputStream.close()`**: set `closed = true`, then if `pendingPrefetch != null`: `var f = pendingPrefetch; f.cancel(false); clearPendingPrefetchIf(f);`. `cancel(false)` does not interrupt running tasks; the task's finally block decrements the counter and CASes the slot regardless. The explicit `clearPendingPrefetchIf(f)` here is defense-in-depth: if the consumer somehow re-enters before the task's finally runs (it shouldn't per the not-thread-safe contract), the slot is already null and a fresh prefetch can be submitted on a subsequent read of a re-opened stream. The consumer never holds the prefetch task's pin.
8. **`seek()` invalidation**: if `position` moves outside `[chunkN+1 start, chunkN+1 end)`, AND `pendingPrefetch != null`: `var f = pendingPrefetch; f.cancel(false); clearPendingPrefetchIf(f);`. Both steps are required — without the explicit `clearPendingPrefetchIf`, the cancelled future stays pinned in the slot until the still-running task's finally clears it, and the consumer's admission gate at line 321 sees `pendingPrefetch != null` and silently suppresses all subsequent prefetch submissions for the (potentially long) remaining task runtime. **Note:** the in-flight task may still complete and pin a now-irrelevant chunk; that pollutes RAM cache temporarily but the pin is released in the task's finally and TTL eviction reclaims the chunk normally. An acceptance test (`seekAwayDuringPrefetchDoesNotSuppressNextPrefetch`) seeks to a new range immediately after submitting prefetch and asserts that a subsequent prefetch submission at the new position is NOT blocked by the cancelled-but-running prior task.

### 5c new IoStatistics counters

Phase 5c adds three counters to `IoStatistics`:
- `prefetchSkipped(reason)` — bump-with-reason for **all** non-density admission failures: queue-full rejection (`reason="queue_full"`), byte-budget admission failure (`reason="budget"`), heap-pressure admission failure (`reason="heap_pressure"`), and any future `RejectedExecutionException` reason. Implementation uses a small fixed reason → AtomicLong map; only well-known reasons accepted (unknown → silently routes to a `prefetchSkipped("other")` bucket).
- `prefetchEvictedBeforeUse()` — TTL evicted the prefetched entry before the consumer awaited.
- `prefetchEligibleSuppressedBytes()` — admission gate's density predicate rejected a position-eligible, backoff-elapsed chunk; lower-tail observability for collision-induced suppression (see §Decisions §6). **Union signal**: bumps on both genuine cold-scan suppression and collision-induced false-negative suppression; not disambiguable from this counter alone — operators correlate with `scanTrackerMaxEntries()` and the `prefetchEligibleSuppressedBytes / readBytes` ratio vs known baseline.

**Phase 5a adds one bootstrap-level counter** (NOT per-stream — the bump-site has no `IoStatistics` in scope):
- `AggregatedIoStatistics.staleScanIdRecoveries()` — bumped when `withScanId` enters with a stale ThreadLocal slot and auto-recovers (see Phase 5a wiring §2). Lives on `bootstrap.aggregateIoStats` (which is `AggregatedIoStatistics`, not `IoStatistics`). Bump path: `bootstrap.aggregateIoStats.incStaleScanIdRecoveries()` inside the stale-slot branch of `withScanId`. `AggregatedIoStatistics.add(IoStatistics)` does **NOT** merge this counter — it has no per-stream source. Exposed via the existing bootstrap-level dynamic-IOStatistics path (alongside `scanTrackerEntries` / `scanTrackerMaxEntries`) using `DynamicIOStatisticsBuilder.withLongFunctionCounter(name, ToLongFunction<String>)` (the counter variant of `withLongFunctionGauge`). Adapter name: `cachedfs_stale_scan_id_recoveries`. Surfaces crashing-task patterns where prior tasks fail before `close()`.

  **`AggregatedIoStatistics` counter partition (class-level javadoc):** the class now hosts two semantically distinct counter groups. The class javadoc partitions them and prescribes a naming convention so future contributors can tell at-a-glance which group a counter belongs to:
  1. **Merged-from-stream counters** (the original surface): sum of per-stream `IoStatistics` values, populated by `add(IoStatistics)`. Examples: `readBytes`, `prefetchBytes`, `prefetchSkippedByReason`, `prefetchEligibleSuppressedBytes`, `prefetchEvictedBeforeUse`. Naming: same as the per-stream field. `add(IoStatistics)` MUST merge these.
  2. **Bootstrap-only counters** (new in 5a): JVM-wide signals bumped directly by `CacheBootstrap`. Currently: `staleScanIdRecoveries`. Naming convention: future additions MUST use a `bootstrap*` prefix; `staleScanIdRecoveries` keeps its un-prefixed name because it is already an external metric surface (`cachedfs_stale_scan_id_recoveries`) and carries a field-adjacent javadoc explicitly marking it as the grandfathered exception: `/** Un-prefixed by exception — name is locked to the existing cachedfs_stale_scan_id_recoveries external metric. New bootstrap-only counters MUST use the bootstrap* prefix per class javadoc. */`. `add(IoStatistics)` MUST NOT merge these — they have no per-stream source. An acceptance test runs `add(...)` with a stream whose counters are set and asserts the bootstrap-only group is untouched.

All three Phase 5c prefetch counters are byte-only (no event count partner) since their use cases are about volume; `staleScanIdRecoveries` is event-count-only (no bytes meaningful). **Open follow-up (lower priority — rate alerting via `rate(counter[1m])` works equally well on bytes or events)**: add event-count partners for the three prefetch counters if operators need alerting rate-of-suppression-events rather than rate-of-suppressed-bytes; mirrors the existing pattern in other IoStatistics pairs. Adapter exposes the prefetch counters under `cachedfs_stream_prefetch_skipped_bytes`, `cachedfs_stream_prefetch_evicted_bytes`, and `cachedfs_stream_prefetch_eligible_suppressed_bytes`. Implementation order: the IoStatistics fields and inc* methods land alongside Phase 5c.0; `AggregatedIoStatistics` is introduced in 5a wiring step 7 with the `staleScanIdRecoveries` field already present; `AggregatedIoStatistics.add(IoStatistics)` snapshot loop includes the three prefetch counters (and explicitly skips `staleScanIdRecoveries`) — verified by an acceptance test. **Bump-site invariant for future contributors** (scoped to prefetch admission-GATE paths only — does NOT cover `staleScanIdRecoveries` or the `queue_full` rejection-handler path): every prefetch admission-gate-FALSE branch MUST bump a dedicated `IoStatistics` counter (a known `prefetchSkipped(reason)` bucket — `"budget"` or `"heap_pressure"` — or `prefetchEligibleSuppressedBytes`); a new admission-gate failure mode that falls through silently (or wrongly into `prefetchEligibleSuppressedBytes`) is a design-invariant violation. Acceptance test asserts `admissionGateFalseCount * chunkSize == prefetchSkipped("budget") + prefetchSkipped("heap_pressure") + prefetchEligibleSuppressedBytes` (all terms in bytes; the `"queue_full"` bucket is structurally outside this invariant because it fires after the gate has passed).

**`prefetchSkipped(reason)` reason map.** Implemented as `Map<String, AtomicLong>` (immutable structure, mutable AtomicLong values; the keyset is fixed at construction). On unknown reason: the counter routes to the `"other"` bucket AND logs a single deduped WARN per unknown key (via `ConcurrentHashMap<String,Boolean> seenUnknownReasons`). This surfaces contributor bugs (someone added a new rejection mode without registering its reason) rather than silently absorbing them.

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
