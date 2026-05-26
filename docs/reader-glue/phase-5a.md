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

