# Reader Glue Port Plan — velox §5

> **Status:** v8.2, 2026-05-26 (HEAD = c6e7b3e). Plan-only; no code changes yet. See `CHANGELOG` below for the v5→v8.2 convergence history (30+ santa-method review rounds).
>
> ### CHANGELOG (terse)
>
> - **v8.2** — overview status-banner reformat (CHANGELOG-style); collision math de-duplicated.
> - **v8.1** — phase-5c.md `pin.close()` syntax fix; phase-5b release-order rationale (FIFO/queue, not stack) + partial-promotion safety; phase-5c-0 WARN-not-throw integrity guard with unconditional decrement.
> - **v8.0** — split monolithic plan into per-phase docs (overview, 5a, 5b, 5c-0, 5c, followups) so each converges independently.
> - **v7.21** — releaseFailedExclusive package-visibility, admissionGateFalseCount → AtomicLong, heapPressureTtlNs instance field (config knob now effective), state-machine text drift.
> - **v7.20** — sequential HWM moved to explicit `compareAndSet` loop so `seqHwmRegimeResets` bumps exactly once per regime change.
> - **v7.18** — `seqHwmRegimeResets` event-count counter exposed via dynamic IOStatistics gauge.
> - **v7.17** — sequential CAS regime-change reset (`|prev − candidate| > 2 chunks → reset`) breaks the positional-bootstrap dead-zone.
> - **v7.12** — all HWM read-path writers unified under `updateAndGet` (PositionedReadable contractually concurrent w/ sequential reads).
> - **v7.6** — `staleScanIdRecoveries` moved to `AggregatedIoStatistics`; `releaseCurrentScanId()` API for intentional nesting.
> - **v7.5** — stale-slot recovery semantics (WARN + auto-recover) replacing nested-rejection (Spark thread-reuse cascade fix).
> - **v7.10–v7.4** — `prefetchEligibleSuppressedBytes` bump-site spec, `AdmissionResult` flyweight, `RejectedExecutionException` catch at submit, queue-default reconciled to 64, fileNumHash via Murmur3.fmix32, ScanTracker.size() / scanTrackerMaxEntries() gauges, AggregatedIoStatistics counter partition.
> - **v6→v7** — TrackingId 29-bit node space, PrefetchTask non-lambda + `execute()` not `submit()`, package-private `PENDING_VH` + co-located helper, key-not-pin design, single-CAS-winner heap-pressure refresh.
> - **v5** — initial reader-glue plan (CachedBufferedInput + ScanTracker port from velox §5).

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

   **Why this is still acceptable.** Tolerability does NOT rest on collisions being rare; it rests on the per-pair locality of the impact. A collision causes two unrelated files to share one `TrackingData` entry, mixing the density signal **only for those two files**. Expected collision count `≈ k²/(2N)`; at 100k files this is ~9.3, affecting ~20 files (~0.02%). At 500k files ~233 collisions / ~466 files (~0.1%). The impacted fraction stays at most `k/N`.

   **Aggregate prefetch-precision impact (upper-tail only).** The `staleDensityAfterSeekAwayDoesNotExplodePrefetch` acceptance test caps wasted prefetch at 1.5× the random baseline for a stale-signal scenario. Collision-induced mixing is structurally similar **only on the upper tail** (false-positive prefetch on a cold file paired with a hot file): the colliding-pair tracker has weighted-averaged density rather than per-file density; for the cold member, the mixed density is above its true density, so the test's upper-tail bound (wasted prefetch ≤ 1.5× random baseline) applies. Files not involved in a collision (the overwhelming majority) see exactly the density they should.

   **Lower tail (missed prefetch) is NOT bounded by the existing test.** For the hot member of a collision pair, mixed density is below true density — prefetch may be suppressed below the threshold and the hot file loses its prefetch acceleration silently. At 100k files (~10 collisions, ~10 hot files possibly affected) and at 500k files (~233 collisions, ~233 hot files possibly affected), this is bounded in count but not in performance impact.

   **Mitigation (Phase 5c, not 5a):** Phase 5c adds an `IoStatistics.prefetchEligibleSuppressedBytes` counter that ticks when the admission gate's density predicate is the sole reason a chunk is dropped (i.e., `pendingPrefetch == null` AND `position in trigger-tail-fraction` AND `backoff elapsed` AND `readPct < threshold`). See §Phase 5c step 4 for the exact bump site. The counter is a **union signal**: it ticks for (a) genuine low-density streams correctly suppressed and (b) collision-induced false-negative suppression on hot files. Operators cannot disambiguate from this counter alone; the two **observable** signals available for attribution are: (1) the ratio `prefetchEligibleSuppressedBytes / readBytes` compared against a known-baseline workload — a step-change rise at constant workload suggests collision pressure; (2) correlation with `scanTrackerMaxEntries()` growth (high = more collision pressure). Both signals are already exposed via the dynamic gauge / IoStatistics surface. **Per-scan readPct distribution was considered as a third signal but is intentionally NOT exposed in Phase 5a/5c** — it would require a histogram surface on ScanTracker; deferred to the open-follow-ups list. A future per-tracker collision-counter would refine attribution; also tracked there.

   **Backward compatibility note.** Bit-width change to `TrackingId` is binary-incompatible if any future code persists `TrackingId.id` (e.g., to an SSD scoring snapshot). Today no such persistence exists; document the requirement: any future serialized state including `TrackingId` must carry a version byte to allow re-interpretation.

