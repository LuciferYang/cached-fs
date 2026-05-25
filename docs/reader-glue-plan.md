# Reader Glue Port Plan — velox §5

> **Status:** draft, 2026-05-25. Plan-only; no code changes yet.

## Goal

Port velox's `CachedBufferedInput` + `ScanTracker` reader-side wiring (velox-file-read-cache.md §5) into cached-fs. Today the Hadoop decorator's `CachingInputStream` does per-chunk fetches via `findOrCreate`; it does not coalesce multi-chunk IO, prefetch, or feed `ScanTracker` / `IoStatistics`.

## Existing inventory

| Piece | Location | State |
| --- | --- | --- |
| `ScanTracker` | `cached-fs-core/.../tracker/ScanTracker.java` (109 lines) | Implemented (refs/reads/snapshot), **not wired** into any reader |
| `TrackingId` / `TrackingData` | same package | Implemented; `readPct`, `adjustedReadPct`, `readDensity` available |
| `IoStatistics` | `cached-fs-core/.../stats/IoStatistics.java` (159 lines) | Implemented (read/prefetch/ssdRead/ramHit/rawOverreadBytes/latencies), **not wired** |
| `CoalesceIo` | `cached-fs-core/.../CoalesceIo.java` (107 lines) | Implemented (gap-based grouping primitive), **unused by CachingInputStream** |
| `CoalescedLoad` | `cached-fs-core/.../CoalescedLoad.java` (293 lines) | Implemented (multi-entry IO load via `preadv` into many pins), **unused by CachingInputStream** |
| `CachingInputStream` | `cached-fs-hadoop/.../CachingInputStream.java` (337 lines) | Per-chunk fetch only. Implements `Seekable` + `PositionedReadable`. |

Gap: the primitives exist in core; the reader-side glue in cached-fs-hadoop does not use them.

## Velox §5 mapping → Java port

Velox's design is built around the Hive connector, which orchestrates many columns (streams) per file per stripe. Hadoop's `FSDataInputStream` is one stream per file; column-level orchestration lives in the consumer (Parquet/ORC reader). Implications:

- `ScanTracker`'s scope in velox is **per-(TableScan, stream)**. In a Hadoop port we don't see the consumer's column structure, so the natural scope shrinks to **per-(scanId, file)** at most — or **per-file** if we don't have a scan-id source. Spark passes a scan-id through `TaskContext` but Hadoop's `FileSystem.open` doesn't propagate it. Compromise: optional `scanId` on the Hadoop config, default to "global".
- `CachedBufferedInput` orchestrates many streams in one buffered input. The Hadoop equivalent is one `CachingInputStream` per file. **Multi-chunk-within-a-single-file** is what we can coalesce; cross-file coalescing isn't possible at this layer (the consumer manages per-file opens independently).
- `prefetchPct_` (dormant in OSS velox §5.2.7) we can leave dormant too, or wire it for sequential-scan detection.

## Proposed phases

### Phase 5a — ScanTracker + IoStatistics wiring (small, foundation)

Wire the existing primitives into `CachingInputStream`. No new IO patterns; same per-chunk fetch, but with observability + density signals.

**Changes:**

1. **`CacheBootstrap`** holds a `ConcurrentMap<String, ScanTracker> scanTrackers` keyed by `scanId`. New accessor `trackerFor(scanId)` lazy-creates. Defaults to a single global "default" tracker when no scanId is plumbed.
2. **`CachedFsConfig`** adds an optional `fs.cached.scan-id` Hadoop config key. `CachedFileSystem.open` reads it once per open() and binds the resulting tracker into the `CachingInputStream` constructor.
3. **`CachingInputStream`** new fields:
   - `ScanTracker tracker` (non-null; falls back to "default")
   - `TrackingId trackingId` (always `(0, 0)` for now since we have no column metadata)
   - `IoStatistics ioStats` (per-stream; owned by the stream's lifetime)
4. On each `readFullyFromCache` call:
   - Before the loop: `tracker.recordReference(trackingId, length)` and `ioStats.read.addAndGet(length)`.
   - Per `copyChunk`: if `cache.exists(key)` was true (Hit path), `ioStats.ramHit.addAndGet(copyLen)`. Otherwise on a fill, no ramHit counter bump.
   - On every read of consumed bytes: `tracker.recordRead(trackingId, copyLen)`.
5. **`CachingInputStream.ioStatistics()`** accessor returns the per-stream `IoStatistics`. Wrap as `FSDataInputStreamWithStats` later (out of scope here).

**Acceptance:**
- New unit test: open a `CachedFileSystem`, read 8 MiB across multiple loadQuantum chunks, assert `ioStats.read == 8MiB`, `ioStats.ramHit` increases on warm re-read, `tracker.data(id).referencedBytes == 8MiB`, `readPct == 100`.
- README config table picks up `fs.cached.scan-id`.

**Risk:** low. No IO behavior change. The wiring touches one constructor and one read path.

### Phase 5b — Multi-chunk coalescing (medium)

When a single `read(long, byte[], int, int)` crosses multiple chunks AND those chunks miss, use `CoalescedLoad` to fill them with one `preadv` instead of one per chunk.

**Changes:**

1. Rewrite `readFullyFromCache` to:
   1. Walk the chunk range and split into (Hit, Miss) entries via `cache.findOrCreate(key, size, contiguous)`. Hit pins are kept open in a list; Miss yields Exclusive pins.
   2. Group consecutive Miss chunks via `CoalesceIo` (gap threshold ≤ `loadQuantum`; storage planning default 512 KiB, matches velox).
   3. For each coalesced group: build a `CoalescedLoad` over the exclusive pins, invoke `handle.readFile().preadv(...)` once with all pins' `dataRanges()` concatenated.
   4. Promote each exclusive to shared via `exclusiveToShared(true)`.
   5. Copy bytes out of each pin into the destination buffer.
   6. Close all pins.
3. On any failure during the coalesced fill, release every exclusive pin via `releaseFailedExclusive` to wake waiters; rethrow.

**Acceptance:**
- Existing CachingInputStream tests pass unchanged (semantics preserved).
- New test asserts a 3-chunk read on a cold cache triggers exactly one `ReadFile.preadv` call (verified via spy on `HadoopReadFile`).
- `IoStatistics.rawOverreadBytes` increments by the coalescing gap when chunks are adjacent (zero overread) and stays zero when they are.

**Risk:** medium. Multiple exclusive pins held simultaneously means exception paths must release all of them. The existing `CoalescedLoad` already handles this for the core path — verify it's usable from the Hadoop layer without leaking pins on partial failure.

### Phase 5c — Async prefetch (large)

Add an executor-driven prefetch path so the next `loadQuantum`-aligned chunk loads while the consumer is still processing the current one.

**Changes:**

1. **`CacheBootstrap`** holds an `ExecutorService prefetchExecutor` (bounded; default `Runtime.getRuntime().availableProcessors()`). Lifecycle: created in `installIfNeeded`, shutdown in `close`.
2. **`CachingInputStream`** maintains `Optional<CompletableFuture<CachePin>> pendingPrefetch` (a future that resolves to a Shared pin on the next chunk).
3. After each `read()` consuming bytes from chunk N:
   - If `pendingPrefetch` is empty AND `position` is within the last `loadQuantum / 4` of chunk N AND `tracker.data(id).readDensity >= 0.5`: submit `prefetch(chunk N+1)` to the executor. `prefetch` does `findOrCreate(...)` on the next chunk key; if Exclusive, fills via `preadv` and promotes to Shared; if Hit, returns the pin directly; if Waiting, awaits then retries.
4. On the next `read()` that crosses into chunk N+1: if `pendingPrefetch` is set, await it instead of doing a fresh `findOrCreate`. `IoStatistics.prefetch` increments by the prefetched bytes.
5. Prefetch admission gate matches velox `shouldPreload`:
   - Allocator has free space: always admit.
   - Otherwise: cap pending prefetches at half the cache pages. Requires a `cache.pendingPrefetchBytes()` counter (Phase 5c-prereq: add it to `AsyncDataCache`).

**Acceptance:**
- Sequential read of a large file: `IoStatistics.prefetch / IoStatistics.read >= 0.5` on the steady state.
- `cacheWaitLatencyUs` decreases significantly vs Phase 5a baseline.
- Random read pattern: `prefetch` stays close to zero (tracker density gates suppress it).
- Close releases any pending prefetch pin to avoid leak.

**Risk:** high. Async pins crossing thread boundaries, executor lifecycle on bootstrap close, prefetch admission gate must not deadlock the allocator. Worth a separate santa-method convergence pass.

## Out of scope (for now)

- **Per-column tracking** (`TrackingId` for individual streams). The Hadoop layer doesn't see columns; the consumer (Parquet/ORC reader) does. Per-column tracking belongs in the consumer or in a future "per-stream" reader-side API.
- **DirectBufferedInput** (velox §5.3): "no-cache path" already exists via `fs.cached.enabled=false` toggle.
- **Adaptive next-quantum prefetch** (velox §5.2.7): dormant in OSS velox; leave dormant.

## Test plan (cumulative)

- Phase 5a: 2 unit tests (basic wiring + warm re-read counters).
- Phase 5b: 1 integration test asserting a single `preadv` call for a 3-chunk read, plus 2 unit tests on coalescing gap behavior.
- Phase 5c: 1 integration test on sequential prefetch + 1 on random-read suppression + 1 on close-during-prefetch (pin release).

Total estimated work (assuming roughly the same per-phase santa-method bar as TTL): 5a ≈ 2-3 commits, 5b ≈ 4-6 commits, 5c ≈ 6-10 commits.

## Open questions for review

1. **scanId source** — is `fs.cached.scan-id` a Hadoop config key the right surface, or should `CachedFileSystem` look at a `TaskContext`-equivalent thread-local? Today the config key is the simplest.
2. **Default `loadQuantum` for coalescing gap** — velox uses 512 KiB for storage. For the Hadoop port, should the gap be `loadQuantum` (8 MiB default in cached-fs) or a separately-configurable `fs.cached.coalesce-distance-bytes`?
3. **Executor for prefetch** — share the bootstrap's executor or thread the executor per-FileSystem? Sharing simplifies lifecycle but creates cross-FileSystem coupling.
4. **IoStatistics exposure** — wrap `FSDataInputStream` in a subclass that exposes `ioStatistics()`, or attach as a `getStatistics()`-style call on `CachedFileSystem`?

## Recommendation

Start with **Phase 5a** as its own santa-method convergence. It's the cheapest, unblocks observability, and surfaces the open questions above without committing to async pins or coalesced fills. Phases 5b/5c can be decided independently after 5a lands.
