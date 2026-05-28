# M2 design — per-`(scanId, fileNum)` tracker keying

> **Status:** 🟢 Implemented 2026-05-28 (same day). M2 + R3 landed together per reviewer verdict; M2.1 (Spark-side scanId production) carved out as a separate roadmap row. See `docs/ROADMAP.md` for live status.

## Why this is on the roadmap

`ScanTracker.data` currently keys on `TrackingId`, which packs `(fileNumNode29bits, streamKind2bits)` into a 32-bit int:

```java
// CachedFileSystem.open() (phase-5a wiring, today)
long h = fileNum ^ (fileNum >>> 32);
int fileNumNode = (int)(h & ((1L << 29) - 1));   // 29-bit space → ~536M buckets
TrackingId trackingId = TrackingId.of(fileNumNode, 0);
```

The 29-bit hash space implies birthday-paradox collisions around **√(2 × 2²⁹) ≈ 33,000 unique fileNums** in a single scan. Once two files collide they share the same `TrackingData` entry, which means:

1. **Density contamination.** `readPct()` and `adjustedReadPct()` see the union of two file-access patterns. A file read densely + a file read sparsely collapse into a misleading composite density that mis-drives prefetch admission.
2. **`recordReference` cross-talk.** A reference recorded for fileA bumps the shared counter, then fileB's density predicate sees an inflated `referencedBytes`.

Partitioned-table scans across a Spark cluster routinely touch 50k+ unique files per stage. The contamination is real — and shows up as wasted prefetch + lower hit rate on workloads that the static density model otherwise handles fine.

## Acceptance criteria

The change is **done** when:

1. `ScanTracker.data` is keyed by raw `long fileNum` (not a hashed `TrackingId`). All existing unit tests for `ScanTracker` pass unchanged after the key swap.
2. `CachedFileSystem.open()` passes the raw `handle.fileNum()` (a `long`) to `CachingInputStream` instead of constructing a `TrackingId`. `TrackingId` is either deleted (preferred) or marked `@Deprecated` with a removal target.
3. A regression test exercises the collision case: two files with `fileNum` values that hash to the SAME 29-bit `fileNumNode` are independently tracked after this change. Before the change they share a `TrackingData`; after, they don't.
4. The bootstrap-level gauge `scanTrackerMaxEntries()` is documented to grow proportional to the **distinct fileNums** the scan touched, not the distinct hash buckets. Memory cost: ~40 bytes per entry × distinct fileNums × distinct scanIds.
5. No public-API regression on the package-private `recordReference` / `recordRead` surface — they take `long fileNum` directly.

The Spark-side scanId production is **out of scope** for M2. Today every Spark task collapses to the `"default"` scanId (no `fs.cached.scan-id` is set per-task). A separate follow-up will plumb stage/partition-derived scanIds through the Spark extension; until then, the storage-side fix is still valuable because it eliminates intra-scan file-vs-file contamination, which is the dominant pain point.

## Design

### Storage change

Replace:

```java
// cached-fs-core ScanTracker
private final ConcurrentMap<TrackingId, AtomicReference<TrackingData>> data = ...;

public void recordReference(TrackingId id, long bytes) { ... }
public void recordRead(TrackingId id, long bytes) { ... }
public TrackingData data(TrackingId id) { ... }
```

with:

```java
private final ConcurrentMap<Long, AtomicReference<TrackingData>> data = ...;

public void recordReference(long fileNum, long bytes) { ... }
public void recordRead(long fileNum, long bytes) { ... }
public TrackingData data(long fileNum) { ... }
```

`Long.hashCode` is identity for the lower 32 bits XOR upper 32 bits — same as the existing `fileNumNode` derivation, except `HashMap` uses the full 64-bit value as the equality key. **No collision on equality**; the HashMap bucket may still collide (it always does at some load factor), but the chain walk finds the correct entry via `Long.equals`.

`TrackingId` becomes dead code. Three call sites today; all collapse to passing `long fileNum` directly. Plan: delete the class in the same commit.

### Reader change

`CachingInputStream` holds a `tracker` and a `trackingId` field. After the change:

```java
// before
private final TrackingId trackingId;
tracker.recordReference(trackingId, len);

// after
private final long fileNum;
tracker.recordReference(fileNum, len);
```

`CachedFileSystem.open()` passes `handle.fileNum()` directly:

```java
// before
TrackingId trackingId = TrackingId.of(fileNumNode(handle.fileNum()), 0);
new CachingInputStream(..., tracker, trackingId, ioStats);

// after
new CachingInputStream(..., tracker, handle.fileNum(), ioStats);
```

`fileNumNode()` helper is deleted.

### Test changes

- `TrackingIdTest` deleted.
- `ScanTrackerTest` rewritten to use `long fileNum` directly — the existing assertions about per-id densities hold unchanged.
- New `ScanTrackerCollisionRegressionTest` constructs two `fileNum` values whose `fileNumNode` hashes collide (trivially: any two longs whose low 29 bits XOR'd with `(x >>> 32) & ((1L << 29) - 1)` produce the same value) and asserts their `TrackingData` entries are independent. The test acts as documentation that the prior keying lost this information.
- `CachingInputStreamTest` updated to pass `long fileNum` instead of constructing a `TrackingId`.
- All cached-fs-hadoop ITs unchanged (they exercise the read path through the cache; the keying is invisible at that layer).

### Migration / compatibility

No public API removed. The `TrackingId` class is `public` but the constructor and `of(int, int)` factory are entry points only the reader-glue layer uses internally. We can either:

- (a) Delete `TrackingId` in this commit and rely on `@RemovedInVersion`-style release notes.
- (b) Keep `TrackingId` as a `@Deprecated` shell that wraps a `long fileNum`, with the constructor and helpers no-op'd. Rip it out in the next minor.

Recommended: **(a) delete**. The class wasn't on any external contract; the package-private `ScanTracker` methods change signature in the same commit.

## Risks

1. **Memory growth.** The current scan that fits 50k unique files into 33k hash buckets (~60% collision rate) will, after this change, expand to 50k entries. At 40 bytes per `MutableData` + `Long` key overhead, that's ~3 MB per scan. For 1000 concurrent scans of 50k files each, ~3 GB of `ScanTracker` state. This is the same condition that triggers reader-glue follow-up #3 (cap per-`ScanTracker` inner map at 10k entries) — which is now strictly more urgent. The cap should land in the same release as M2; not in the same commit, but tracked together. Roadmap entry **R3** flips from ⚪ Deferred to 🔵 Planned as part of this change.

2. **`Long` key boxing.** `ConcurrentHashMap.computeIfAbsent` boxes the `Long`. At per-chunk fetch rates (every 64 KB read on a sequential scan = ~1000 ops/MB), this adds GC pressure. Mitigations:
   - Use Eclipse Collections' `LongObjectHashMap` or a hand-rolled long-keyed concurrent map.
   - Reuse the boxed `Long` per stream — the reader sees one `fileNum` per stream lifetime.

   Recommendation: use the second mitigation; the reader already has `long fileNum` as an instance field. One `Long.valueOf(fileNum)` per stream open. Probably negligible compared to the rest of the stream-open cost.

3. **TTL controller's tracker iteration.** `CacheTTLController` walks `bootstrap.scanTrackers` periodically. Each tracker's inner `data` map is now potentially larger (no more 60% hash-bucket compression). The walk cost goes from O(buckets) to O(distinct files). For 50k-file scans this is ~50% slower iteration. Not a correctness issue; logged for the perf-watchers.

## Plan

Single commit, sized at 6 bullets per the followups.md estimate methodology (≈ 2-4 review-rounds expected):

1. `TrackingId` deleted; `ScanTracker` data map retyped to `ConcurrentMap<Long, AtomicReference<TrackingData>>`.
2. `ScanTracker.recordReference` / `recordRead` / `data` signatures take `long fileNum`.
3. `CachedFileSystem.open()` drops `fileNumNode` helper; passes raw `fileNum`.
4. `CachingInputStream` field swap (`TrackingId trackingId` → `long fileNum`).
5. `ScanTrackerTest` rewritten + new `ScanTrackerCollisionRegressionTest`.
6. README config-reference table: bump `fs.cached.scan-id` description note (memory cost is now proportional to distinct fileNums).

Roadmap update in same commit: M2 ⚪→🟡, R3 ⚪→🔵 with note "now coupled to M2 — fileNum keying eliminates the 60% hash-compression that masked the cap requirement."

## Open questions for reviewer

1. Is `(a) delete` the right migration choice for `TrackingId`, or should we soft-deprecate?
2. Should the inner cap from R3 land in the same commit, or as a follow-up? Argument for same commit: the memory growth is induced by M2. Argument for separate: bigger diff is harder to review.
3. Spark-side scanId production: should that be a follow-up M2.1 in the roadmap, or stay in `docs/reader-glue/followups.md` as a deferred item?
