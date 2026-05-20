# Velox File Read Cache — Implementation Mechanism

A deep-dive into how `facebook::velox::cache` implements a two-tier (RAM + local SSD) file read cache, including the reader-side glue that turns a column scan into coalesced, prefetched, deduplicated IO.

All file paths are relative to `velox/` in the velox repo.

---

## 0. Glossary

A few terms recur throughout. Reading them here first will make the rest of the doc less ambiguous.

| Term | Meaning |
| --- | --- |
| **Split** | A piece of work assigned to a worker — typically one file (or a byte range of one file) coming out of a connector |
| **Stripe** | A row group in DWRF/ORC; a Parquet "row group" plays the same role |
| **Stream** | A per-column byte stream within a stripe (data, nulls, lengths, dictionary, etc.) |
| **Region (cache)** | The byte range a single cache entry covers — chosen by the reader, capped by `loadQuantum`. Distinct from "Region (SSD)" below: one SSD region holds many cache regions |
| **Region (SSD)** | A 64 MiB slab inside an `SsdFile` that holds many cache entries packed end-to-end |
| **`loadQuantum`** | The maximum size of a single cache entry. Large columns are chopped into `loadQuantum`-sized pieces so partial reads can hit the cache. Default 8 MiB |
| **`fileNum`** | A `uint64` id minted by `StringIdMap` for a file path; used as cache key prefix |
| **`groupId`** | A `uint64` id for a directory / Hive partition; same `StringIdMap` |
| **`scanId`** | A string `{taskId}.{planNodeId}` minted per TableScan operator instance (`Connector.h:448`); all splits of one scan share one `ScanTracker` |
| **`TrackingId`** | A `(node, streamKind)` pair packed into an `int32_t` (node in high 27 bits, stream kind in low 5); the sentinel `id_ == -1` indicates "empty" — per-stream identifier within a scan |
| **`CachePin` / `SsdPin`** | RAII (Resource Acquisition Is Initialization) handles that prevent eviction while a reader holds them |
| **`evictAllUnpinned`** | Per-call boolean; when `true`, every unpinned entry is evictable regardless of score. Set by `makeSpace` after repeated failures, by `shrink` always, and by `clear()` always |
| **`ssdSaveable_`** (per-entry) | Per-entry `bool` flag (`AsyncDataCache.h:388`) marking the entry as eligible for SSD save. Set by `setExclusiveToShared` only when `ssdFile_ == nullptr` (i.e., not already on SSD); the `ssdFile_ != nullptr` branch suppresses the set rather than clearing an existing flag. (See §3.6.) Code uses two spellings: `ssdSaveable_` (with `e`) is the field; `ssdSavable` (no `e`) is the boolean argument |
| **`ssdSaveable_`** (cache-wide) | A different, unrelated field with the same identifier: `AsyncDataCache::ssdSaveable_` (`AsyncDataCache.h:1049`) is a `tsan_atomic<uint64_t>` byte counter accumulating "saveable bytes since last flush". `possibleSsdSave` (§3.8) compares this counter against the flush thresholds |
| **`folly::SharedPromise`** | Single-producer, multi-consumer promise; many waiters can `wait()` on its future and all wake on `setValue`. Used to broadcast "exclusive entry filled" to threads queued behind a `kExclusive` placeholder |
| **`memory::Allocation`** | A discontiguous run of pages returned by `MemoryAllocator`; the storage backing for non-contiguous cache entries |
| **DWRF** | Facebook's evolved variant of ORC (Optimized Row Columnar) and the default columnar format Velox reads |
| **DWIO** | Velox namespace for the columnar reader subsystem (DWRF, Parquet, etc.) |
| **`AsyncSource<T>`** | Velox's lazy-future primitive. Kicks off background work; first reader blocks until ready |
| **MemoryAllocator** | Velox's process-wide page allocator. The cache competes with operator buffers under one accounting system; "memory arbitration" is what asks the cache to release pages under pressure |
| **`folly::Executor`** | Lambda scheduler from Folly; the cache uses one for prefetch IO and a separate one for SSD writes |
| **`preadv` / `pwritev`** | POSIX vectored read/write syscalls — one syscall transfers between an offset and multiple buffers |

The cache is built around **integer keys**, not strings: `RawFileCacheKey{uint64 fileNum, uint64 offset}` (`AsyncDataCache.h:115-122`) is 16 bytes regardless of path length.

---

## 1. High-Level Architecture

```text
   Column reader (DWRF / Parquet)
       └── CacheInputStream                  per-stream byte cursor
                ▲
                │ Next()
                │
   ┌────────────┴───────────────────────────────────────────────┐
   │ CachedBufferedInput  (per file split)                      │
   │   - enqueue(region) → CacheRequest                         │
   │   - load() → coalesce → DwioCoalescedLoad / SsdLoad        │
   │   - prefetch via connector ioExecutor                      │
   └────────────────────────────┬───────────────────────────────┘
                                │ findOrCreate / find / makePins
                                ▼
   ┌────────────────────────────────────────────────────────────┐
   │ AsyncDataCache  (RAM tier; sharded; clock + sampled-pct)   │
   │   CacheShard × N  (key→entry map + clock sweep)            │
   │   CachePin (RAII) — exclusive (filling) | shared (reader)  │
   └─────────────┬──────────────────────────────┬───────────────┘
                 │ miss                         │ savable
                 ▼                              ▼
   ┌──────────────────────────┐    ┌──────────────────────────────┐
   │  ReadFile via FileHandle │    │  SsdCache                    │
   │  (S3 / HDFS / local      │    │   SsdFile shards × M         │
   │   POSIX filesystems)     │    │   regions × 64 MiB           │
   └──────────────────────────┘    │   checkpoint + evict-log     │
                                   │   writes via separate        │
                                   │   SsdCache::Config::executor │
                                   └──────────────────────────────┘

   Cross-cutting plumbing:
     * StringIdMap converts file paths → uint64 fileNums (cache keys)
     * ScanTracker tracks per-stream readPct to drive prefetch decisions
```

Three independent caches collaborate (sections §2-§4 below cover them in order; §5 covers the reader-side glue):

| Cache | Caches | Keyed by | Eviction policy |
| --- | --- | --- | --- |
| `FileHandleFactory` (`SimpleLRUCache`) | open `ReadFile` + `StringIdLease` | path + auth-token provider | LRU, count-based (one slot per handle) |
| `AsyncDataCache` | byte ranges in memory | `(fileNum, offset)` | clock sweep + sampled-percentile threshold |
| `SsdCache` (`SsdFile` shards) | byte ranges on local SSD | `(fileNum, offset)` | per-region score with geometric decay |

> **Important**: the two tiers shard *independently* (§3.3 vs §4.1) and have *independent* `numShards` settings. Their shard maps do **not** align — an entry's RAM shard is `hash(fileNum, offset) & ramMask`, whereas its SSD shard is `fileNum % numSsdShards`.

---

## 2. Identity Layer — `StringIdMap` / `FileHandle`

### 2.1 StringIdMap (`StringIdMap.h:27-105`)

A process-wide bidirectional map from string (file path) to dense `uint64` id with **per-id reference counts** held in `Entry::numInUse` (`StringIdMap.h:97`). The `fileIds()` singleton (`FileIds.h:22`) backs the entire cache.

`StringIdLease` (`StringIdMap.h:108-179`) is RAII over a reference: ctor calls `makeId`/`addReference`, dtor calls `release`. As long as any cache entry, SSD entry, or in-flight load holds a lease, the path↔id mapping is stable. `recoverId` (`StringIdMap.h:62`) is critical for SSD recovery — when reading the SSD checkpoint after a restart, we replay the original id assignments so already-on-disk shards remain reachable.

`lastId_` is a monotonically-incrementing counter starting at 0 (`StringIdMap.cpp:57-84`); ids are allocated densely from the bottom of the `uint64` range. This is what makes the SSD checkpoint sentinels (§4.6) safe.

Memory savings are real: `RawFileCacheKey` is 16 bytes regardless of path length, and one `Entry` exists per unique file even if thousands of cache pages reference it.

### 2.2 FileHandle (`FileHandle.h:38-50`, `FileHandle.cpp:42-74`)

Bundles `(shared_ptr<ReadFile>, StringIdLease uuid, StringIdLease groupId)`. The `uuid` lease's `id()` is what later sections call `fileNum`; `groupId` is the lease for the directory / Hive partition.

**`groupId` consumers in OSS velox.** `groupId` is stored on every cache entry (`AsyncDataCacheEntry::groupId_`, `AsyncDataCache.h:372`) and passed to `FileGroupStats`, but in OSS velox `FileGroupStats` is a **stub**: `recordReference`/`recordRead` are empty, and `shouldSaveToSsd` always returns `true` (`FileGroupStats.h:24-61`). The hooks exist for embedders (e.g. Prestissimo) to plug in group-level admission decisions without forking the cache. The dormant trigger that *would* light it up is `AsyncDataCache::incrementNew` (`AsyncDataCache.cpp:1059-1071`): every `max(pageBytes(cachedPages_), 256 MiB)` worth of new misses (≈ "every cache-worth of misses, floored at 256 MiB"), it calls `groupStats().updateSsdFilter(maxBytes * 0.9)` — passing the 90% admission threshold to the (stub) filter. Treat `groupId` as plumbing in OSS.

`FileHandleGenerator::operator()` (`FileHandle.cpp:42-74`) calls `filesystems::getFileSystem(filename, properties_)->openFileForRead(...)`. The handle deduplicates `open()` syscalls *and* mints the integer ids the data caches use as keys.

### 2.3 FileHandleFactory + SimpleLRUCache

`FileHandleFactory` is a `CachedFactory<FileHandleKey, FileHandle, FileHandleGenerator>` (`FileHandle.h:111-117`) backed by `SimpleLRUCache<FileHandleKey, FileHandle>` (`FileHandle.h:93-94`). **This is a separate cache from the data cache** — it caches the open file (descriptor / TLS session), not file bytes. `FileHandleSizer` returns 1 (`FileHandle.cpp:27-31`); the cache is sized in number-of-files via `numCacheFileHandles()`.

**Why the key includes a token provider.** `FileHandleKey{filename, tokenProvider}` (`FileHandle.h:57-76`) participates in equality via the token-provider object so the same path under two different identities (e.g. different STS tokens) gets two different handles. The underlying filesystem session carries credentials, so sharing a handle across identities would be a confidentiality bug — the token provider is in the key to prevent that.

`CachedFactory::generate` (`CachedFactory.h:376-439`) implements **single-flight generation** (request collapsing — only one thread does the open; concurrent askers wait for the result): the first thread inserts into a `pending_` set under `pendingMu_`; concurrent askers `wait` on `pendingCv_` and pick up the freshly inserted entry on wake.

`SimpleLRUCache` is **not internally thread-safe** (`SimpleLRUCache.h:116`); `CachedFactory` wraps every operation with `cacheMu_`. It uses `folly::IntrusiveList` for LRU order and refcounts pinned entries — only `numPins == 0` entries can be evicted. The class also exposes a time-based `expireDurationMs`, but the Hive connector currently constructs the cache without it (`HiveConnector.cpp:39-52`); time-based expiry is supported by the data structure but not wired up.

---

## 3. RAM Tier — `AsyncDataCache`

`AsyncDataCache` is normally accessed via the pointer injected through `ConnectorQueryCtx`, but `AsyncDataCache::getInstance()` (`AsyncDataCache.cpp:840-851`) returns the same canonical instance for code paths without a connector context (telemetry, cleanup hooks). The embedder calls `setInstance(...)` once at process init.

### 3.1 Cache Entry & Pin

`AsyncDataCacheEntry` (`AsyncDataCache.h:156-393`) owns one cached byte range. Three storage paths chosen by size and the caller's `contiguous` flag:

| Trigger | Field | Notes |
| --- | --- | --- |
| `size < kTinyDataSize (2048)` | `tinyData_` (`std::string`) | Inline string; skips the page allocator path entirely |
| `size >= 2048 AND contiguous=true` | `contiguousData_` (`void*`) | Single contiguous range from `allocateBytes`; used when the connector wants one flat buffer |
| `size >= 2048 AND contiguous=false` | `nonContiguousData_` (`memory::Allocation`) | Page runs from the velox `MemoryAllocator`; default for typical column reads |

The entry distinguishes two sizes: `size_` is the requested bytes (`AsyncDataCache.h:354`); `dataCapacity()` (`AsyncDataCache.cpp:201-204`) is the actual allocated bytes — `tinyData_.capacity()` for tiny, `size_` for contiguous (no padding), and `nonContiguousData_.byteSize()` for non-contiguous (which includes page-rounding padding). The non-contiguous path is the only one where `dataCapacity()` typically exceeds `size_`. `CacheStats` separates `tinySize/largeSize` from `tinyPadding/largePadding` for that reason. The stale-entry self-heal in §3.4 pivots on `size_`: a cached entry whose `size_ < requested` is rebuilt; a *larger* `size_` is reused.

**Memory accounting and the tinyData carve-out.** `cachedPages_` (returned by `AsyncDataCache::cachedPages()`) is the *page-allocator-backed* portion of the cache; it is a subset of `allocator_->numAllocated()` and is not double-counted (cache pages live inside the allocator's allocation, not alongside it). However, **tiny entries (< 2 KiB) are stored in `tinyData_` as a `std::string` heap allocation outside the page allocator** and are **not** included in `cachedPages_`. Operators doing capacity math from `cachedPages()` will undercount cache memory by the total `tinySize` + `tinyPadding`; the half-of-cache prefetch admission gate in §3.4 / §5.2.6 also computes against `cachedPages` and so excludes tinyData by construction.

State is encoded in a single `std::atomic<int32_t> numPins_`:

- `kExclusive = -10000` — being filled by one thread; everyone else waits via `promise_`
- `0` — evictable
- `> 0` — number of shared readers (hits)

`CachePin` (`AsyncDataCache.h:395-471`) is RAII over `numPins_`. It is the only public way to touch entry data, so the pin lifetime *is* the protection against eviction.

**Writer-side surface — `dataRanges`.** `AsyncDataCacheEntry::dataRanges(length)` (`AsyncDataCache.h:215`) returns a vector of writable buffer ranges that cover the first `length` bytes. This is what writers use to pour bytes into the entry regardless of which storage path it took. Tiny entries return one range, contiguous entries return one range, non-contiguous entries return one range per page run. `SsdFile::load`, `DwioCoalescedLoad::loadData`, and the storage `preadv` lambda all consume `dataRanges` to fill new entries.

**Side-door insertion: `cacheRegion` and `preload`.**

- `CachedBufferedInput::cacheRegion(offset, length, IOBuf)` (`CachedBufferedInput.cpp:680-725`) — used when bytes arrived through a different channel (e.g. a metadata pre-read). Allocates an exclusive entry, copies the bytes via `entry->contiguousData()` / `entry->nonContiguousData().runAt(i)` with a `folly::io::Cursor`, then transitions to shared.
- `CachedBufferedInput::preload()` (`CachedBufferedInput.cpp:176-220`) — for tiny files, inserts a single cache entry covering the *entire file* keyed at offset 0 (bypassing `loadQuantum` chunking). Subsequent `enqueue` calls take a `setPreloadedPin` shortcut and skip the load/coalesce path. Gated externally by file-size threshold (`ReaderBase` calls `preload()` only when `fileLength_ <= options_.filePreloadThreshold()`); `shouldPreload` is a *different* gate that applies to `prefetch(Region)` (`CachedBufferedInput.cpp:658-664`), not to `preload()`.

### 3.2 Score / Eviction Stats

`AccessStats` (`AsyncDataCache.h:72-102`) holds `lastUse` (≈0.5–2 ms resolution from `folly::hardware_timestamp() >> 21`, per the source comment) and `numUses`. The retention score is:

```text
score(now, /*size unused*/) = (now - lastUse) / (1 + numUses)
```

(The `size` parameter is part of the signature but currently ignored.) Higher = less worth keeping. `lastUse == 0` returns `INT_MAX` — the explicit "evictable now" sentinel set by `makeEvictable()`. A prefetched-but-never-used entry has a *high but finite* score `(now − lastUse) / 1` that grows with age — distinct from the `INT_MAX` sentinel.

### 3.3 Sharding

`AsyncDataCache` owns `shards_[numShards_]` (default 4, must be a power of 2 — power-of-2 enables `& shardMask_` instead of modulo, and 4 balances mutex contention against per-shard housekeeping at typical core counts).

The shard index for a key is `std::hash<RawFileCacheKey>()(key) & shardMask_` (`AsyncDataCache.cpp:874`) — a hash of the *full* `(fileNum, offset)` pair, not just `fileNum`. Pages from one file therefore fan out across all RAM shards. **This is different from the SSD tier**, which routes by `fileNum % numSsdShards` (§4.1) and so puts all of one file's pages in a single SSD shard.

### 3.4 `findOrCreate` — the cache hit/miss path

The combined logic of `CacheShard::findOrCreate` (`AsyncDataCache.cpp:293-326`) and `lookupLocked` (`AsyncDataCache.cpp:249-291`) is:

```text
findOrCreate(key, size):
  shard = shards_[hash(key) & shardMask_]
  with shard.mutex:
    # --- lookupLocked ---
    if found = entryMap_.find(key):
      if exclusive(found):
        return empty pin + future on found.promise_
      if found.size < requested size:
        # stale (different connector used different load quantum)
        ++numStales_; erase; lookupLocked returns nullopt
      else:
        if found.isPrefetch():
          isFirstUse_ = true; clear prefetch flag    # NOT counted as hit yet
        else:
          ++numHit_; hitBytes_ += found.size()       # actual reuse; uses cached entry size
        touch(); ++numPins_  → return shared pin
    # --- miss path ---
    entry = getFreeEntryLocked() or new
    entry.numPins_ = kExclusive
    entryMap_[key] = entry
    ++numNew_
  # outside mutex (initEntry):
  entry.initialize(key)                # allocate Allocation/contiguous/tiny
  cache_->incrementNew(entry.size())   # AsyncDataCache.cpp:377
  return exclusive pin                 # caller fills it, then setExclusiveToShared
```

Key invariants:

- **Insertion under mutex, allocation outside.** The `kExclusive` placeholder is published in `entryMap_` while the (possibly slow) `MemoryAllocator` work runs without blocking the shard.
- **Stale-entry self-heal.** Entries are keyed on offset alone with no length component; if a different reader had cached the same offset with a smaller `loadQuantum`, the shorter entry is erased and `lookupLocked` returns `nullopt` so `findOrCreate` falls through to the miss path. Counted as `numStales_` (`AsyncDataCache.cpp:267-278`). A *smaller* request can reuse a *larger* cached entry without resizing.
- **Shared promise for collisions.** A second thread that finds an `kExclusive` entry receives the entry's `SharedPromise` future and is woken when the first thread calls `setExclusiveToShared` after filling.
- **Prefetch promotion.** A prefetched entry's first reuse does *not* count as a hit (`isPrefetch_` is cleared and `isFirstUse_` set instead). Only the second and subsequent reads bump `numHit_`. This lets prefetch effectiveness be measured separately from cache effectiveness.
- **Live prefetch counter (`prefetchPages_`).** `prefetchPages_` (`AsyncDataCache.h:1039`) is a live counter incremented/decremented by `setPrefetch`. It is the underlying state read by `CachedBufferedInput::shouldPreload` — the cache-wide gate on speculative IO that binds *only under memory pressure*. The full gate logic is in §5.2.6.
- **Failure unwind for exclusive entries.** If the producer's `loadData` throws (storage IO failure, S3 timeout, decompression error, OOM during `initialize`), the `CachePin` for the exclusive entry is destroyed without ever calling `setExclusiveToShared`. `AsyncDataCacheEntry::release()` (`AsyncDataCache.cpp:112-127`) detects `numPins_ == kExclusive` and calls `shard_->removeEntry(this)` which atomically erases from `entryMap_` and moves the `SharedPromise` so all queued waiters are woken via `setValue(true)`. Each woken waiter then re-looks up, misses, and retries the load. Without this path, IO failures would leak `kExclusive` placeholders and deadlock every concurrent reader on the dead `promise_`.

### 3.5 Eviction — clock + sampled threshold

`CacheShard::evict` (`AsyncDataCache.cpp:515-628`) is a **clock sweep with a sampled-percentile threshold**:

1. `clockHand_` walks `entries_` round-robin. (Per-shard `entries_` — distinct from the SSD `entries_` map in §4.3.)
2. **Calibration triggers.** Recalculate `evictionThreshold_` when (a) it's still `kNoThreshold`, OR (b) `eventCounter_ > entries_.size() / 4`, OR (c) `numChecked > entries_.size() / 8` for this call.
3. **Calibration procedure.** `calibrateThresholdLocked` (`AsyncDataCache.cpp:624-645`) samples up to 10 entries (`numSamples = min(kMaxEvictionSamples=10, entries_.size())`), spaced `entries_.size() / numSamples` apart starting at `clockHand_ % entries_.size()` — strided sequential, not random. The clock hand provides the rotation. Sets `evictionThreshold_` to the 80th percentile of the sample (`kEvictionPercentile = 80`), targeting the worst quintile per pass.
4. A candidate is evictable if `numPins_ == 0` AND (`!fileNum.hasValue()` OR `evictAllUnpinned` OR `score >= evictionThreshold_`).
5. **Saveable skip-window and post-sweep dispatch.** While `ssdCache_->writeInProgress()` is true, evict skips entries with `ssdSaveable_ == true` (unless `evictAllUnpinned` is set, in which case saveables are evicted alongside everything else) and counts them in `evictSaveableSkipped` (`AsyncDataCache.cpp:567-569`). At the end of the sweep, if any were skipped (`AsyncDataCache.cpp:600-611`), evict tries `ssdCache_->startWrite()`. Two mutually exclusive outcomes:
    - **Success:** resets `numSkippedSaves_` to 0 and calls `cache_->saveToSsd()` so the previously-skipped data is flushed and becomes evictable on the next pass.
    - **Failure:** increments `numSkippedSaves_` and proceeds.

    `numSkippedSaves_` is mutated only here; the alternative `possibleSsdSave` path (§3.8) does not touch it.
6. Evicted memory is funneled by `acquireEvictedData` (`AsyncDataCache.cpp:476-513`) — pages either go into the caller's `AcquiredMemory acquired` (handed back to the allocation that triggered eviction, no free/realloc roundtrip) or into `toFree` (released after the loop).

`makeSpace` (`AsyncDataCache.cpp:900-994`) ties this to memory arbitration. It is *not* serialized by a mutex (memory arbitration cannot tolerate one). Notable details that affect tail latency under pressure:

- **`canTryAllocate`** (`AsyncDataCache.cpp:1037-1048`) is the cheap pre-check — `requestBytes - acquired ≤ allocator->capacity() - allocator->numAllocated()` — that decides whether to even attempt `allocate()`.
- **Progressive eviction batch.** A `sizeMultiplier` starts at 1.2 and is doubled on each retry for sub-8 MiB requests as long as `sizeMultiplier < 4` *before* the doubling — so the actually-reached sequence is 1.2× → 2.4× → 4.8× → 4.8× (capped). Each failed attempt evicts a bigger batch than the last.
- **SSD-write back-pressure stall.** When `nthAttempt > 2` (from the 4th attempt onwards), if `ssdCache_->writeInProgress()` is true the loop **sleeps 500 ms** (`AsyncDataCache.cpp:955-959`) waiting for the SSD writer to release pinned bytes. This is the dominant tail-latency cliff under memory pressure.
- **Rank-based fairness.** `numThreadsInAllocate_` ranks concurrent allocators; later arrivals get a higher rank → larger `backoff(nthAttempt + rank)` so first-comers are favored.
- **Final emergency pass.** After several failures, the loop switches to `evictAllUnpinned=true` and gives every unpinned entry as a candidate; final `kNoCacheSpace` if even that fails.

`shrink(targetBytes)` (`AsyncDataCache.cpp:997-1035`) is the **opposite policy** — invoked by memory arbitration under host pressure, not to fund a specific allocation. It loops over shards, evicting up to `targetBytes` with `evictAllUnpinned=true` and `bytesToAcquire=0` (so freed memory is released, not handed back), and then calls `allocator_->unmap()` to return pages to the OS. Because `evictAllUnpinned=true` bypasses the saveable-skip check, shrink intentionally drops in-flight SSD candidates rather than preserve them — freeing memory beats preserving SSD admission under pressure.

### 3.6 `CoalescedLoad` — multi-entry IO

`CoalescedLoad` (`AsyncDataCache.h:477-539`, `AsyncDataCache.cpp:388-446`) groups several missing keys into a single IO. State machine: `kPlanned → kLoading → kLoaded | kCancelled`. `loadOrFuture(wait)` either returns the existing future (if another thread is loading), or transitions `kPlanned → kLoading` and calls the virtual `loadData(prefetch)`. After a successful `loadData`, every pin is moved exclusive→shared via `setExclusiveToShared(ssdSavable)`.

The destructor calls `setEndState(kCancelled)` so any thread waiting on a half-built load is unblocked even if the producer was destroyed mid-flight.

**`ssdSavable` argument and the SSD-source de-dup.** Both storage and SSD loads pass `ssdSavable = options_.cacheable()` — `CachedBufferedInput`'s `options_` member is `io::ReaderOptions` (defined in `velox/common/io/Options.h`), and the accessor defaults to `true`; embedders set it `false` only on no-cache code paths. The de-dup that prevents re-saving SSD-resident data lives inside `setExclusiveToShared` itself (`AsyncDataCache.cpp:99-109`):

```cpp
if (!ssdSavable) return;
if (ssdCache != nullptr && ssdFile_ == nullptr) {   // not already on SSD
  if (groupStats().shouldSaveToSsd(...)) {
    ssdSaveable_ = true;
    cache->possibleSsdSave(size_);
  }
}
```

`SsdFile::load` calls `setSsdFile(this, offset)` on each loaded pin *before* it returns, so by the time `setExclusiveToShared` runs the `ssdFile_ != nullptr` check fails and the entry is silently *not* marked saveable. No bitwise OR is involved; `ssdSaveable_` is a plain assignment. (How the flag is consumed is described in §3.8.)

### 3.7 Helper: `readPins` (`AsyncDataCache.h:1101-1111`)

Generic coalesced-IO helper used by both the storage path and the SSD path. **Caller must pre-sort pins by offset** before invoking `readPins` (`CachedBufferedInput.cpp:268-271` sorts both the `storageLoad` and `ssdLoad` request lists by their respective offsets — storage by `key.offset`, SSD by the SSD-physical offset — before they are fed to `readPins` through `makeLoads`). `readPins` itself only groups pins by gap (`maxGap`) and batch (`maxBatch`) and calls a user-supplied `readFunc(pins, begin, end, offset, buffers)`. Each caller passes a lambda that captures its own file handle (the connector input stream for storage, `readFile_` for SSD) and does only the syscall — gap/batch grouping is `readPins`' job. Returns a `CoalesceIoStats{int32_t numIos, int64_t payloadBytes, int64_t extraBytes}` (`CoalesceIo.h:27-35`) — `payloadBytes` is the requested data, `extraBytes` is the gap waste. Both `DwioCoalescedLoad::loadData` and `SsdLoad::loadData` (the wrapper around `SsdFile::load`) consume this triple to feed `IoStatistics::incRawOverreadBytes` (gap waste) and the per-load IO/byte counters surfaced in §5.4.

### 3.8 Save-to-SSD trigger flow

The "saveable" tag on an entry is one half of the story; the other half is *who* turns it into an actual SSD write. Two trigger sites:

- **`possibleSsdSave(bytes)`** (`AsyncDataCache.cpp:1073-1095`) is called from `setExclusiveToShared` whenever a new saveable entry is published. It accumulates into the **cache-wide** `AsyncDataCache::ssdSaveable_` byte counter (distinct from the per-entry flag of the same name — see glossary) and dispatches a save when **either** condition holds (the source converts both sides to pages before comparing; bytes shown here for readability):
  1. `ssdSaveable bytes > max(minSsdSavableBytes, ssdSavableRatio × cached bytes)`, **or**
  2. `ssdFlushThresholdBytes > 0 AND ssdSaveable bytes > ssdFlushThresholdBytes` (the secondary lower trigger).

  …**and** `ssdCache_->startWrite()` returns true. If `startWrite()` returns false (a previous batch is still in flight), `possibleSsdSave` simply returns — it does **not** increment `numSkippedSaves_`.
- **`CacheShard::evict`** (`AsyncDataCache.cpp:600-611`) calls `cache_->saveToSsd()` if it had to skip any saveable entries on this pass and bumps `numSkippedSaves_`. This accelerates the flush so the skipped entries become evictable on the next pass.

`saveToSsd(saveAll)` (`AsyncDataCache.cpp:1097-1105`):

```text
for each shard:
    shard->appendSsdSaveable(saveAll, pins)   # cap on cumulative pins, see below
ssdCache_->write(pins)                        # dispatched per SSD shard via SSD executor
```

`appendSsdSaveable` (`AsyncDataCache.cpp:695-…`) walks each shard's `entries_` and includes only pins whose **per-entry** `ssdSaveable_` flag is `true`. The per-shard cap is `entries_.size() * maxWriteRatio`, and the comparison is `if (pins.size() >= cap) break` evaluated *after* each push. Cumulative `pins` is shared across shards.

Worked example with `maxWriteRatio = 0.7`:

| Shard | `entries_.size()` | cap (= entries × 0.7) | cumulative `pins.size()` before | Pins added (saturating) |
| --- | --- | --- | --- | --- |
| 0 | 1000 | 700 | 0 | up to 700 (last push raises cumulative to 700, then break) |
| 1 | 800 | 560 | 700 | 1 (cap already exceeded; one push raises to 701, then break) |
| 2 | 1200 | 840 | 701 | up to 139 (last push raises cumulative to 840, then break) |

Shard-iteration order is `shards_[0..numShards_-1]`, deterministic. The intent is back-pressure that prevents an SSD save from pinning a majority of cache entries simultaneously; in practice early shards exhaust later shards' caps.

### 3.9 Lifecycle: `shutdown`, `clear`, `removeFileEntries`

| Method | Effect |
| --- | --- |
| `AsyncDataCache::shutdown()` (`AsyncDataCache.cpp:855-862`) | Calls `ssdCache_->shutdown()` (busy-waits on `writesInProgress_`, then forces a final `checkpoint(true)` on every SSD shard), then per-shard `shutdown()` clears `entries_`/`freeEntries_`. After this the cache is unusable. The only path that guarantees in-flight saves complete. |
| `AsyncDataCache::clear()` (`AsyncDataCache.cpp:1139-1145`) | One-liner: `evict(uint64_max, evictAllUnpinned=true, 0, _)` per shard. Drops all unpinned entries; pinned ones survive. Used by tests and by Prestissimo's manual cache-wipe operation. Distinct from `shrink` (which respects `targetBytes` and unmaps). |
| `removeFileEntries(filesToRemove, filesRetained)` (`AsyncDataCache.cpp:1107-1126`) | Per-fileNum drop; fans out to every `CacheShard::removeFileEntries` (RAM) **then** every `SsdFile::removeFileEntries` (SSD). Pinned entries are added to `filesRetained` for the caller to retry next cycle. The TTL controller (§4.11) is the primary caller. |

---

## 4. SSD Tier — `SsdCache` / `SsdFile`

### 4.1 SsdCache sharding

`SsdCache` owns `std::vector<std::unique_ptr<SsdFile>> files_` (`SsdCache.h:198`), one shard per file, count = `Config::numShards` (independent from RAM `numShards`). Routing: `files_[fileId % numShards_]` (`SsdCache.cpp:80-83`). Capacity is rounded up so every shard holds the same `fileMaxRegions`.

The shard count and `fileNum` are the *only* placement inputs — there is no rebalancing. Because routing is by `fileNum` only (not `(fileNum, offset)`), all of one file's pages live in a single SSD shard, which is what makes coalesced reads from one file efficient. The trade-off versus the RAM tier is: one large file can hot-spot one SSD shard, but the gain is that adjacent ranges of the same file are physically packed on that shard's SSD device (§4.9).

### 4.2 Write batching & backpressure

A single counter `writesInProgress_` (`SsdCache.h:201`, declared `std::atomic_int32_t`) gates concurrency. `startWrite()` (`SsdCache.cpp:85-95`) takes `mutex_`, checks for zero, then bumps the counter by `numShards_` — "writing" is an all-or-nothing per-batch state. The per-shard decrement at the end of each shard's executor task is a lock-free `--writesInProgress_` (`SsdCache.cpp:140`) and the no-store fast-path uses `fetch_sub(numNoStore)` (`SsdCache.cpp:151`); the atomic exists precisely to allow these decrements without retaking `mutex_`. A second batch cannot start until every shard from the previous one finishes. `shutdown()` busy-waits on this counter before forcing a final checkpoint (`SsdCache.cpp:220-222`).

`SsdCache::write` (`SsdCache.cpp:97-152`) splits pins by destination shard, wraps each shard's vector in a shared `PinHolder`, and dispatches each shard to `Config::executor` — **a separate executor from the connector's `ioExecutor`** that drives reader prefetches. This split matters: SSD writes can't queue behind read IO and vice-versa.

### 4.3 SsdFile on-disk layout

Each SSD shard is one regular file divided into 64 MiB regions: `kRegionSize = 1 << 26` (`SsdFile.h:320`). 64 MiB matches typical SSD erase-block multiples for write amplification. **Entries never span regions** (`SsdFile.h:259`); `getSpace` only allocates within the current writable region and rolls to the next when the entry won't fit (`SsdFile.cpp:256-291`). The file grows lazily one region at a time via `truncate` until `maxRegions_` is reached.

`writableRegions_` is a FIFO: `getSpace` always pops `writableRegions_[0]`; `growOrEvictLocked` `push_back`s newly grown or freshly-evicted regions. So one writable region is filled completely before another is touched, and combined with the §4.9 sort-by-`(fileNum, offset)` this is what guarantees that one SSD region holds a maximal run of one file's adjacent bytes — making §4.8's coalesced reads effective.

`entries_` is a `folly::F14FastMap<FileCacheKey, SsdRun>` (`SsdFile.h:643`). `SsdRun` packs `(offset, size)` into a single `uint64_t fileBits_`:

- low **23 bits**: `size - 1` — stored value range `0 .. (2²³−1)`, decoded `size` ranges 1 byte .. 8 MiB. Decoders must add 1.
- high **41 bits**: file offset.

The `−1` offset lets the inclusive max size be exactly `2²³ = 8 MiB` (matching the default `loadQuantum`) without losing a representable bit; zero-byte entries are never stored, so the encoding loses nothing.

A separate `uint32_t checksum_` (CRC32 — 32-bit Cyclic Redundancy Check) is populated only when `checksumEnabled_`.

### 4.4 Region pin bookkeeping

`regionPins_[regionIndex]` (`SsdFile.h:640`) is a per-region pin counter. `SsdFile::find` increments it under the write lock before constructing an `SsdPin`; the `SsdPin` destructor delegates to `clear()` (`SsdFile.cpp:80-85`) which calls `unpinRegion`. Eviction candidates with `regionPins[i] > 0` are skipped (`SsdFileTracker.cpp:60`; lines 48-58 also exclude pinned regions when computing the average score). **Invariant**: while any `SsdPin` exists for a region, the underlying bytes cannot be overwritten.

### 4.5 Region eviction & scoring

`SsdFileTracker` (`SsdFileTracker.cpp:22-71`) maintains a `double` score per region:

- Reads add bytes via `regionRead`.
- `fileTouched` multiplies *all* scores by `15/16` (i.e. each score is reduced *to* 93.75% of its prior value, a 6.25% reduction per pass) once `numTouches_` exceeds *both* `kDecayInterval = 1000` and `totalEntries / 2` (effective cadence ≈ `max(1000, totalEntries/2)` lookups). The `15/16` factor is bit-shift-friendly geometric decay; the lookup-based interval makes the decay rate proportional to access frequency rather than wall clock.
- `regionFilled` (`SsdFileTracker.cpp:32-36`, called when a region transits writable→evictable) sets the region's score to `max(currentScore, 1.1 × best)` where `best` is the max across all regions including this one. Just enough above the current top to survive one decay pass without becoming permanently top-of-mind.
- `findEvictionCandidates` returns up to `kNumEvictionCandidates = 3` (`SsdFile.h:424`) **unpinned** regions with score `<= avg`, sorted ascending.

`growOrEvictLocked` (`SsdFile.cpp:293-340`) is called only when `writableRegions_` is empty:

1. Grow: if `numRegions_ < maxRegions_`, truncate file by one region.
2. Else pick up to 3 candidates from `tracker_`.
3. If all regions are pinned, set `suspended_ = true` and return false; the cache stops accepting reads/writes until pins drop.
4. Otherwise `logEviction(candidates)` (synchronous append — see §4.6), `clearRegionEntriesLocked` to drop map entries pointing into those regions, reset the regions to `writableRegions_`.

`unpinRegion` retries `growOrEvictLocked` once a pin drops to zero (`SsdFile.cpp:159-161`), so **suspended state self-heals**.

### 4.6 Checkpointing & recovery

Two side files: `<name>.cpt` and `<name>.log` (`SsdFile.h:587-588`).

Checkpoint format (`SsdFile.cpp:800-904`):

```text
[4-byte magic CPT1/CPT2]      ← CPT2 enables checksum mode
[maxRegions]
[numRegions]
[regionScores[]]
[{fileId, name} pairs]
[kCheckpointMapMarker = 0xfffffffffffffffe]
[{fileNum, offset, fileBits[, checksum]} triples]
[kCheckpointEndMarker  = 0xcbedf11e]
```

Both markers are declared as `int64_t` (`SsdFile.h:412, 414`) and read at 64-bit width (`SsdFile.cpp:1091, 1117`). Collision avoidance for both relies on the same density argument: `StringIdMap::lastId_` is a monotonically-incrementing counter starting at 0 (`StringIdMap.cpp:57-84`), so allocated `fileNum`s are dense at the bottom of the `uint64` range. Reaching `kCheckpointMapMarker` (`0xfffffffffffffffe`, ≈`2^64`) would require an astronomical number of `makeId` calls; reaching `kCheckpointEndMarker` (`0xcbedf11e`, ≈3.4 billion) would require that many unique files in one process lifetime — both impractical.

Critical ordering:

1. The data file `flush()` is dispatched to the cache executor via `AsyncSource` so it can run **in parallel** with checkpoint metadata serialization. The checkpoint thread waits on `fileSync->move()` (`SsdFile.cpp:876`) before appending the end marker, so the marker is never durable until the data has been fsynced.
2. The eviction log is truncated **after** the checkpoint succeeds (`SsdFile.cpp:888-893`) — recovering can never see a checkpoint without its corresponding evictions.
3. When checkpointing is enabled, `logEviction` does a **synchronous** write on every eviction (`SsdFile.cpp:667-679`); when checkpointing is disabled, no log is written.
4. `readCheckpoint` reads the log into `evictedMap` and skips any entry whose region was logged as evicted (`SsdFile.cpp:1100-1132`).

If checkpointing is disabled, the constructor proactively deletes pre-existing `.cpt`/`.log` to avoid the toxic combination `{data file + stale checkpoint}` (`SsdFile.h:538-548`).

**Startup recovery.** `SsdFile::initializeCheckpoint` (`SsdFile.cpp:923-963`) is the entry point that ties recovery together at process start: it opens `.cpt` and `.log`, calls `readCheckpoint` (which replays evicted regions, recovers `StringIdLease`s via `recoverId`, and rebuilds `entries_`/`regionScores_`). On parse failure it clears `entries_` and deletes the meta files (graceful degrade — start cold rather than crash). A failure to *open* the eviction log is fatal (`VELOX_FAIL`) — process won't start. This is the entire "warm cache after restart" mechanism; without it the SSD tier would behave as a write-only cache.

### 4.7 Disk failure handling

`State` is `kActive` or `kNoSpace` (`SsdFile.h:315-318`). On `ENOSPC` from `pwrite`, `state_` flips to `kNoSpace`; subsequent `write()` calls short-circuit with `++stats_.writeSsdDropped`. Checkpointing is skipped while in `kNoSpace`. A failed checkpoint calls `checkpointError`, which deletes meta files and zeros `checkpointIntervalBytes_` so the cache continues without checkpointing (`SsdFile.cpp:743-749`).

### 4.8 SSD read path

`SsdFile::find` (`SsdFile.cpp:164-181`):

1. Take the write lock, call `tracker_.fileTouched(entries_.size())` for decay accounting.
2. Look up key in `entries_`; on miss return empty `SsdPin`.
3. `pinRegionLocked(run.offset())`; return an `SsdPin` (RAII).

`SsdFile::load(ssdPins, pins)` (`SsdFile.cpp:194-247`):

1. Validate each `runSize >= entry->size()`; call `regionRead` for scoring.
2. Dispatch to `readPins` with **gap = 25 KB when average payload size < 10 KB, otherwise 50 KB** (`SsdFile.cpp:223`) and `maxBatch = 900` (`SsdFile.cpp:227`). The 900 leaves headroom under the typical Linux `IOV_MAX = 1024` for internal vector entries plus user buffers. SSD-side gaps are aggressive because tiny gaps cost less than extra `preadv` syscalls.
3. The lambda calls `readFile_->preadv(offset, buffers)`. May issue *multiple* `preadv` calls if gap or batch limits are exceeded.
4. After IO, each entry is tagged with `setSsdFile(this, offset)` and optionally CRC-verified by `maybeVerifyChecksum` (`SsdFile.cpp:984-1010`). Read verification is gated by `checksumReadVerificationEnabled_` (`SsdFile.h:604`) — a flag *distinct* from the write-side `checksumEnabled_` (the latter populates checksums during write, the former checks them during read). Both must be true for read verification to fire (`SsdCache.cpp:47-51` forces `checksumReadVerificationEnabled = false` if `checksumEnabled` is false). On checksum mismatch the function increments `stats_.readSsdCorruptions` and `VELOX_FAIL`s with an `IOERR:` message — corruption is **fail-fast**, not silently masked.

### 4.9 SSD write path

`SsdFile::write` (`SsdFile.cpp:363-469`):

1. Drop early if `kNoSpace` or `entries_.size() + pins.size() >= maxEntries_`.
2. **Sort pins by `(fileNum, offset)`** (`SsdFile.cpp:384`) so storage-adjacent data lands SSD-adjacent — this is what makes future SSD reads coalescable.
3. Loop calling `getSpace`, packing as many sequential pins as fit into the head writable region. `getSpace` (`SsdFile.cpp:256-291`) is what calls `regionFilled` and pops the region from `writableRegions_` when no more pins fit.
4. Build per-entry `iovec` arrays (multi-run for non-contiguous allocations) respecting `IOV_MAX`; flush with `writeFile_->write(iovecs, offset, length)` — a single `pwritev` per writable-region span. Small entries from the same `fileNum` end up physically adjacent on SSD.
5. Under the lock, populate `entries_[key] = SsdRun(offset, size, checksum)` and call `entry->setSsdFile(this, offset)` so the RAM entry knows where its SSD copy lives (and `setExclusiveToShared`'s `ssdFile_ != nullptr` check will short-circuit any future re-save attempt — §3.6).
6. If `checkpointEnabled()`, end with `checkpoint()` — a no-op until `bytesAfterCheckpoint_ >= checkpointIntervalBytes_`.

### 4.10 SSD save admission (thresholds)

The defaults that gate when `possibleSsdSave` (§3.8) actually flushes (full table in §8):

- `maxWriteRatio = 0.7` — per-shard `appendSsdSaveable` cap (see §3.8).
- `ssdSavableRatio = 0.125` — primary trigger; flush once unsaved bytes exceed 12.5% of cache.
- `minSsdSavableBytes = 16 MiB` — floor for the primary trigger; matches typical SSD erase-block size.
- `ssdFlushThresholdBytes = 0` — optional **secondary trigger**; if non-zero, also flushes when unsaved bytes exceed this value (an *additional* lower trigger, not an upper bound). 0 disables.

### 4.11 TTL controller and `removeFileEntries`

`CacheTTLController` (`CacheTTLController.h:44-101`) is a process-wide singleton tracking `fileNum → {openTimeSec, removeInProgress}` in a `folly::Synchronized<F14FastMap>`.

- **TTL is opt-in and externally driven.** In OSS velox there is no caller of `applyTTL` outside tests; the embedding application (e.g. Prestissimo) must call it on its own schedule. `addOpenFileInfo` is wired by `FileSplitReader` only when the controller has been explicitly `create()`d. Use cases include compliance (PII age-out) and invalidating files known to be replaced upstream.
- **Two-tier removal.** `applyTTL(ttlSecs)` computes `getAndMarkAgedOutFiles(now - ttlSecs)`, then calls `cache_.removeFileEntries(filesToRemove, filesRetained)` (§3.9) which fans out to RAM shards first, then SSD shards.
- `cleanUp` prunes the controller's own map, *keeping* retained files so the next cycle can retry pinned entries.
- `getCacheAgeStats` reports the age of the oldest open file.

`SsdFile::removeFileEntries` (`SsdFile.cpp:590-665`) walks `entries_`, drops entries whose `fileNum` is in `filesToRemove` unless the region is pinned. After erasing, any **unpinned** region whose `erasedRegionSizes_[r] > regionSizes_[r] * 50/100` (`kMaxErasedSizePct`, `SsdFile.h:420`; condition at `SsdFile.cpp:633`) is fully cleared and re-added to `writableRegions_` — opportunistic compaction. 50% is the break-even where the cost of clearing and re-writing recovers the wasted space.

---

## 5. Reader Glue — `CachedBufferedInput`

> **Connector scope.** In OSS velox the Hive connector is the only consumer of `CachedBufferedInput`; Iceberg, Hudi, and Delta tables are read through Hive-connector extensions (e.g. `HiveIcebergSplit`, `FileSplitReader`) and inherit this exact cache wiring.

This section covers the bridge between a column reader and the cache. §5.1–§5.3 cover the reader→cache wiring; §5.4–§5.5 cover the observability surfaces those wirings produce. Two key collaborators:

### 5.1 ScanTracker (`ScanTracker.h:87-167`)

Lives at the Task / TableScan level — `Connector::trackers_` is a `weak_ptr` map keyed by `scanId` only (`Connector.cpp:80, 84, 91`); stored as `weak_ptr` so a finished scan's tracker can be destroyed by its last `shared_ptr` holder without leaving a dangling map entry (`unregisterTracker` is the explicit cleanup path). The `loadQuantum` argument is accepted by `ScanTracker`'s constructor (`ScanTracker.h:99`) but **never stored as a member** — it is silently dropped for *all* callers, not just the second. **Consequence**: if two callers in the same scan ask for the tracker with different `loadQuantum`s, the second caller does not influence the tracker either way; both callers share whatever single tracker exists.

Streams (column / null-stream / length-stream) are identified by `TrackingId = (node << 5) | streamKind` (5-bit stream kind, 27-bit node id), so each per-file stream has its own counter.

Two events:

- `recordReference(id, bytes, fileId, groupId)` — recorded when `CachedBufferedInput::enqueue` *plans* to read a stream's region in the upcoming stripe.
- `recordRead(id, bytes, ...)` — recorded by `CacheInputStream::Next` when bytes are actually consumed (filter pushdown / row skipping may make this much smaller).

`ScanTracker::readPct(id) = readBytes / referencedBytes * 100`. Why this matters: low `readPct` means filter pushdown is skipping most rows of a column, so the column does *not* deserve aggressive prefetching/coalescing.

`adjustedReadPct` (`BufferedInput.h:244-258`) uses `readBytes / (referencedBytes - lastReferencedBytes) * 100`, returning 0 when the denominator collapses. The subtraction excludes the *most-recent* reference batch (the one for the upcoming quantum, not yet read). For the **first** stripe of a column, `referencedBytes == lastReferencedBytes`, so `adjustedReadPct` returns 0 — the column is treated as unproven and prefetch is suppressed. On *subsequent* stripes the denominator reflects only completed reference/read pairs, so the percentage reports accurate steady-state density without being depressed by the just-added pending bytes.

`scanId` and `groupId` are orthogonal axes: `scanId` tracks one TableScan operator across all the files it reads, while `groupId` tracks files belonging to the same Hive partition.

### 5.2 CachedBufferedInput

Construction is wired by `HiveConnectorUtil::createBufferedInput`: pulls `AsyncDataCache*` from `ConnectorQueryCtx`, the `ScanTracker` from `Connector::getTracker(scanId, loadQuantum)`, and `(fileNum, groupId)` leases from the `FileHandle`.

The per-stripe lifecycle has seven phases: enqueue (§5.2.1) → plan loads (§5.2.2) → coalesce demand into prefetch (§5.2.3) → build CoalescedLoads (§5.2.4) → issue IO (§5.2.5) → schedule prefetch (§5.2.6) → adaptive next-quantum prefetch (§5.2.7).

#### 5.2.1 Enqueue phase

Per stream/region the reader calls `enqueue(region, sid)`:

1. Record a reference on the tracker.
2. Return a `CacheInputStream` that the column reader will pull from later.
3. Append a `CacheRequest{RawFileCacheKey{fileNum, offset}, length, trackingId, stream*}` to `requests_`. The `stream*` back-pointer is what later lets `CacheInputStream::Next` find "the load that contains me" via the `streamToCoalescedLoad_` map.

No IO yet.

#### 5.2.2 Load planning — `load(LogType)` (`CachedBufferedInput.cpp:222-274`)

(`LogType` is just an IO-statistics/tracing tag; `LogType::FILE` distinguishes data IO from metadata IO.)

1. Each request is split by `loadQuantum` (`makeRequestParts`). Large columns only opt their parts into coalescing if **all three** hold: `trackingData.referencedBytes > 0` (i.e. the stream has prior history) AND `readPct >= FLAGS_cache_prefetch_min_pct` (the **gflag** also listed in §8) AND `readDensity >= 0.8` (hard-coded threshold). `readDensity` is `trackingData.readBytes / (1 + trackingData.referencedBytes)` (`CachedBufferedInput.cpp:134-138`) — cumulative-since-scan-start, same fields as `readPct` but a 0.0–1.0 ratio rather than a 0–100 percentage. So `readDensity >= 0.8` corresponds to ≥ 80%, not 0.8%. The `referencedBytes > 0` guard means the very first stripe is never opted in via this path.
2. Each part is bucketed into `storageLoad[2]` / `ssdLoad[2]` indexed by *prefetch (1) vs demand (0)*. The two slots run on different schedules: prefetch loads are dispatched async to the executor; demand loads run synchronously when consumed. The bucket is chosen by:
    ```cpp
    loadIndex = (prefetchAnyway ||
                 isPrefetchPct(adjustedReadPct(trackingData))) ? 1 : 0
    ```
    where `prefetchAnyway` is set for sequential-file markers and metadata, and `isPrefetchPct(pct)` is the one-liner `pct >= FLAGS_cache_prefetch_min_pct` (`CachedBufferedInput.cpp:116-118`).
3. **Hit/miss probe**: `cache_->exists(part->key)` skips parts already in RAM; `ssdFile->find(part->key)` returns a non-empty `SsdPin` if it lives on local SSD.
4. After sorting by offset, `makeLoads` runs `groupRequests` which calls `coalesceIo`. Two distinct gap budgets apply:
   - **Storage planning** uses `options_.maxCoalesceDistance()`, which reads `io::ReaderOptions::maxCoalesceDistance_` (default `kDefaultCoalesceDistance = 512 KiB`, `velox/common/io/Options.h:66, 211`). Remote round-trip dominates over wasted bytes. (Note: the inherited `BufferedInput::maxMergeDistance_` defaults to `kMaxMergeDistance = 1.25 MiB`, but it is *not* what the storage coalescer reads.)
   - **SSD planning** uses a hard-coded **20 KB** in `groupRequests<kSsd=true>` (`CachedBufferedInput.cpp:298`).

  At *issue* time the SSD path further coalesces with the 25/50 KB thresholds in `SsdFile::load` (§4.8) — a two-stage coalescer.

#### 5.2.3 `moveCoalesced` — demand piggybacks on prefetch

`moveCoalesced` (`BufferedInput.h:264-…`) walks the prefetch group ranges (between consecutive prefetch requests `j-1` and `j`), finds non-prefetch (demand) requests whose `[offset, end)` is fully covered by the gap `[prefetchEnd_{j-1}, prefetchStart_j)`, and **interleaves them into the prefetch vector in offset order**. Non-prefetch requests not covered stay in the demand list. This is the central optimization that turns sparse column reads cheap: a single prefetch IO satisfies demand reads for free.

#### 5.2.4 Coalesced loads

`readRegion` creates either:
- `SsdLoad` (`CachedBufferedInput.cpp:483-523`) — read from local SSD into RAM pages.
- `DwioCoalescedLoad` (`CachedBufferedInput.cpp:424-480`) — read from remote storage via `ReadFile`.

`DwioCoalescedLoadBase` (`CachedBufferedInput.cpp:336-421`) is a small abstract parent for both — they need identical request/stats bookkeeping but differ only in *where* bytes come from. The base owns the `requests_` vector, `groupId_`, `IoStatistics` references, and the `updateStats(stats, prefetch, ssd)` helper that increments the right combination of `read` / `ssdRead` / `prefetch` / `incRawOverreadBytes` based on load type and prefetch-ness.

Each load is recorded in `streamToCoalescedLoad_` so `CacheInputStream::Next` can find it (`CachedBufferedInput.cpp:609-624`). The lookup function **erases the entries for every sibling stream in the same load** before returning, so the *next* stream's `Next` won't re-trigger the same load — this is how demand loads stay deduplicated across columns of the same stripe.

#### 5.2.5 Issuing the IO

`DwioCoalescedLoad::loadData`:

1. `cache_->makePins(keys, sizeFn, callback)` — atomically allocate exclusive `CachePin`s for all keys.
2. `cache::readPins(pins, maxCoalesceDistance_, /*maxBatch*/ 1000, ...)` calls back into `input_->read(buffers, offset, LogType::FILE)` — **a single `preadv` per coalesced range, filling many cache pages from one `ReadFile`**. Note the storage path uses `maxBatch = 1000` (pin-grouping cap, indirectly bounds iovec count); the SSD path uses `maxBatch = 900` chosen specifically for `IOV_MAX = 1024` headroom.
3. `updateStats` increments `read()`, `prefetch()`, `ssdRead()`, and `incRawOverreadBytes` (gap bytes that coalescing brought in).

#### 5.2.6 Prefetch scheduling

`readRegions` pushes each prefetch load onto `executor_` via `executor_->add(... pendingLoad->loadOrFuture(nullptr, ssdSavable))`. **The executor is the connector's `ioExecutor_`** (`HiveConnector.cpp:45`) — the same one used for connector IO generally — *not* a dedicated cache pool. There is no internal queue check or rejection: backpressure depends entirely on the executor's policy (a bounded `CPUThreadPoolExecutor` will queue or block; an unbounded one won't). SSD writes use a *separate* executor passed via `SsdCache::Config::executor`.

**Cache-wide prefetch admission gate.** `CachedBufferedInput::shouldPreload` (`CachedBufferedInput.cpp:87-112`, called before submitting any prefetch) admits if **either** of two conditions holds:

1. The page allocator has free space: `numPages < allocator->capacity() - allocator->numAllocated()`. While free pages exist, prefetch is unrestricted.
2. Otherwise (allocator full): `pendingPrefetch + planned < cachePages / 2` — i.e. half-cache cap. `pendingPrefetch` comes from the live `AsyncDataCache::prefetchPages_` counter (§3.4).

So the half-cache rule is the binding cap *under memory pressure only*; in a half-empty cache prefetch is essentially uncapped at this layer. Without condition (2), a runaway scan that filled the allocator would saturate the cache with speculative data.

`readRegions` tracks `startIndex` so it only submits loads created by *this* call, and compacts `coalescedLoads_` to drop already-finished loads. Demand loads are not submitted; they will run synchronously when the first `CacheInputStream::Next` hits them.

#### 5.2.7 Adaptive next-quantum prefetch (dormant in OSS)

`CacheInputStream::Next` (`CacheInputStream.cpp:106-110`) is wired to issue `bufferedInput_->prefetch(nextRegion)` once consumption crosses `prefetchPct_` of the current quantum, giving overlap between IO and decode within a single column. However, `prefetchPct_` defaults to **200** (`CacheInputStream.h:187`) and the gate condition is `prefetchPct_ < 100`, so the path is normally inactive. The only setter `setPrefetchPct` (`CacheInputStream.h:108`) has no production caller in OSS Velox — it is exercised only by `velox/dwio/dwrf/test/CacheInputTest.cpp`. Treat this as a wired-but-dormant feature, similar to the `FileGroupStats` stub in §2.2.

### 5.3 No-cache path: DirectBufferedInput

`DirectBufferedInput` shares the enqueue/load/coalesce flow and even reuses `ScanTracker` and `IoStatistics`, but `DirectCoalescedLoad::loadData` writes bytes into a private `memory::Allocation` per `LoadRequest` (or into `request.tinyData` for `length <= kTinySize`, mirroring the §3.1 RAM-tier carve-out). There's no `findOrCreate`, no SSD probe, no executor-driven inter-query reuse, and `shouldPrefetchStripes()` returns `false`. It exists for queries that explicitly disable the cache.

### 5.4 Observability — IoStatistics

`IoStatistics` (`velox/common/io/IoStatistics.h`) is what operators read to diagnose cache effectiveness. Counters here are scoped per-`IoStatistics` instance, typically attached to one query/operator. Complementary to the cache-wide `CacheStats` in §5.5 (which is aggregated process-wide via `AsyncDataCache::refreshStats`).

Beyond byte counters (`read`, `prefetch`, `ssdRead`, `ramHit` — the per-reader mirror of `CacheStats::numHit` from §5.5) it exposes:

- `rawOverreadBytes` — bytes pulled in by gap-spanning coalescing that no requester actually wanted. **High values mean coalescing is paying for round-trips with bandwidth — typically a good trade for remote storage; a warning sign for local SSD.**
- `queryThreadIoLatencyUs` — IO time charged to the query thread (synchronous misses).
- `storageReadLatencyUs` — wall time of remote-storage reads.
- `ssdCacheReadLatencyUs` — wall time of SSD reads.
- `cacheWaitLatencyUs` — time spent waiting on `kExclusive` entries. **High value with low `ramHit` indicates lock contention rather than cache effectiveness.**
- `coalescedSsdLoadLatencyUs` / `coalescedStorageLoadLatencyUs` — per-load wall times.
- `inputBatchSize` / `outputBatchSize` — for tuning batch sizing.

### 5.5 CacheStats — entry-level counters

For the lower-level entry counters surfaced in `AsyncDataCache::refreshStats` (each is per-shard summed cache-wide; SSD counters in `SsdCacheStats` follow the same naming):

| Stat | Counts |
| --- | --- |
| `numHit` / `hitBytes` | Reuse of a *non-prefetch* cached entry. First reuse of a prefetched entry doesn't count (it bumps `isFirstUse_`). |
| `numNew` | New entries created on miss. |
| `numEvict` | Entries dropped by the score-based clock sweep. |
| `numSavableEvict` | Subset of `numEvict` that were still SSD-saveable (i.e., data lost between tiers). |
| `numEvictChecks` | Entries inspected by the clock. `numEvictChecks / numEvict` is the inverse of eviction efficiency — higher means the clock had to inspect more candidates per successful eviction. |
| `numAgedOut` | Entries removed by `removeFileEntries` (TTL or explicit drop). **Not** scored aging — counted separately from `numEvict`. |
| `numStales` | Entries evicted because a larger-quantum request arrived for the same offset. |
| `numWaitExclusive` | Hits on entries currently being filled by another thread. |
| `sumEvictScore` | Sum of scores of evicted entries; `sumEvictScore / numEvict` correlates to time data lives in cache. |
| `numSkippedSaves` (cache-wide) | Counts evict-time *failed* `startWrite()` attempts (SSD already busy). Reset to 0 when a subsequent evict-time `startWrite()` succeeds. `possibleSsdSave` does not touch it. |

---

## 6. End-to-End Flows

### 6.1 Cold read (RAM miss + SSD miss)

```text
HiveConnectorSplit
   └── FileHandleFactory::generate           ← handle cache MISS → openFileForRead
        └── CachedBufferedInput
             ├── enqueue: tracker.recordReference, push CacheRequest
             ├── load():
             │     cache.exists(key)        → false
             │     ssdFile.find(key)        → empty SsdPin
             │     part → storageLoad[loadIndex]
             │           # loadIndex = (prefetchAnyway ||
             │           #              isPrefetchPct(adjustedReadPct(...))) ? 1 : 0
             │           # first-stripe column: adjustedReadPct == 0 → loadIndex = 0
             │           # steady state with proven density: loadIndex = 1
             │     coalesceIo               → groups
             │     readRegion → DwioCoalescedLoad
             ├── if loadIndex == 1: executor → loadOrFuture(nullptr)
             │   else: runs synchronously on first CacheInputStream.Next
             ├── loadData(prefetch):
             │     cache.makePins                            (exclusive entries)
             │     readPins → input.read → ReadFile.preadv   ← actual remote IO
             │     setExclusiveToShared(ssdSavable=cacheable)   # cacheable typically true
             │       → since ssdFile_ == nullptr (fresh from storage):
             │           ssdSaveable_ = true; possibleSsdSave(size)
             │            → MAY dispatch a batched SSD write later (§3.8)
             └── CacheInputStream.Next → finds shared pin
# numHit not bumped (first use, isFirstUse_ flag)
```

### 6.2 Warm read (RAM hit)

```text
CachedBufferedInput::load:
   cache.exists(key)  → true   → drop from both load lists
CacheInputStream::Next → loadPosition → loadSync:
   cache.findOrCreate(key, length, ...) → shared CachePin (already loaded)
   getAndClearFirstUseFlag() returns false (not first reuse) →
       ioStatistics.ramHit() ++   (CacheInputStream.cpp:222-225)
# Zero IO. Zero allocation.
```

### 6.3 Warm read (SSD hit, promoted to RAM)

```text
CachedBufferedInput::load:
   cache.exists(key)  → false
   ssdFile.find(key)  → SsdPin (covers requested size)
   part → ssdLoad[0/1]
makeLoads<true> → SsdLoad
SsdLoad::loadData:
   cache.makePins                          ← new exclusive RAM entries
   ssdPins[0].file().load(ssdPins, pins):
        readPins → readFile_.preadv        ← one or more coalesced preadv calls
                                            (gap- and IOV-cap bounded)
                                            Adjacent SSD entries are physically packed
                                            because of sort-by-(fileNum, offset) at write time
   each pin → setSsdFile(this, offset)     ← marks entry as SSD-resident
              setExclusiveToShared(ssdSavable=cacheable)
                 → ssdFile_!=nullptr branch short-circuits, no re-save attempted
ioStatistics.ssdRead() ++
# Subsequent reads for the same key → §6.2
```

---

## 7. Non-Obvious Invariants & Design Decisions

| # | Invariant | Where enforced |
| --- | --- | --- |
| 1 | RAM and SSD shard **independently**: RAM by `hash(fileNum, offset) & shardMask`, SSD by `fileNum % numSsdShards`. They have independent shard counts. | `AsyncDataCache.cpp:874`, `SsdCache.cpp:80-83` |
| 2 | A `kExclusive` placeholder is published in `entryMap_` while the slow allocation runs *outside* the mutex | `AsyncDataCache.cpp:300-325` |
| 3 | Eviction skips `ssdSaveable_` entries while `ssdCache_->writeInProgress()`; if any were skipped, evict tries `startWrite()` — on success calls `saveToSsd()` and resets `numSkippedSaves_`, on failure increments `numSkippedSaves_`. Mutually exclusive | `AsyncDataCache.cpp:567-569, 600-611` |
| 4 | `numPins_ < 0` (`kExclusive`) is the only "writer" state; readers atomically incr only when shared | `AsyncDataCache.h:357` |
| 5 | `CoalescedLoad`'s destructor cancels via `setEndState(kCancelled)` so waiters never block forever | `AsyncDataCache.cpp:383-386` |
| 6 | All-or-nothing SSD batch writes — `writesInProgress_` bumped by `numShards_` under the SsdCache mutex; lock-free per-shard decrement at completion | `SsdCache.cpp:85-95, 140` |
| 7 | Eviction log is written **synchronously before** region bytes are reused (and **only when** checkpointing is enabled) | `SsdFile.cpp:667-679`, called from `growOrEvictLocked` at `SsdFile.cpp:334-337` |
| 8 | Checkpoint finalizes (end marker durable) **after** the data-file flush completes; eviction log truncated **after** the checkpoint succeeds | `SsdFile.cpp:876, 878, 888-893` |
| 9 | Stale `.cpt` + reused data file is toxic; if checkpointing is disabled, meta files are deleted at startup | `SsdFile.h:538-548` |
| 10 | `regionFilled` sets score to `max(currentScore, 1.1 × best)` — fresh regions survive at least one decay pass without becoming permanently top-of-mind | `SsdFileTracker.cpp:32-36` |
| 11 | `SsdRun` packs `(size-1)` in low 23 bits — decoders must add 1; `−1` lets max size = 2²³ = 8 MiB exactly, matching `loadQuantum` | `SsdFile.h:39-92` |
| 12 | `suspended_` self-heals: `unpinRegion` retries `growOrEvictLocked` when a pin drops | `SsdFile.cpp:159-161` |
| 13 | Pins on the SSD side stop in-place region overwrite (not just LRU eviction) — same pattern as RAM | `SsdFile.cpp:164-181` |
| 14 | `StringIdLease` keeps file id stable across SSD restart via `recoverId` | `StringIdMap.h:62` |
| 15 | Stale-entry self-heal: a too-small cached entry is silently evicted when a larger load quantum requests the same offset | `AsyncDataCache.cpp:267-278` |
| 16 | Sort SSD writes by `(fileNum, offset)` so future SSD reads of adjacent storage offsets coalesce | `SsdFile.cpp:384` |
| 17 | SSD coalescer gap = 25 KB when avg payload < 10 KB, else 50 KB; SSD `maxBatch = 900` (under `IOV_MAX = 1024`). Storage-side `DwioCoalescedLoad` uses per-instance `maxCoalesceDistance` and `maxBatch = 1000` | `SsdFile.cpp:223, 227`; `CachedBufferedInput.cpp:447-476` |
| 18 | First reuse of a prefetched entry does **not** count as a hit; `numHit` measures only confirmed reuse | `AsyncDataCache.cpp:280-282` |
| 19 | SSD-source dedup happens inside `setExclusiveToShared`: SSD-loaded entries already have `ssdFile_` set by `SsdFile::load`, so the `ssdFile_ != nullptr` branch short-circuits before `ssdSaveable_` would be assigned. `SsdLoad` still passes `ssdSavable=true` to be uniform with storage loads | `AsyncDataCache.cpp:99-109` |
| 20 | `shrink` (memory-arbitration path) uses `evictAllUnpinned=true` and bypasses the saveable-skip — under host pressure, freeing memory beats preserving SSD admission | `AsyncDataCache.cpp:997-1035` |
| 21 | `makeSpace` sleeps **500 ms** when `nthAttempt > 2` (4th attempt onwards) and SSD writes are in progress — the dominant tail-latency cliff under memory pressure | `AsyncDataCache.cpp:955-959` |
| 22 | `shouldPreload` admits if either (a) allocator free space available, or (b) `pendingPrefetch + planned < cachePages / 2`. The half-cache cap binds only when the allocator is full | `CachedBufferedInput.cpp:87-112` |
| 23 | `Connector::trackers_` is keyed by `scanId` only; the `loadQuantum` argument to `getTracker` is *never stored* by `ScanTracker` — silently dropped for all callers | `Connector.cpp:80-91`, `ScanTracker.h:99` |
| 24 | Failed exclusive entry recovery: when `loadData` throws, the destructor of the exclusive `CachePin` calls `release()` which removes the entry and wakes all queued waiters via the `SharedPromise`, who retry. Without this, IO failures would deadlock the shard | `AsyncDataCache.cpp:112-127` |

---

## 8. Configuration Surface (Quick Reference)

### 8.1 `AsyncDataCache::Options`

| Option | Default | Effect |
| --- | --- | --- |
| `numShards` | 4 | Mutex sharding; must be power of 2; **independent of** `SsdCache` numShards |
| `maxWriteRatio` | 0.7 | Per-shard cap on `appendSsdSaveable` cumulative pin count (= `entries_.size() × ratio`); back-pressure on foreground reads |
| `ssdSavableRatio` | 0.125 | Primary trigger; flush once unsaved bytes exceed 12.5% of cache |
| `minSsdSavableBytes` | 16 MiB | Floor for the primary trigger; matches typical SSD erase-block size |
| `ssdFlushThresholdBytes` | 0 | Optional **secondary trigger**; if non-zero, also flushes when unsaved bytes exceed this value (additional lower trigger, not an upper bound). 0 = disabled |

### 8.2 `SsdCache::Config`

| Option | Default | Effect |
| --- | --- | --- |
| `numShards` | — | Number of `SsdFile` shards; independent from RAM sharding; need not be power of 2 |
| `maxBytes` | embedder-set | Total bytes (rounded up to `numShards × kRegionSize` multiple) |
| `maxEntries` | 0 (= no limit) | Total entries, divided evenly across shards |
| `checkpointIntervalBytes` | 0 (= no checkpointing) | Bytes-since-last-checkpoint trigger; 0 disables checkpoints entirely |
| `checksumEnabled` | false | Populate per-entry CRC32 at write time (CPT2 magic instead of CPT1) |
| `checksumReadVerificationEnabled` | false | Verify CRC32 at read time. Both this AND `checksumEnabled` must be true for verification to fire (`SsdCache.cpp:47-51` enforces this) |
| `executor` | — | `folly::Executor*` for SSD writes; **distinct** from the connector's `ioExecutor` |

### 8.3 `CachedBufferedInput`

| Option | Default | Effect |
| --- | --- | --- |
| `loadQuantum` | 8 MiB | Split unit for `enqueue` requests; matches SSD `SsdRun` max size |
| `maxCoalesceDistance` | 512 KiB (`kDefaultCoalesceDistance`) | Storage-side coalescer gap budget; reads from `io::ReaderOptions`. (The inherited `BufferedInput::maxMergeDistance_` uses `kMaxMergeDistance = 1.25 MiB` but isn't consulted here.) |
| `maxCoalesceBytes` | embedder-set | Coalescer batch size budget |
| `executor` | embedder-set | folly executor for prefetch loads (Hive connector passes `ioExecutor_`) |

### 8.4 Process-wide gflags

Not per-instance options:

| Flag | Default | Effect |
| --- | --- | --- |
| `FLAGS_cache_prefetch_min_pct` | 80 (`flags.cpp:118-121`) | Read-density floor for opt-in coalescing & prefetch slot; consumed by `isPrefetchPct` (§5.2.2). Values > 100 disable prefetch entirely |

### 8.5 `FileHandleFactory`

| Option | Default | Effect |
| --- | --- | --- |
| `numCacheFileHandles` | embedder-set | Count-based LRU capacity for open `ReadFile` objects |
| `expireDurationMs` | unset | Supported by `SimpleLRUCache` but not currently wired by the Hive connector |

### 8.6 Deployment Assumptions

- The SSD tier requires a **local POSIX filesystem** (typically a dedicated NVMe device). The `<name>.cpt`/`<name>.log` files are written via `pwrite`/`pwritev`/`flush`; networked or shared filesystems are not supported and will likely break the recovery invariants.
- There is **no internal RAID or replication** — the cache is rebuildable on data loss; the checksum option detects corruption but does not repair it.
- The cache is a **single-process** structure; there is no inter-process or inter-host coordination.
- The connector's `ioExecutor` and `SsdCache::Config::executor` should be distinct so SSD writes don't queue behind reader prefetches.

---

## 9. Where to Read Next

For deeper study:

- **Memory accounting & arbitration.** `velox/common/memory/MemoryAllocator.h`, `Memory.h` — the `Cache` interface AsyncDataCache implements (`makeSpace`, `shrink`).
- **Coalescing primitives.** `velox/common/base/CoalesceIo.h` — generic algorithm reused across RAM, SSD, direct paths.
- **Reader integration tests.** `velox/dwio/dwrf/test/` and `velox/common/caching/tests/AsyncDataCacheTest.cpp` — ground truth on the state machine semantics.
- **Connector wiring.** `velox/connectors/hive/HiveConnectorUtil.cpp` — see how `CachedBufferedInput` is constructed per split.
- **Recovery scenarios.** `velox/common/caching/tests/SsdFileTest.cpp` — exhaustive checkpoint/log/crash matrix.
- **IO stats.** `velox/common/io/IoStatistics.h` — full counter list.

---

*Generated from velox HEAD (`/Users/yangjie01/SourceCode/git/velox`).*
