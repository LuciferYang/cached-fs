## Phase 5b — Multi-chunk coalescing (medium)

Cut IO syscalls for sequential reads. When a positional read crosses N missed chunks, fill via one `preadv`. Drive `CoalesceIo` + `preadv` directly.

### Algorithm

1. **Walk + classify.** For each chunk in `[startChunk, endChunk]`, call `findOrCreate(key, size, false)`. Build a `List<Resolved>`:
   - `Hit` → `Resolved.hit(pin)`.
   - `Exclusive` → `Resolved.exclusive(pin)`.
   - `Waiting` → **abort sub-routine**: release every pin held so far in ascending offset order (pins were acquired in ascending order; releasing in the same order matches stack discipline and is simpler to reason about than the v3 descending-order claim). Hit pins via `pin.close()`; Exclusive pins via `pin.close() (which routes Exclusive pins to CacheShard.releaseFailedExclusive internally; see CachePin.close at line 101-112)`. If any release throws, accumulate via `Throwable.addSuppressed` and continue releasing the rest; rethrow the original abort trigger only after every pin is released. Await the future. Increment a per-call `restartCount`; restart from `startChunk`. **Bound:** `fs.cached.coalesce.max-restarts` (default 3). On exceeding the bound, fall back to the per-chunk `copyChunk` path.
2. **Coalesce Exclusives.** Group consecutive `Resolved.exclusive(...)` via `CoalesceIo` with `maxGap = fs.cached.coalesce.max-gap-bytes`. Apply group cap (see §Group sizing).
3. **Issue IO per group.** Concatenate `dataRanges(chunkSize)` from every member; one `handle.readFile().preadv(groupStartOffset, buffers)` per group. `ioStats.incRawOverreadBytes(gapBytes)` is called HERE, where `gapBytes` is the bytes the coalescer absorbed (`groupSpan - sum(chunkSize)`); 0 for purely-adjacent chunks.
4. **Promote.** For each Exclusive in the group, `exclusiveToShared(true)`. On any throw in steps 3 or 4: release every still-Exclusive in ascending order via `pin.close() (which routes Exclusive pins to CacheShard.releaseFailedExclusive internally; see CachePin.close at line 101-112)`; close every already-promoted Shared pin and every Hit pin (suppressed-exception chain); rethrow.
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

