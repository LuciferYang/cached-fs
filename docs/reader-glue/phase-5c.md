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
   4. If timeout, `prefetchExecutor.shutdownNow()` AND a second `awaitTermination(10s)`. If STILL not terminated, log at ERROR and proceed. **Upper bound on leaked pins:** prefetch tasks each hold at most one chunk's exclusive pin at a time AND the executor is bounded to `threads` concurrent tasks AND the queue is capped at `queueSize`. The maximum leaked pin bytes is `(threads + queueSize) × loadQuantumBytes`. Document this bound; with default `threads=availableProcessors` (~16) and `queueSize=64` and `loadQuantum=8 MiB`, peak leak ≈ `80 × 8 MiB = 640 MiB` — bounded and operator-tunable. Operators with tighter budgets should drop `queueSize` further (Phase 5c knob `fs.cached.prefetch.queue`).
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
   - `private final AtomicLong sequentialReadHighWater = new AtomicLong(-1L)` — sentinel `-1L` means "no read observed yet; prefetch disabled until armed." **AtomicLong (not plain `long`)** because `PositionedReadable.read(long, byte[], int, int)` is contractually thread-safe per Hadoop's `PositionedReadable` javadoc AND can run concurrently with sequential `read()` operations on the same stream (PositionedReadable explicitly does not modify the current position, so the Hadoop contract permits the mix). Every update site MUST tolerate concurrent callers: the **sequential** read path uses an explicit `compareAndSet` loop (v7.20 — see §5c counter spec for the body); the **positional** read path uses `updateAndGet` with a contiguity-or-bootstrap lambda; the `Seekable.seek` reset uses `set(-1L)` justified by the single-threaded seek contract. A plain set on the read paths would unconditionally clobber a higher CAS-advanced value from a concurrent peer, violating monotonicity; only seek's `set(-1L)` is allowed because Hadoop's `Seekable.seek` is contractually serialized.
   - **Updated via CAS** in three sites:
     1. **Sequential read path** (`InputStream.read()` / `Seekable.read()` after per-chunk fill): explicit `compareAndSet` loop (see §5c counters for the body). Three cases inside the loop: sentinel → bootstrap; |prev - newCandidate| > 2 chunks → **regime-change reset** (positional bootstrap left HWM far ahead/behind of the sequential locus; reset to the sequential candidate, breaking the dead-zone); otherwise → monotone advance via Math.max. The 2-chunk gap tolerance avoids spurious resets on small forward seeks within sequential reads. The explicit CAS loop (vs `updateAndGet`) is required so the `seqHwmRegimeResets` counter bump derives from the SAME `prev` the winning `compareAndSet` observes — see §5c counters.
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
   Acceptance tests: (a) 1000 scattered `readFully(pos, …)` calls (pos values randomly distributed) produce `prefetchBytes == 0`; (b) 1000 contiguous `readFully(pos, …)` calls starting at `pos=0` with `pos += chunkSize` each iteration trigger prefetch on the same cadence as a sequential `read(…)` loop, AND final `seqHWM.get() == 1000 * chunkSize` (proves the state machine actually advanced, not just bootstrapped-and-froze), AND `seqHwmRegimeResets == 0` (no contention, no resets); (c) 8 threads each issuing `readFully(threadIdx*chunkSize, …)` then `readFully((threadIdx+1)*chunkSize, …)` (interleaved contiguous patterns) — no HWM corruption, no negative HWM, monotonicity-or-regime-change holds (a regime-change reset is allowed; a torn write or unexpected regression is not), AND final HWM advances to at least `2*chunkSize` (proving CAS-advance isn't dead-zoned for the bootstrapping thread); (d) **mixed sequential+positional**: one thread runs `InputStream.read()` loop while another thread runs `readFully(pos, …)` loop against the SAME `CachingInputStream`; both contiguous, interleaved at random; assert no torn writes observable via a probe thread sampling `seqHWM.get()` every 10ms (regime-change resets ARE allowed when the two threads' positions diverge), AND `seqHwmRegimeResets <= N_calls` (sanity rate bound — one reset per call is the worst case under maximal divergence), AND final HWM is within 2 chunks of one of the two threads' final position-plus-length; (e) **positional-then-sequential regime change**: open stream, `readFully(10 * chunkSize, chunkSize)` (positional bootstrap), then 100 sequential `read(...)` calls starting at position=0 — assert prefetch fires on the sequential phase (proves the regime-change reset breaks the v7.16 positional-first dead-zone), AND `seqHwmRegimeResets == 1` (the single regime-change event when sequential starts).

   Field javadoc: `/** Prefetch-only high-water-mark. Read-path writers use updateAndGet: positional path is monotone-advance under a strict contiguity check (pos == prev); sequential path is monotone-advance EXCEPT for a 2-chunk regime-change reset that breaks positional-bootstrap dead-zones — see step-4 lambda. Hadoop permits mixed sequential + positional concurrent access on one stream, so plain set would clobber a CAS-advanced peer. Only Seekable.seek uses set(-1L) as a reset, valid because Seekable.seek is contractually single-threaded per Hadoop. Sentinel -1L disables prefetch arming. Each regime-change reset bumps IoStatistics.seqHwmRegimeResets so operators can detect oscillation under sustained mixed traffic. */`.

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
       admissionGateFalseCount.incrementAndGet();                              // package-private debug counter (see acceptance test below)
     } else {  // readPct >= threshold AND !adm.admit()
       ioStats.incPrefetchSkipped(adm.reason(), chunkSize);    // "budget" | "heap_pressure"
       admissionGateFalseCount.incrementAndGet();
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

   **Bump-site invariant + acceptance test mechanism.** The debug counter `private final AtomicLong admissionGateFalseCount = new AtomicLong()` on `CachingInputStream` (package-private accessor, default 0) is **incremented via `admissionGateFalseCount.incrementAndGet()`** at every false-branch above. **v7.21 promoted this from `volatile long` + `++` to `AtomicLong`** because Hadoop's `PositionedReadable.read` is contractually thread-safe and can run concurrently with sequential reads on the same stream — `volatile long ++` is read-modify-write and would lose updates under that concurrency, breaking the load-bearing invariant. The field carries a co-located javadoc reiterating the single-writer invariant. Acceptance test reads this counter post-run via a package-private accessor `long admissionGateFalseCountForTesting()` and asserts the invariant **in bytes** (all terms have matching units):

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
       pin.close() (which routes Exclusive pins to CacheShard.releaseFailedExclusive internally; see CachePin.close at line 101-112)(excPin);
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
   private final long heapPressureTtlNs;  // initialized from fs.cached.prefetch.heap-pressure-ttl-ms (default 100) at construction; v7.21 — was `static final` which silently ignored the documented config knob.
   // Initialized in the constructor as `System.nanoTime() - heapPressureTtlNs - 1`
   // so the first call always refreshes AND the static-sentinel overflow edge
   // (when nanoTime() happens to start near Long.MAX_VALUE / 2) is eliminated.
   private final AtomicLong heapPressureLastCheckedNs;
   private volatile boolean heapPressureActive;
   public boolean isHeapPressureHigh() {
     long now = System.nanoTime();
     long prev = heapPressureLastCheckedNs.get();
     // Single-CAS-winner pattern: only one thread per TTL window refreshes,
     // even under N concurrent admission-gate callers. Hard bound on MBean calls.
     if (now - prev > heapPressureTtlNs && heapPressureLastCheckedNs.compareAndSet(prev, now)) {
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

Phase 5c adds four counters to `IoStatistics`:
- `prefetchSkipped(reason)` — bump-with-reason for **all** non-density admission failures: queue-full rejection (`reason="queue_full"`), byte-budget admission failure (`reason="budget"`), heap-pressure admission failure (`reason="heap_pressure"`), and any future `RejectedExecutionException` reason. Implementation uses a small fixed reason → AtomicLong map; only well-known reasons accepted (unknown → silently routes to a `prefetchSkipped("other")` bucket).
- `seqHwmRegimeResets` (v7.20, event-count, no bytes) — incremented exactly once per logical regime-change event via an **explicit CAS loop** (NOT `updateAndGet`, whose lambda is documented as "should be side-effect-free" and whose return value cannot reliably identify the branch the winning CAS took because the lambda's `prev` parameter differs from any pre-`get()` capture under contention). The sequential read path replaces the v7.18/v7.19 `updateAndGet` form with:
   ```java
   while (true) {
     long prev = seqHWM.get();
     long candidate = position + chunkSize;
     long next;
     boolean wasReset;
     if (prev == -1L) {
       next = candidate;
       wasReset = false;  // bootstrap, not reset
     } else if (Math.abs(prev - candidate) > 2L * chunkSize) {
       next = candidate;
       wasReset = true;   // regime-change reset
     } else {
       next = Math.max(prev, candidate);
       wasReset = false;  // monotone advance
     }
     if (seqHWM.compareAndSet(prev, next)) {
       if (wasReset) ioStats.incSeqHwmRegimeResets();
       break;
     }
     // CAS lost the race; retry with a fresh prev
   }
   ```
   The `wasReset` flag is derived from the **same** `prev` that the winning `compareAndSet` observes — exactly-once semantics regardless of retries. Acceptance test bound: under a workload of N=1000 contiguous sequential reads with no positional interleave, `seqHwmRegimeResets == 0` (no resets); under sustained mixed traffic at >2-chunk divergence, the counter ticks at the divergence-event rate.
- `prefetchEvictedBeforeUse()` — TTL evicted the prefetched entry before the consumer awaited.
- `prefetchEligibleSuppressedBytes()` — admission gate's density predicate rejected a position-eligible, backoff-elapsed chunk; lower-tail observability for collision-induced suppression (see §Decisions §6). **Union signal**: bumps on both genuine cold-scan suppression and collision-induced false-negative suppression; not disambiguable from this counter alone — operators correlate with `scanTrackerMaxEntries()` and the `prefetchEligibleSuppressedBytes / readBytes` ratio vs known baseline.

**Phase 5a adds one bootstrap-level counter** (NOT per-stream — the bump-site has no `IoStatistics` in scope):
- `AggregatedIoStatistics.staleScanIdRecoveries()` — bumped when `withScanId` enters with a stale ThreadLocal slot and auto-recovers (see Phase 5a wiring §2). Lives on `bootstrap.aggregateIoStats` (which is `AggregatedIoStatistics`, not `IoStatistics`). Bump path: `bootstrap.aggregateIoStats.incStaleScanIdRecoveries()` inside the stale-slot branch of `withScanId`. `AggregatedIoStatistics.add(IoStatistics)` does **NOT** merge this counter — it has no per-stream source. Exposed via the existing bootstrap-level dynamic-IOStatistics path (alongside `scanTrackerEntries` / `scanTrackerMaxEntries`) using `DynamicIOStatisticsBuilder.withLongFunctionCounter(name, ToLongFunction<String>)` (the counter variant of `withLongFunctionGauge`). Adapter name: `cachedfs_stale_scan_id_recoveries`. Surfaces crashing-task patterns where prior tasks fail before `close()`.

  **`AggregatedIoStatistics` counter partition (class-level javadoc):** the class now hosts two semantically distinct counter groups. The class javadoc partitions them and prescribes a naming convention so future contributors can tell at-a-glance which group a counter belongs to:
  1. **Merged-from-stream counters** (the original surface): sum of per-stream `IoStatistics` values, populated by `add(IoStatistics)`. Examples: `readBytes`, `prefetchBytes`, `prefetchSkippedByReason`, `prefetchEligibleSuppressedBytes`, `prefetchEvictedBeforeUse`. Naming: same as the per-stream field. `add(IoStatistics)` MUST merge these.
  2. **Bootstrap-only counters** (new in 5a): JVM-wide signals bumped directly by `CacheBootstrap`. Currently: `staleScanIdRecoveries`. Naming convention: future additions MUST use a `bootstrap*` prefix; `staleScanIdRecoveries` keeps its un-prefixed name because it is already an external metric surface (`cachedfs_stale_scan_id_recoveries`) and carries a field-adjacent javadoc explicitly marking it as the grandfathered exception: `/** Un-prefixed by exception — name is locked to the existing cachedfs_stale_scan_id_recoveries external metric. New bootstrap-only counters MUST use the bootstrap* prefix per class javadoc. */`. `add(IoStatistics)` MUST NOT merge these — they have no per-stream source. An acceptance test runs `add(...)` with a stream whose counters are set and asserts the bootstrap-only group is untouched.

The three byte-form Phase 5c prefetch counters (`prefetchSkipped(reason)`, `prefetchEvictedBeforeUse`, `prefetchEligibleSuppressedBytes`) are byte-only (no event count partner) since their use cases are about volume; `staleScanIdRecoveries` and `seqHwmRegimeResets` are event-count-only (no bytes meaningful). **Open follow-up (lower priority — rate alerting via `rate(counter[1m])` works equally well on bytes or events)**: add event-count partners for the three byte-form prefetch counters if operators need alerting rate-of-suppression-events rather than rate-of-suppressed-bytes. Adapter exposes the prefetch counters under `cachedfs_stream_prefetch_skipped_bytes`, `cachedfs_stream_prefetch_evicted_bytes`, `cachedfs_stream_prefetch_eligible_suppressed_bytes`, and `cachedfs_stream_seq_hwm_regime_resets`. Implementation order: the IoStatistics fields and inc* methods land alongside Phase 5c.0; `AggregatedIoStatistics` is introduced in 5a wiring step 7 with the `staleScanIdRecoveries` field already present; `AggregatedIoStatistics.add(IoStatistics)` snapshot loop includes the three prefetch counters (and explicitly skips `staleScanIdRecoveries`) — verified by an acceptance test. **Bump-site invariant for future contributors** (scoped to prefetch admission-GATE paths only — does NOT cover `staleScanIdRecoveries` or the `queue_full` rejection-handler path): every prefetch admission-gate-FALSE branch MUST bump a dedicated `IoStatistics` counter (a known `prefetchSkipped(reason)` bucket — `"budget"` or `"heap_pressure"` — or `prefetchEligibleSuppressedBytes`); a new admission-gate failure mode that falls through silently (or wrongly into `prefetchEligibleSuppressedBytes`) is a design-invariant violation. Acceptance test asserts `admissionGateFalseCount * chunkSize == prefetchSkipped("budget") + prefetchSkipped("heap_pressure") + prefetchEligibleSuppressedBytes` (all terms in bytes; the `"queue_full"` bucket is structurally outside this invariant because it fires after the gate has passed).

**`prefetchSkipped(reason)` reason map.** Implemented as `Map<String, AtomicLong>` (immutable structure, mutable AtomicLong values; the keyset is fixed at construction). On unknown reason: the counter routes to the `"other"` bucket AND logs a single deduped WARN per unknown key (via `ConcurrentHashMap<String,Boolean> seenUnknownReasons`). This surfaces contributor bugs (someone added a new rejection mode without registering its reason) rather than silently absorbing them.

### Configuration knobs

- `fs.cached.prefetch.enabled` (default `false` — opt-in).
- `fs.cached.prefetch.threads` (default `Runtime.getRuntime().availableProcessors()`).
- `fs.cached.prefetch.queue` (default 64; backpressure-sized, not throughput-sized — `DiscardAndCountHandler` is the steady-state safety valve. Reduces worst-case pin-leak window at close from 8.3 GiB to ~640 MiB with default threads/loadQuantum).
- `fs.cached.prefetch.max-pending-bytes` (default `loadQuantumBytes * threads * 4`; reasoned as "~4 chunks per thread of headroom"; see Open Follow-ups for a measurement task).
- `fs.cached.prefetch.trigger-tail-fraction` (default `0.5` — submit when the consumer is past the chunk midpoint).
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

