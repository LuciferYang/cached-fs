# Prefetch-multiplier benchmark plan (R1.1)

A self-contained plan to run the cached-fs JMH benchmarks on representative hardware and **validate**
the default for `fs.cached.prefetch.max-pending-multiplier`. Concretely, this single-stream rig can
*refute* a too-high default (a clear regression at higher multipliers) and confirm 4 is harmless; it
**cannot** prove 4 is optimal — that needs the concurrent-stream follow-up (see **Scope & limits**).
Run it on a quiet box that resembles your deployment, capture the JSON, and report results back (see
**After the run**).

## Objective

1. **Headline:** on representative hardware, check whether the current default of **4**
   (`CachedFsConfig.DEFAULT_PREFETCH_MAX_PENDING_MULTIPLIER`) is right — primarily, confirm it causes
   **no regression** versus smaller values, and detect a knee if one exists. Read the **Scope &
   limits** note first: with a single sequential reader and the production +1-chunk look-ahead, the
   budget rarely binds, so the realistic outcome is "4 is harmless" rather than a sharp optimum.
2. **Sanity:** confirm the admission gate's `LongAdder` accounting is not a contention bottleneck at
   your thread counts.

## Scope & limits (read first)

This rig measures a **single sequential reader**. Production prefetch reads only **one chunk ahead**
(`CachingInputStream` prefetches `nextChunkStart = thisChunkEnd`), so a single stream keeps at most
~1–2 chunks outstanding *regardless of the budget* — the multiplier (which sets
`loadQuantum × threads × multiplier`) is therefore rarely the binding constraint here. Consequences
you must factor into the verdict:

- A **flat** SEQUENTIAL curve across multipliers (even on a quiet box) is the *expected* result, and
  means "the default is harmless," NOT "the default is optimal."
- The multiplier's real leverage appears when **many streams concurrently** contend for the shared
  prefetch pool/budget — which this single-stream rig does **not** exercise. A true "optimal
  multiplier" answer would need a concurrent-stream benchmark (a follow-up, not this plan).
- So treat a clear **regression** at higher multipliers as a real "don't raise it" signal, and
  flatness as "4 is fine" — don't over-read a noisy 5–15% wiggle as a knee.

## What's being measured (and what isn't)

The budget is `loadQuantum × prefetchThreads × multiplier`. A bigger budget keeps more chunk loads
in flight, hiding more per-read latency — until the prefetch **thread pool**, not the budget, caps
concurrency. So the optimal multiplier is **thread-dependent**; that's why `prefetchThreads` is a
swept parameter.

The backing store is synthetic (`InMemoryLatencyFileSystem`): no disk/network, a fixed first-byte
latency parked once per chunk load (stride-aware — see `prefetch-multiplier.md`). This isolates the
multiplier's latency-hiding effect and is fully reproducible. **The numbers it produces are a model,
not real I/O** — read the *shape* (where the knee is, whether high multipliers regress), not the
absolute ms. Specifically it does NOT model:

- **Storage bandwidth.** Latency is injected (a park); per-byte transfer cost is not. The multiplier
  hides latency, so this is the right isolation, but absolute ms won't match real reads.
- **Timer granularity.** The park uses `LockSupport.parkNanos`, whose resolution/overshoot is ~tens
  of µs on most OSes. So the `latencyMicros=200` point (and the `0` control) is timer-dominated and
  its absolute value is unreliable; **trust `2000` (2 ms) for the decision** and treat 200/0 as
  directional only.
- **Concurrency.** The consumer is a single thread reading one file at a time; concurrent readers
  contending on the shared prefetch pool are out of scope. This materially limits what the multiplier
  sweep can conclude — see **Scope & limits** above.

## Prerequisites

- **JDK 21** — the project's compile target (`maven.compiler.release=21`). Install one if absent
  (e.g. `sdk install java 21-tem` via SDKMAN, your distro's `temurin-21-jdk`, or `brew install
  --cask temurin@21`). **Both** the Maven build and the benchmark run must use JDK 21:
  ```bash
  export JAVA_HOME=/path/to/jdk21
  export PATH="$JAVA_HOME/bin:$PATH"
  java -version   # must report 21
  ```
  Setting both `JAVA_HOME` and `PATH` (as in Setup) makes `mvn` and the JMH fork both run under 21;
  verify with `mvn -v` (it reports the Java version it will use). If you can't make `java` on `PATH`
  be 21 for the *run*, JMH forks a child JVM that must still be 21 — pass `-jvm "$JAVA_HOME/bin/java"`
  on every `java -jar` command. (The Maven build can't be redirected that way; `mvn` itself must run
  under JDK 21.)
- **Maven 3.9+** — the repo has no `mvnw` wrapper, so install Maven (`sdk install maven`, `brew
  install maven`, or your distro's package) and confirm `mvn -v` reports Java 21.
- A **quiet, frequency-stable machine**, ideally matching production core count. The knee can be a
  ~10% effect, so CPU frequency drift will swamp it unless you pin it — see **Quiesce the box**.
- Know your deployment's `fs.cached.prefetch.threads` (default = available processors) and your
  storage's first-byte latency. If you don't know the latency, use `2000` µs for a cloud object
  store / `200` µs for NVMe (directional only — the decision uses the 2000 µs slice; see caveats)
  and record the assumption.

## Quiesce the box

For a comparable, low-variance run:

- Set the CPU governor to `performance` (Linux: `sudo cpupower frequency-set -g performance`) or
  otherwise pin frequency / disable turbo so clocks don't drift mid-run; run on AC power (laptops).
- Close other heavy processes and other JVMs.
- Rule of thumb: if a single combo's JMH `Error` (run-to-run scatter) is more than ~10% of its
  score, the box isn't quiet enough to locate the knee — fix that before trusting results.

## Setup

Run **all** commands below from the repo root.

```bash
git clone https://github.com/LuciferYang/cached-fs.git && cd cached-fs
export JAVA_HOME=/path/to/jdk21          # <-- EDIT THIS to your real JDK 21 path
export PATH="$JAVA_HOME/bin:$PATH"
# Gate: both MUST report 21 before you build. If either says 17/24/25, fix JAVA_HOME and re-export.
java -version && mvn -v
mvn -pl cached-fs-bench -am package -DskipTests   # no -q, so a wrong-JDK error is visible
# produces cached-fs-bench/target/benchmarks.jar (self-contained, runnable)
java -jar cached-fs-bench/target/benchmarks.jar -l   # sanity check (see expected output below)
```

`-l` must print exactly two benchmarks (in either order):
`io.github.luciferyang.cachedfs.bench.AdmissionGateBenchmark.admitAndAccount` and
`io.github.luciferyang.cachedfs.bench.PrefetchMultiplierBenchmark.scan`. If either is missing, the
`package` step didn't shade the JMH `BenchmarkList` correctly — re-run it with a `clean`
(`mvn -pl cached-fs-bench -am clean package -DskipTests`) to discard any stale or wrong-JDK
`target/`.

The `-rff <file>` outputs below are **relative to the current directory**, so from the repo root
they land in the repo root (e.g. `./prefetch-sweep.json`). Adjust the path if you want them
elsewhere. Record the machine spec (see **Record**) before running.

> Memory: the RAM cache does **not** auto-evict (entry aging is driven by a TTL controller the
> benchmark never runs), every scan reads a unique 64 MiB path, and the cached chunk payload is
> **off-heap direct memory** (`ByteBuffer.allocateDirect`) — so the resource that would be exhausted
> is `-XX:MaxDirectMemorySize` (defaults to ≈ `-Xmx`), not the heap, and the OOM reads `Cannot
> reserve N bytes of direct buffer memory`. `PrefetchMultiplierBenchmark` therefore **clears the
> cache before every scan** (untimed `@Setup(Level.Invocation)`; scans are tens-to-hundreds of ms,
> well within JMH's guidance). `clear()` drops the references; the JDK Cleaner reclaims the native
> memory on direct-memory pressure, so live direct memory stays bounded to roughly one scan
> (GC-timed, not freed instantly) while every scan stays a genuine cold miss. **Default memory is
> fine on an 8 GB+ box for all slices — leave it alone.** Do NOT tighten `-XX:MaxDirectMemorySize`
> to "save memory": a too-small cap forces reclaiming GCs into the timed scan and inflates the score
> (measured 3.5× on the 0 µs slice at 384m). Only ever *raise* it (`-jvmArgsAppend
> "-XX:MaxDirectMemorySize=8g"`) and only if a genuinely long run OOMs.

## Pick your parameters

| Param | What to set it to |
| --- | --- |
| `prefetchThreads` | your deployment's prefetch pool size **and its neighbours** — replace the `8,16,32` example with your actual `fs.cached.prefetch.threads` and a couple of nearby values; don't copy the example literally |
| `latencyMicros` | your storage's first-byte latency: `2000` ≈ cloud object store (S3/GCS/ABFS), `200` ≈ warm NVMe (timer-noisy — see caveats), `0` = CPU-bound control |
| `multiplier` | the knob under test: `1,2,4,8,16` |
| `pattern` | `SEQUENTIAL` (the representative case the decision uses). `STRIDED` is a wasted-prefetch *stressor*, not a tuning signal — do not read a knee off it |

## Run 1 — multiplier sweep (the headline)

Statistically meaningful settings (3 forks, pinned warmup). Tune the param lists to your hardware:

```bash
java -jar cached-fs-bench/target/benchmarks.jar PrefetchMultiplierBenchmark \
  -f 3 -wi 5 -w 2 -i 10 -r 2 \
  -p pattern=SEQUENTIAL \
  -p latencyMicros=2000 \
  -p prefetchThreads=8,16,32 \
  -p multiplier=1,2,4,8,16 \
  -rf json -rff prefetch-sweep.json 2>&1 | tee prefetch-sweep.txt
```

The `| tee prefetch-sweep.txt` keeps the human-readable summary table (with the `±Error` column)
that `-rff` does **not** write — capture it on the first run so you don't have to repeat the ~25 min.

- Mode is `AverageTime` → **ms per 64 MiB scan, lower is better.**
- Always pin the params as above. Running the bare jar (`... PrefetchMultiplierBenchmark` with no
  `-p`) uses the source defaults — `latencyMicros={0,200,2000}` × `pattern={SEQUENTIAL,STRIDED}` ×
  all 5 multipliers at the single default `prefetchThreads=8` — which is a smoke run, **not** the
  decision sweep.
- Add `-p latencyMicros=200,2000` to cover NVMe + cloud in one run (doubles the matrix; ~50 min).
- **Time (≈25 min at 2 ms latency, treat as a floor):** 1 pattern × 1 latency × 3 threads × 5
  multipliers = 15 combos. Each fork runs 5 warmup + 10 measurement iters at ~2 s each = 30 s; 3
  forks/combo = 90 s; 15 combos ≈ **22.5 min** of iteration time, plus ~3 min of fork-JVM startup →
  **~25 min**. This is a floor (and scales with latency / the matrix size): a single cold scan at
  high latency / low multiplier can exceed the 2 s window, so JMH runs one long op per iteration and
  real time drifts higher — that's normal, not a hang.
- **GC check (recommended, optional):** the per-scan clear makes a sawtooth allocation profile, so
  add `-prof gc` to the headline command so allocation/GC time is reported per combo. If
  `gc.alloc.rate` or pause time
  varies wildly across forks, GC is contaminating the result — raise `-jvmArgsAppend
  "-XX:MaxDirectMemorySize=4g"` (and `-Xmx` if heap GC is the culprit) and re-run. (Treat that,
  alongside a large `Error`, as "box too noisy".)
- **Quick rig-smoke only** (do NOT decide a knee from it — 2 forks give error bars too wide to
  trust): `-f 2 -wi 3 -w 1 -i 5 -r 1` (~6–8 min). Note the explicit `-w 1`: if you drop `-w`, JMH
  falls back to its 10 s default warmup time and the "quick" run is actually *slower* in warmup than
  the headline.
- Optional control: a separate `-p latencyMicros=0` run should show the multiplier is irrelevant
  (nothing to hide) — a confidence check that the rig behaves.

## Run 2 — gate contention micro (sanity)

```bash
for t in 1 8 32 64; do   # span 1 → your deployment's prefetch-thread count (not the example literally)
  java -jar cached-fs-bench/target/benchmarks.jar AdmissionGateBenchmark \
    -f 3 -wi 5 -w 2 -i 10 -r 2 -t $t -p budgetMiB=64 \
    -rf json -rff gate-admit-t$t.json 2>&1 | tee gate-admit-t$t.txt
done
```

The per-`$t` `tee gate-admit-t$t.txt` mirrors the `-rff gate-admit-t$t.json` name so each thread
count's table is captured without overwriting the previous one. **Time (≈6–8 min):** `-p
budgetMiB=64` pins one combo per thread count, so 4 thread counts × 3 forks × (5 + 10) iters × 2 s =
360 s ≈ **6 min** of iteration, plus ~1 min of fork-JVM startup; like Run 1 this is a floor, not a
hang.

`-t` overrides the benchmark's `@Threads(8)` annotation. The `-p budgetMiB=64` pins the admit path,
so each file has just that one row (the source default also carries a `budgetMiB=0` reject-path
control, not needed for this sanity check); throughput is `ops/µs`. A **decline** as
threads rise is expected and fine — `LongAdder.sum()` scans more cells with more threads. What
matters is that the floor stays orders of magnitude above the real admission rate: at your highest
thread count expect roughly the reference shape below (and at minimum **~1 ops/µs ≈ 1 million gate
decisions/sec**) — already ~1000× above any real prefetch admission rate (bounded by chunk-load
latency, i.e. thousands/sec at most). If it holds there, the gate is not the bottleneck and raising
the multiplier costs memory headroom, not gate CPU. (This micro isolates only the gate's `LongAdder`
sum/increment/decrement — **not** the surrounding prefetch scheduling — so read it as a floor on
gate-accounting cost, not an end-to-end admission budget.) (Reference dev-machine shape: ~112 ops/µs
at 1 thread settling to ~24 ops/µs at 8–32 — a decline, but a floor ~5 orders of magnitude above the
real rate.)

## Record (per machine)

- **Git commit SHA** (`git rev-parse HEAD`) — so the results are traceable to the exact code.
- CPU model, **physical** cores / logical threads, RAM, OS/kernel, storage type.
- `java -version`; GC in use; any `-Xmx` / `-jvmArgsAppend` you set (and confirm the run used JDK 21).
- Frequency scaling: governor/turbo state from **Quiesce the box**.
- The JMH JSON files (`-rff ...`) **and** the printed summary tables (which include the `±Error`
  column) — the `*.txt` files the `| tee` in the Run 1 / Run 2 commands already captured (`-rff`
  writes only the JSON; the human-readable table goes to stdout).
- Whether the box was otherwise idle.

## How to read it / decision rule

JMH prints one row per combo. `Score` is the mean (ms/op here; ops/µs for Run 2), `Error` is the
±99.9% confidence half-width (`scoreError` in the JSON), and the parenthesised columns are the
params:

```
Benchmark             (latencyMicros) (multiplier) (pattern)  (prefetchThreads)  Mode  Cnt   Score    Error  Units
…scan                            2000            2 SEQUENTIAL                  8  avgt   30  208.4 ±   6.1  ms/op   <- this Score / this Error
…scan                            2000            4 SEQUENTIAL                  8  avgt   30  211.0 ±   5.8  ms/op
```

(Worked through the rule below: those two intervals — `[202.3, 214.5]` and `[205.2, 216.8]` —
overlap, so 2 and 4 are *not clearly different*; the knee is the smaller, **2**, but 4 is within
noise of it, so the action is still **keep 4**. Don't read "knee = 2" as "lower the default.")

**Decide only from the `latencyMicros=2000` (2 ms) slices** — the 200 µs and 0 slices are
timer-noise-dominated (see caveats) and are NOT decision-grade even though 200 µs maps to NVMe; a
real NVMe-specific tune needs a real-I/O benchmark, not this rig.

For each `(prefetchThreads, 2000)` slice, sort by `Score` and find the minimum, then use `Error` to
judge ties:

- Two multipliers are **not clearly different** if their `[Score − Error, Score + Error]` intervals
  overlap. (Note this is a *conservative* tie test — overlapping CIs can still hide a real
  difference; it errs toward declaring ties, hence toward the smaller multiplier. That bias is
  *aligned* with our goal of preferring the smallest budget that performs, so it's acceptable here.)
- The **knee** = the smallest multiplier whose interval overlaps the minimum's. Smaller budget at
  equal performance wins (less memory, less churn).
- If `Error` is large relative to the gaps between multipliers (common with too few forks), the data
  can't resolve a knee — add forks (`-f 5`) and re-run rather than guessing.
- Note the reference point (`min`) is a *selected* extremum — having won a 5-way race partly on
  noise, its `Score` is biased slightly low, so its CI is mildly optimistic. Combined with the
  conservative overlap test the two biases roughly offset; if a decision hinges on a single
  marginal overlap, add forks rather than trust it.
- **Require the knee to replicate — don't act on one slice.** You make a dozen-plus overlap calls
  (≈5 multipliers × 3 thread counts, × 2 latencies if you ran both), so by chance alone one slice can
  show a spurious knee. Only act on a knee that **repeats across your thread counts**; treat one that
  appears in a single slice as unconfirmed and add forks / thread counts before believing it.

Then decide, symmetrically:

- **Flat within `Error`** across multipliers (the expected single-stream outcome — see **Scope &
  limits**) → **keep the default at 4** (it's harmless; this rig can't show it's optimal). Mind the
  resolving power: the rig can only separate two multipliers whose gap exceeds their **combined**
  `Error` half-widths, so if you let `Error` run up to the ~10% noise ceiling, a real knee of ≤~10%
  is *masked* as flat. "Flat" here means "no effect this rig can resolve at 3 forks," not "provably
  no effect" — tighten the box or add forks to see a smaller effect, and run the concurrent-stream
  follow-up (see **Scope & limits**) to resolve effects this single-stream rig structurally can't.
- Knee **= 4** (or `≤ 4` and 4 is within noise of the best) across your thread counts/latencies →
  **keep the default at 4.**
- Knee consistently **= 2**, with 4/8/16 clearly (beyond `Error`) worse → consider lowering the
  default to 2.
- Knee **> 4** at your higher thread counts → consider raising the default, or documenting a
  per-deployment override (`fs.cached.prefetch.max-pending-multiplier`).
- Expect (and confirm) **regression past the knee** — more budget → slower — because beyond the pool
  size the extra in-flight budget only adds prefetch churn + eviction pressure.
- **Ignore STRIDED for this decision** even if you ran it (prefetch is pure waste there, so lower
  multiplier is always nominally "better" — it tells you nothing about the right default).

> Prior data point (do **not** anchor on it — it's one dev laptop): at 8 threads / 2 ms latency the
> knee sat around 2–4 with the larger multipliers somewhat slower. Your representative hardware is
> the authority; record what *you* observe.

To change the default: edit `CachedFsConfig.DEFAULT_PREFETCH_MAX_PENDING_MULTIPLIER`, update the
README config-reference row, and adjust `CachedFsConfigTest.prefetchMaxPendingMultiplier` (it pins
the auto-default arithmetic).

## After the run

Report the results against `github.com/LuciferYang/cached-fs` — **no push access or fork needed**:
just open a GitHub issue and attach the `*.json` files (and the `tee`'d `*.txt` tables) from both
runs **plus the filled-in Record section** (git SHA, CPU/cores/RAM/OS, `java -version`, `-Xmx`,
frequency state, idle state). (If you do have push access, a PR adding the files under `docs/bench/`
works too.) The knee is uninterpretable without the core count, latency, and commit context, so the
Record section is not optional. The reviewer then folds
the result tables into `docs/bench/prefetch-multiplier.md` (replacing the dev-machine "illustrative"
section) with the verdict, and bumps `DEFAULT_PREFETCH_MAX_PENDING_MULTIPLIER` if the data warrants.

## Tuning beyond the swept params

`prefetchThreads`, `latencyMicros`, `multiplier`, and `pattern` are JMH `@Param`s — no rebuild
needed. The load quantum (1 MiB) and file size (64 MiB / 64 chunks) are constants in
`PrefetchMultiplierBenchmark`; if your deployment's quantum or typical file size differs materially,
edit `LOAD_QUANTUM` / `FILE_SIZE` and rebuild (or ask to have them parameterised too).
