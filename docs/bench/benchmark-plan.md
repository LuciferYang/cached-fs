# Prefetch-multiplier benchmark plan (R1.1)

A self-contained plan to run the cached-fs JMH benchmarks on representative hardware and decide the
right default for `fs.cached.prefetch.max-pending-multiplier`. Run it on a quiet box that resembles
your deployment, capture the JSON, and paste results back into `prefetch-multiplier.md`.

## Objective

1. **Headline:** find the multiplier that minimises cold-scan time for your storage latency and
   prefetch-thread count — confirm or revise the current default of **4**
   (`CachedFsConfig.DEFAULT_PREFETCH_MAX_PENDING_MULTIPLIER`).
2. **Sanity:** confirm the admission gate's `LongAdder` accounting is not a contention bottleneck at
   your thread counts.

## What's being measured (and what isn't)

The budget is `loadQuantum × prefetchThreads × multiplier`. A bigger budget keeps more chunk loads
in flight, hiding more per-read latency — until the prefetch **thread pool**, not the budget, caps
concurrency. So the optimal multiplier is **thread-dependent**; that's why `prefetchThreads` is a
swept parameter.

The backing store is synthetic (`InMemoryLatencyFileSystem`): no disk/network, a fixed first-byte
latency parked once per chunk load (stride-aware — see `prefetch-multiplier.md`). This isolates the
multiplier's latency-hiding effect and is fully reproducible. It deliberately does **not** model
storage bandwidth (per-byte transfer) or multi-query contention, so absolute ms won't match real
I/O — the **shape** (where the knee is, whether high multipliers regress) is the durable signal.

## Prerequisites

- **JDK 21** (the project target). JMH forks a child JVM that must also be 21 — if your default
  `java` is older/newer, pass `-jvm "$JAVA_HOME/bin/java"` on every command.
- A **quiet machine**, ideally matching production: same physical core count, and you know your
  deployment's `fs.cached.prefetch.threads` and storage first-byte latency.

## Setup

```bash
git clone <repo> && cd cached-fs
export JAVA_HOME=/path/to/jdk21
mvn -q -pl cached-fs-bench -am package -DskipTests
# produces cached-fs-bench/target/benchmarks.jar (runnable)
java -jar cached-fs-bench/target/benchmarks.jar -l   # sanity: lists both benchmarks
```

Then record the machine spec (see **Record** below) before running.

## Pick your parameters

| Param | What to set it to |
| --- | --- |
| `prefetchThreads` | your deployment's prefetch pool size, plus neighbours — e.g. `8,16,32` |
| `latencyMicros` | your storage's first-byte latency: `200` ≈ warm NVMe, `2000` ≈ cloud object store (S3/GCS/ABFS); add your measured p50 if known. `0` is a CPU-bound control. |
| `multiplier` | the knob under test: `1,2,4,8,16` |
| `pattern` | `SEQUENTIAL` (representative). `STRIDED` is a wasted-prefetch stressor — run only if relevant. |

## Run 1 — multiplier sweep (the headline)

Statistically meaningful settings (3 forks, real warmup). Tune the param lists to your hardware:

```bash
java -jar cached-fs-bench/target/benchmarks.jar PrefetchMultiplierBenchmark \
  -f 3 -wi 5 -w 2 -i 10 -r 2 \
  -p pattern=SEQUENTIAL \
  -p latencyMicros=2000 \
  -p prefetchThreads=8,16,32 \
  -p multiplier=1,2,4,8,16 \
  -rf json -rff prefetch-sweep.json
```

- Mode is `AverageTime` → **ms per 64 MiB scan, lower is better.**
- Add `-p latencyMicros=200,2000` to cover NVMe + cloud in one run (doubles the matrix).
- Matrix here = 1 pattern × 1 latency × 3 threads × 5 multipliers = 15 combos; each ≈ 3 forks ×
  (5+10) × ~2 s + fork overhead → budget **~45 min**. Trim with `-f 2 -wi 3 -i 5 -r 1` for a quick
  look (~15 min), then a full run for the decision.
- Optional control: a separate `-p latencyMicros=0` run should show the multiplier is irrelevant
  (nothing to hide) — a good confidence check that the rig behaves.

## Run 2 — gate contention micro (sanity)

```bash
for t in 1 8 32 64; do
  java -jar cached-fs-bench/target/benchmarks.jar AdmissionGateBenchmark \
    -f 3 -wi 5 -i 10 -r 2 -t $t -p budgetMiB=64 \
    -rf json -rff gate-admit-t$t.json
done
```

Throughput (ops/µs) should stay 4–5 orders of magnitude above your real prefetch admission rate
(which is bounded by chunk-load latency, i.e. thousands/sec at most). If it doesn't collapse with
thread count, the gate is not the bottleneck — raising the multiplier costs memory headroom, not
gate CPU.

## Record (per machine)

- CPU model, **physical** cores / logical threads, RAM, OS/kernel, storage type.
- `java -version`; note GC and any non-default `-Xmx` (default heap is fine).
- The JMH JSON files (`-rff ...`) and the printed summary tables.
- Whether the box was otherwise idle.

## How to read it / decision rule

For each `(prefetchThreads, latencyMicros)`, find the multiplier minimising ms/op, then the **knee**
= the smallest multiplier within run-to-run noise of that minimum:

- **Knee ≤ 4 across your thread counts/latencies → keep the default at 4.**
- Knee consistently at **2** with 4/8/16 flat-or-worse → consider lowering the default to 2.
- Knee **> 4** at your higher thread counts → consider raising the default, or documenting a
  per-deployment override (`fs.cached.prefetch.max-pending-multiplier`).
- Watch for **regression** past the knee (more budget → slower): expected, because beyond the pool
  size the extra in-flight budget only adds prefetch churn + eviction pressure.

Reference (illustrative dev-machine run, 8 threads, 2 ms latency): knee at 2–4, multipliers 8/16
~10 % slower. Your representative hardware refines this.

To change the default: edit `CachedFsConfig.DEFAULT_PREFETCH_MAX_PENDING_MULTIPLIER`, update the
README config-reference row, and adjust `CachedFsConfigTest.prefetchMaxPendingMultiplier` (it pins
the auto-default arithmetic).

## After the run

Paste the result tables into `docs/bench/prefetch-multiplier.md` (replacing the dev-machine
"illustrative" section) and record the verdict. Send the JSON back and I'll update the docs and bump
the default if the data warrants it.

## Tuning beyond the swept params

`prefetchThreads`, `latencyMicros`, `multiplier`, and `pattern` are JMH `@Param`s — no rebuild
needed. The load quantum (1 MiB) and file size (64 MiB / 64 chunks) are constants in
`PrefetchMultiplierBenchmark`; if your deployment's quantum or typical file size differs materially,
edit `LOAD_QUANTUM` / `FILE_SIZE` and rebuild (or ask to have them parameterised too).
