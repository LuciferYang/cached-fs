# Prefetch admission multiplier — benchmark & tuning (R1.1)

The prefetch admission gate caps in-flight prefetch bytes at

```
maxPendingPrefetchBytes = loadQuantumBytes × prefetchThreads × multiplier
```

where `multiplier` is `fs.cached.prefetch.max-pending-multiplier` (default **4**). R1 made the
multiplier configurable; R1.1 — this doc + the `cached-fs-bench` module — measures whether 4 is the
right default and how the knob behaves across workloads.

## What the multiplier actually trades

- **Benefit (latency hiding):** a bigger budget lets more chunk loads stay outstanding, so more of
  the backing store's per-read latency is overlapped. This only matters when reads are slow —
  against a warm page cache there is nothing to hide.
- **Ceiling:** concurrency is ultimately capped by `prefetchThreads`, not the budget. Once the
  budget admits more chunks than threads can load in parallel, raising it further buys nothing for a
  steady sequential scan; the extra headroom only helps bursty / strided patterns that need
  prefetch to run further ahead.
- **Cost:** every admitted prefetch increments/decrements a `LongAdder` and every gate decision
  reads its `sum()`. `AdmissionGateBenchmark` measures that this stays cheap even at high thread
  counts, so the cost side of raising the multiplier is memory headroom, not CPU contention.

## Running

```bash
# Build the shaded benchmark jar (also builds upstream modules):
mvn -q -pl cached-fs-bench -am package

# Full multiplier × latency × pattern sweep (slow — minutes):
java -jar cached-fs-bench/target/benchmarks.jar PrefetchMultiplierBenchmark

# Focused run (one latency, one pattern, three multipliers, short iterations):
java -jar cached-fs-bench/target/benchmarks.jar PrefetchMultiplierBenchmark \
  -p latencyMicros=2000 -p pattern=SEQUENTIAL -p multiplier=1,4,16 \
  -f 1 -wi 2 -i 3 -w 2 -r 2

# Gate accounting cost under contention (sweep thread count with -t):
java -jar cached-fs-bench/target/benchmarks.jar AdmissionGateBenchmark -t 1
java -jar cached-fs-bench/target/benchmarks.jar AdmissionGateBenchmark -t 8
java -jar cached-fs-bench/target/benchmarks.jar AdmissionGateBenchmark -t 32
```

The benchmark module is in the reactor (it compiles on every build and the harness is guarded by
`BenchHarnessSmokeTest`), but the benchmarks themselves never run during `mvn verify` — only when
you invoke the jar.

### Parameters

| Param | Values | Meaning |
| --- | --- | --- |
| `multiplier` | 1, 2, 4, 8, 16 | the knob under test |
| `latencyMicros` | 0, 200, 2000 | per-read backing-store latency: 0 = CPU/cache-bound baseline, 200 µs ≈ warm NVMe, 2000 µs ≈ cloud object-store first byte |
| `pattern` | SEQUENTIAL, STRIDED | consumer access pattern |

Fixed: 1 MiB load quantum, 64 MiB file (64 chunks), 8 prefetch threads. `PrefetchMultiplierBenchmark`
reports average ms per full-file scan (lower is better).

### Latency model

The cache fills each 1 MiB quantum as ~256 separate 4 KiB page reads (`CacheEntry` stores large
entries as `PAGE_SIZE` pages, and `HadoopReadFile.preadv` issues one positioned read per page). The
synthetic store charges the `readLatencyNanos` first-byte park **only on the leading page of each
chunk** (`latencyStrideBytes` = load quantum), not on all 256 — because against real object storage
only the first read into a region pays the round-trip; the rest stream from the open connection's
read-ahead. So one chunk load ≈ one round-trip, which is what prefetch overlaps. Charging every page
read instead (set `latencyStrideBytes = 0`) models a pathological "every page is a cold round-trip"
backend and inflates scan time ~256×; it is available as a worst-case sensitivity knob but is not
the representative default.

## Results

The numbers below are an **illustrative dev-machine run** (Apple Silicon, macOS, JDK 21), short
settings (`-wi 1 -i 2`), captured to validate the harness and read the shape of the curve. They are
NOT authoritative — re-run on representative target hardware with longer iterations
(`-wi 5 -i 10 -f 2`) and a quiet machine before making a production call. The *shape* (where the
knee is, whether high multipliers regress) is the durable signal; absolute ms are machine-specific.

### `PrefetchMultiplierBenchmark` — ms per 64 MiB scan (`AverageTime`, lower is better)

| latencyMicros | pattern | mult=1 | mult=2 | mult=4 | mult=8 | mult=16 |
| --- | --- | --- | --- | --- | --- | --- |
| 2000 | SEQUENTIAL | 323 | 309 | 326 | 354 | 357 |
| 2000 | STRIDED | 176 | 171 | 174 | 183 | 185 |

(0 µs baseline cold single-shot ≈ 71 ms — at zero latency the multiplier is irrelevant, as expected:
nothing to overlap.)

### `AdmissionGateBenchmark` — gate decisions throughput (higher is better)

| threads | ops/µs (aggregate) |
| --- | --- |
| 1 | 112 |
| 8 | 24 |
| 32 | 25 |

## Verdict (preliminary, from the run above)

- **The knee is at multiplier 2–4; 8 and 16 regress ~10%.** A bigger budget does not buy more
  latency hiding because real concurrency is capped by the 8-thread prefetch pool — once the budget
  admits more than the pool can load in parallel, the extra headroom only adds prefetch churn and
  eviction pressure, mildly hurting the scan.
- **The default of 4 is defensible as-is.** It sits at/just past the knee on the safe side; the data
  does not justify raising it, and arguably 2 is marginally better on this workload. Lowering the
  default to 2 is a possible follow-up but the gain is within noise here — not worth changing
  without a target-hardware run confirming it.
- **STRIDED tracks SEQUENTIAL** (~half the time for half the chunks) — no separate strided knee that
  would argue for a workload-specific override.
- **The gate is never the bottleneck.** Aggregate `admitAndAccount` throughput falls from 112 ops/µs
  (1 thread) to ~24 ops/µs (8–32 threads) as `LongAdder.sum()` scans more cells — real contention,
  but a floor of ~24 million gate decisions/sec is ~4–5 orders of magnitude above the actual
  prefetch admission rate (bounded by chunk-load latency). So raising the multiplier costs memory
  headroom, never CPU on the gate.

The default lives in `CachedFsConfig.DEFAULT_PREFETCH_MAX_PENDING_MULTIPLIER`; change it there (and
the README config-reference row) if a target-hardware run warrants. As of this harness landing, no
change is made — the knee analysis supports keeping 4.
