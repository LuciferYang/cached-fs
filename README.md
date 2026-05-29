# cached-fs

A Java reimplementation of the [velox](https://github.com/facebookincubator/velox) file read cache: a per-process, two-tier (RAM + local SSD) read cache exposed as a transparent Hadoop FileSystem decorator.

- **Toggle with one flag**: `fs.cached.enabled=true`. URIs (`hdfs://`, `s3a://`, `abfs://`, `bos://`, `file://`, …) stay unchanged.
- **Per-executor**: embedded library, no daemon, no separate cache cluster.
- **Two-tier**: RAM (clock + sampled-percentile eviction) + local SSD (64 MiB regions, checkpoint+log recovery).
- **Multi-scheme**: one JVM can transparently cache reads from several Hadoop FileSystems at once — register one decorator per `scheme://authority` (e.g. `hdfs://nn-a`, `s3a://bucket-x`, `bos://bucket-y`) and they share the RAM/SSD tiers without disturbing each other on close.
- **Coalesced fills**: adjacent missing chunks issued by a single read are merged into one `preadv` call against the inner FS, cutting per-chunk RPC overhead on wide-column scans.
- **Async prefetch**: dense sequential readers fire an admission-gated background fill for the next chunk while still consuming the current one, hiding inner-FS latency without thrashing the cache (per-stream byte budget + heap-pressure guard + saturation back-off).
- **IO statistics surface**: every `FSDataInputStream` opens with Hadoop `IOStatisticsSource` wiring (`stream_read_*`, `cachedfs_stream_*`); bootstrap-level aggregates roll up across streams (`cachedfs_aggregate_*`) for Prometheus / dashboard scraping.
- **JDK 21+**, **Hadoop 3.4.x+**.

See [`velox-file-read-cache.md`](velox-file-read-cache.md) for the design spec.

## Configuration

Wire the decorator per scheme by setting `fs.<scheme>.impl=io.github.luciferyang.cachedfs.hadoop.CachedFileSystem` and pointing `fs.cached.inner.impl` at the original implementation class. Examples:

```properties
# AWS S3 via the s3a connector
fs.s3a.impl=io.github.luciferyang.cachedfs.hadoop.CachedFileSystem
fs.cached.inner.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
fs.cached.enabled=true

# Baidu BOS (bos://bucket.bj.bcebos.com/...) — replace the inner impl with the
# class published by whichever BOS Hadoop connector you ship. Any Hadoop
# FileSystem class works the same way.
fs.bos.impl=io.github.luciferyang.cachedfs.hadoop.CachedFileSystem
fs.cached.inner.impl=<your-bos-connector-FileSystem-class>
fs.cached.enabled=true
```

For multi-scheme caching in the same JVM, give each `FileSystem.get(uri, conf)` call its own `Configuration` whose `fs.cached.inner.impl` names the inner class for that scheme — the bootstrap's RAM/SSD tiers are shared while each decorator registers its own `scheme://authority` opener.

### Limitations

- `open(Path)`, `open(Path, int)`, and `openFile(Path).build()` all route through the cache. `openFile(PathHandle)` and `open(PathHandle, int)` route through the cache when the supplied handle implements `io.github.luciferyang.cachedfs.hadoop.spi.ContentAddressedPathHandle` (opt-in SPI exposing `contentHash()` + `contentLength()`); plain `PathHandle`s still delegate to the inner FS because their bytes are opaque.
- `applyTTL(ttlSeconds)` rejects negative values and values larger than the current epoch second.
- The `cached-fs-cli` ships static subcommands (`config`, `version`) and live-state subcommands (`inspect <key>`, `stats`, `recent-streams`, `drain --yes`, `purge <regex> --yes`) backed by the `CachedFsBootstrapMXBean` JMX MBean. Live commands default to the local platform MBean server; pass `--jmx-url <service-url>` to target a remote JVM.

### Configuration reference

All keys live under the `fs.cached.*` namespace; the first `installIfNeeded` call wins (subsequent calls are no-ops), so set them on the `Configuration` you pass to `FileSystem.get`.

| Key | Default | Description |
| --- | --- | --- |
| `fs.cached.enabled` | `false` | Master switch; when false, `open()` delegates straight to the inner FS. |
| `fs.cached.inner.impl` | — | FQCN of the wrapped FileSystem (required when enabled). |
| `fs.cached.ram.shards` | `4` | RAM-tier shard count (power of 2). Higher reduces mutex contention. |
| `fs.cached.ram.max-write-ratio` | `0.7` | Fraction of RAM the cache may use for write-side staging. |
| `fs.cached.ram.ssd-savable-ratio` | `0.125` | Fraction of new entries the SSD tier is asked to absorb. |
| `fs.cached.ram.min-ssd-savable-bytes` | `16777216` | Lower bound on a write batch before flushing to SSD. |
| `fs.cached.ram.ssd-flush-threshold-bytes` | `0` | Pending-write bytes that trigger an SSD flush; `0` defers to `min-ssd-savable-bytes`. |
| `fs.cached.ssd.paths` | — | Comma-separated mount points (omit to disable SSD tier). |
| `fs.cached.ssd.shards` | `4` | SSD-tier shard count; independent of `ssd.paths` count. |
| `fs.cached.ssd.shard-prefix` | `ssd` | Filename prefix for per-shard `.data`/`.cpt`/`.log` files. |
| `fs.cached.ssd.regions-per-shard` | `16` | 64 MiB regions per shard. |
| `fs.cached.ssd.max-entries-per-shard` | `0` | Upper bound on cached entries per shard; `0` means unbounded. |
| `fs.cached.ssd.checkpoint-interval-bytes` | `0` | Cache-wide checkpoint budget; per-shard threshold is this÷shards. `0` disables auto-checkpointing. |
| `fs.cached.ssd.checksum.enabled` | `false` | Compute payload checksums on write. |
| `fs.cached.ssd.checksum.read-verify` | `false` | Verify checksums on read (requires `checksum.enabled`). |
| `fs.cached.load-quantum-bytes` | `8388608` | Read granularity on the cached path; supersedes Hadoop's `bufferSize`. |
| `fs.cached.handle-cache-capacity` | `1024` | Open-handle LRU capacity (per JVM, shared across schemes). |
| `fs.cached.scan-id` | — | Optional scan identifier used by the per-`(scanId, file)` density tracker; resolved at `open()` time as `withScanId` ThreadLocal → this key → `"default"`. |
| `fs.cached.scan-tracker.enabled` | `true` | When false, the tracker behaves as a no-op `ScanTracker.DISABLED`; `readPct()` returns 100% and the prefetch admission gate's density predicate always passes. |
| `fs.cached.scan-tracker.max-entries-per-tracker` | `10000` | Soft cap on the distinct `fileNum`s one `ScanTracker` will admit. New fileNums past the cap silently no-op and bump `CacheBootstrap.scanTrackerEntriesRejected()` (and the `cached_fs.scan_tracker.entries_rejected` gauge). `0` or negative disables the cap. Memory cost is ~40 bytes per entry; the default fits a 10k-file scan with no rejections. |
| `fs.cached.scan-trackers.max-count` | `10000` | JVM-wide soft cap on the number of distinct `ScanTracker`s the bootstrap will retain. Past the cap, `trackerFor()` returns `ScanTracker.DISABLED` for previously-unseen scanIds and bumps `scanTrackersRejected()`. `0` disables the cap. With the per-task scanId plugin (`CachedFsScanIdPlugin`), this is the safety net against tracker leaks from crashed Spark tasks. |
| `fs.cached.recent-streams.capacity` | `64` | Capacity of the ring buffer that retains per-stream `IoStatisticsSnapshot`s pushed by `CachingInputStream.close()`. Useful for post-hoc debugging without instrumenting every reader. `0` disables the ring entirely (snapshots dropped on the floor); the `RecentStreams.DISABLED` sentinel is used in that case. |
| `fs.cached.prefetch.max-pending-multiplier` | `4` | `N` in the auto-default `loadQuantum × threads × N` for `fs.cached.prefetch.max-pending-bytes`. Lets ops tune the in-flight prefetch budget without overriding the absolute byte count. Ignored when `max-pending-bytes` is set explicitly. |
| `fs.cached.path-handle.cache-enabled` | `true` | Routes `openFile(PathHandle)` / `open(PathHandle, int)` reads through the cache when the supplied handle implements `ContentAddressedPathHandle`. Set `false` to revert to inner-FS passthrough (useful for A/B benchmarking or diagnosing connector-side `contentHash` correctness). |
| `fs.cached.jmx.enabled` | `true` | Registers a `CachedFsBootstrapMXBean` on the platform MBean server at install time so the `cached-fs-cli` ops tool + JConsole / jmxterm can inspect and manage a running JVM. Set `false` to disable JMX exposure (some sandboxed deployments forbid MBean registration). |
| `fs.cached.jmx.object-name` | `io.github.luciferyang.cachedfs:type=CachedFsBootstrap` | JMX ObjectName for the bean. Override only when running multiple isolated bootstraps in one process (rare). |
| `fs.cached.metrics.enabled` | `true` | When false, `IoStatistics.NO_OP` is wired into every stream — counter bumps short-circuit and the Hadoop `IOStatistics` surface returns zeros. |
| `fs.cached.coalesce.enabled` | `true` | Coalesce contiguous missing chunks issued by a single read into one `preadv` call against the inner FS. |
| `fs.cached.coalesce.always-on` | `false` | Bypass the auto-disable rule (which turns coalesce off on caches smaller than 8×`loadQuantum`). Set to `true` for unit / IT scenarios that use tiny caches. |
| `fs.cached.coalesce.max-gap-bytes` | `loadQuantum/16` | Maximum gap between two missing chunks that the coalescer may bridge by overreading (counted into `cachedfs_*_raw_overread_bytes`). |
| `fs.cached.coalesce.max-chunks-per-group` | `16` | Upper bound on chunks coalesced into a single `preadv`. Caps tail-latency from one inner-FS hiccup. |
| `fs.cached.coalesce.max-restarts` | `3` | How many `Waiting`-driven walk restarts the coalesce path will attempt before falling back to the per-chunk loop. |
| `fs.cached.prefetch.enabled` | `true` | Master toggle for the async prefetch path. When false, every bump-site is a no-op regardless of state. |
| `fs.cached.prefetch.threads` | `availableProcessors()` | Worker count for the prefetch thread pool. Daemon threads named `cached-fs-prefetch-N`. |
| `fs.cached.prefetch.queue` | `64` | Bounded queue capacity. Rejection routes through `DiscardAndCountHandler` (counts `prefetchSkipped("queue_full")` and arms the per-stream back-off). |
| `fs.cached.prefetch.heap-pressure-check.enabled` | `true` | When true, the admission gate consults `MemoryMXBean.getHeapMemoryUsage()` and rejects new prefetches once heap is at or above the JVM's soft target. |
| `fs.cached.prefetch.heap-pressure-ttl-ms` | `100` | TTL for the cached heap-pressure bit; single-CAS-winner refresh bounds the MBean call rate under contention. |
| `fs.cached.prefetch.rejection-backoff-ms` | `100` | Per-stream cooldown after a queue rejection. Cold-start sentinel `Long.MIN_VALUE/2` lets the first attempt fire unconditionally. |
| `fs.cached.prefetch.trigger-tail-fraction` | `0.5` | Fraction of the current chunk that must be consumed before the next-chunk prefetch is even considered. |
| `fs.cached.prefetch.density-threshold-pct` | `80` | Minimum `tracker.readPct()` for the prefetch to fire. Lower density bumps `prefetchEligibleSuppressed` instead. |
| `fs.cached.prefetch.max-pending-bytes` | `loadQuantum × threads × 4` | JVM-wide byte budget for in-flight prefetch tasks. Over-budget admissions bump `prefetchSkipped("budget")`. |

## TTL (cache aging)

The cache tracks the open-time of every file it's been asked to serve and exposes an externally-driven aging knob via `CacheTTLController`. No background thread runs inside cached-fs — the embedding application invokes `applyTTL(seconds)` on its own schedule (compliance age-out, invalidation of files known to have been replaced upstream, etc.).

```java
import io.github.luciferyang.cachedfs.hadoop.CacheBootstrap;
import io.github.luciferyang.cachedfs.core.ttl.CacheTTLController;

CacheTTLController ttl = CacheBootstrap.get().orElseThrow().ttlController();

// Drop every cache entry whose owning file was first opened more than 1 hour ago.
// Returns the count of files for which neither tier retained any pin — files still
// pinned in EITHER tier are reported back internally and retried on the next cycle.
int dropped = ttl.applyTTL(3600);
```

Two-tier removal calls RAM and SSD independently with the full set of aged-out files — matching velox, the SSD fan-out is NOT gated on RAM retention so a long-lived shared pin in RAM does not extend the SSD copy's lifetime. Pinned entries (in either tier) are reported back and retried on the next cycle. A reader that re-opens a file mid-TTL refreshes the tracking entry so freshly-loaded cache bytes are not silently lost on the cycle's cleanup. `numAgedOut` in `AsyncDataCache.refreshStats()` is the RAM-tier counter for entries dropped via `removeFileEntries` — applicable to TTL but also any other caller of that API; it does NOT count SSD-side drops.

Call `applyTTL` no more than once per minute in production: each invocation scans every RAM shard under its mutex and every SSD region, so frequent calls starve readers.

## Observability

Every `FSDataInputStream` returned by the decorator is an `IOStatisticsSource`. The bootstrap itself also implements `IOStatisticsSource` via `CacheBootstrap.get().orElseThrow().getIOStatistics()` — aggregate totals merged from each stream's `close()` plus a handful of live registry gauges.

Per-stream counters (subset; full list in the `IoStatisticsAdapter` javadoc):

- `stream_read_operations`, `stream_read_bytes` — Hadoop-standard read counters.
- `cachedfs_stream_cache_hit{,_bytes}` — RAM cache hits.
- `stream_read_prefetch_operations`, `cachedfs_stream_prefetched_bytes` — completed prefetch fills.
- `cachedfs_stream_ssd_read_{operations,bytes}` — SSD-tier reads.
- `cachedfs_stream_raw_overread_bytes` — gap bytes the coalescer absorbed between non-adjacent misses.
- `cachedfs_stream_prefetch_skipped_{queue_full,budget,heap_pressure,other}_bytes` — per-reason admission-gate rejections.
- `cachedfs_stream_prefetch_eligible_suppressed_bytes` — admission would have passed but density was below threshold.
- `cachedfs_stream_seq_hwm_regime_resets` — sequential trajectory broke and the HWM CAS-loop reset.

Bootstrap aggregate counters mirror these with the `cachedfs_aggregate_*` prefix and `_operations` suffix on event counts (e.g. `cachedfs_aggregate_read_operations`, `cachedfs_aggregate_prefetch_skipped_budget_bytes`). Additional bootstrap-only signals:

- `cachedfs_stale_scan_id_recoveries` — count of `withScanId` calls that found a leftover slot from a crashed prior task.
- `cachedfs_scan_tracker_count`, `cachedfs_scan_tracker_entries`, `cachedfs_scan_tracker_max_entries` — registry-snapshot gauges.
- `cachedfs_pending_prefetch_bytes` — live in-flight prefetch byte budget.

Disable the surface entirely with `fs.cached.metrics.enabled=false`; per-stream `IoStatistics` becomes `NO_OP` and every counter reads zero.

### Micrometer export (cached-fs-metrics)

The optional `cached-fs-metrics` module ships a `CachedFsMeterBinder` that registers every aggregate counter as a `FunctionCounter` and every bootstrap-level gauge (scan-tracker entries, pending prefetch bytes) as a `Gauge` under the `cached_fs.*` namespace. Bring your own `MeterRegistry` (Prometheus, JMX, statsd):

```java
CacheBootstrap b = CacheBootstrap.get().orElseThrow();
CachedFsMeterBinder.builder(b.aggregateIoStats())
    .scanTrackerEntries(b::scanTrackerEntries)
    .scanTrackerMaxEntries(b::scanTrackerMaxEntries)
    .pendingPrefetchBytes(AsyncDataCache.getInstance()::pendingPrefetchBytes)
    .maxPendingPrefetchBytes(b::maxPendingPrefetchBytes)
    .build()
    .bindTo(registry);
```

Prometheus exporters rewrite dots to underscores (e.g. `cached_fs_read_total`). Reason-tagged counters (`cached_fs.prefetch.skipped.bytes{reason=...}`) have a fixed reason set (`queue_full`, `budget`, `heap_pressure`, `other`) so tag cardinality stays bounded.

## Modules

| Module | Purpose |
| --- | --- |
| `cached-fs-core` | Pure cache library. Mirrors `velox/common/caching/`. No Hadoop dep. |
| `cached-fs-hadoop` | Transparent Hadoop FileSystem decorator. |
| `cached-fs-spark` | Spark soft-affinity scheduling (consistent-hash + feedback-driven duplicate-reading detect). Targets Spark 4.0+ / Scala 2.13 / JDK 21. |
| `cached-fs-metrics` | JMX MBeans + Micrometer/Prometheus exporters. Optional. |
| `cached-fs-cli` | Operations tooling. |
| `cached-fs-bench` | JMH microbenchmarks (prefetch multiplier tuning, admission-gate contention). Run on-demand via `java -jar`; see `docs/bench/`. |

## Spark soft-affinity (cached-fs-spark)

When the cached-fs decorator is wired into a Spark cluster, repeated reads of the same file ideally land on the executor whose cached-fs cache already holds it. The `cached-fs-spark` module adds a driver-side soft-affinity layer that hints Spark's DAGScheduler with the right executor for each file. Drawn from Apache Gluten + lance-spark-zb.

Two cooperating modes:

- **Static (consistent-hash, default on with `enabled=true`):** the file's qualified path is hashed against a consistent-hash ring of live executors. Deterministic, no feedback required, fires on the first task. Replication > 1 produces N fault-tolerant candidates per file.
- **Feedback (`duplicate-reading-detect.enabled=true`):** a SparkListener records the actual `(executor, host)` that ran each FilePartition split (keyed by `path_start_length`). Subsequent reads of the same split prefer the observed executor, overriding the consistent-hash result.

Enable via `spark.sql.extensions` + a handful of `spark.cached-fs.affinity.*` keys:

```properties
spark.sql.extensions=io.github.luciferyang.cachedfs.spark.CachedFsAffinityExtension
spark.cached-fs.affinity.enabled=true
spark.cached-fs.affinity.replication-num=2
spark.cached-fs.affinity.min-target-hosts=1
spark.cached-fs.affinity.virtual-nodes=100
spark.cached-fs.affinity.duplicate-reading-detect.enabled=false
spark.cached-fs.affinity.duplicate-reading.max-cache-items=10000
```

Notes:

- `replication-num` larger than 3 has no effect — Spark caps `FilePartition.preferredLocations` at 3 entries per file.
- Feedback mode (`duplicate-reading-detect.enabled=true`) requires a custom DataSource v2 connector to call `CachedFsAffinity.recordPartitionMap(rddId, splits)` from its scan-planning code. Spark's built-in `FileSourceScanExec` does not expose its planned partition mapping to listeners, so the static (consistent-hash) mode is what runs with Spark's stock Parquet/ORC readers.

The extension installs a SparkListener (executor lifecycle + per-stage task-end events for feedback mode) and a `BlockLocationsProvider` that rewrites `CachedFileSystem.getFileBlockLocations` to return `executor_<host>_<execId>` strings — recognized by Spark as `ExecutorCacheTaskLocation` for PROCESS_LOCAL scheduling. When HDFS native locality already covers `min-target-hosts` matching executors, the hint is suppressed so soft affinity does not stomp better locality.

`CachedFileSystem` also overrides `listLocatedStatus` + `listFiles` so Spark 4's s3a fast-path (default `spark.sql.sources.useListFilesFileSystemList="s3a"` → `HadoopFSUtils.listFiles`) picks up the same hint. S3A's `S3ALocatedFileStatus` and ABFS's `AbfsLocatedFileStatus` are reflectively rebuilt with the rewritten block-locations so the affinity hint AND the `EtagSource` metadata Hadoop's `ManifestCommitter` consumes are both preserved. HDFS `LocatedFileStatus` subclasses pass through unchanged to preserve `FileEncryptionInfo` + erasure-coding policy.

For custom DataSource v2 connectors that own their own scan, the same hint is available as a public API: `CachedFsAffinity.getPreferredLocations(path, nativeHosts)` returns the executor location strings directly.

### Per-task scanId (cached-fs-spark)

`cached-fs-spark` also ships a Spark plugin that produces per-task scanIds so concurrent queries on the same JVM no longer collapse to the `"default"` scanId. Wire it on the driver:

```properties
spark.plugins=io.github.luciferyang.cachedfs.spark.CachedFsScanIdPlugin
```

The executor plugin opens a `withScanId("task-{stageId}-{partitionId}-{taskAttemptId}")` scope at `onTaskStart` and closes it at `onTaskSucceeded`/`onTaskFailed`. Each Spark task gets its own `ScanTracker`, so density-tracking for one query never contaminates another's, and retries get fresh trackers (the taskAttemptId rolls forward). Reads issued from a thread other than the task-runner thread (e.g. a `ForkJoinPool` task spawned inside the task body) won't see the scope and fall back to the existing `fs.cached.scan-id` resolution chain. Compose with other plugins by passing a comma-separated list.

## Build

```sh
mvn -DskipTests package
```

## License

Apache License 2.0.
