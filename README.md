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

- Only the legacy `open(Path)` / `open(Path, int)` reader entry points route through the cache. The modern `openFile(Path)` builder API and `open(PathHandle, ...)` paths delegate to the inner FS unchanged. Configure your reader (Spark, Iceberg, Parquet, etc.) to call the legacy `open` if you want cache hits.
- `applyTTL(ttlSeconds)` rejects negative values and values larger than the current epoch second.
- The `cached-fs-metrics` module is not yet shipped; depending on it today gets an empty jar.

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

## Modules

| Module | Purpose |
| --- | --- |
| `cached-fs-core` | Pure cache library. Mirrors `velox/common/caching/`. No Hadoop dep. |
| `cached-fs-hadoop` | Transparent Hadoop FileSystem decorator. |
| `cached-fs-metrics` | JMX MBeans + Micrometer/Prometheus exporters. Optional. |
| `cached-fs-cli` | Operations tooling. |

## Build

```sh
mvn -DskipTests package
```

## License

Apache License 2.0.
