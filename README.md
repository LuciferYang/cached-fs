# cached-fs

A Java reimplementation of the [velox](https://github.com/facebookincubator/velox) file read cache: a per-process, two-tier (RAM + local SSD) read cache exposed as a transparent Hadoop FileSystem decorator.

- **Toggle with one flag**: `fs.cached.enabled=true`. URIs (`hdfs://`, `s3a://`, `abfs://`, `bos://`, `file://`, …) stay unchanged.
- **Per-executor**: embedded library, no daemon, no separate cache cluster.
- **Two-tier**: RAM (clock + sampled-percentile eviction) + local SSD (64 MiB regions, checkpoint+log recovery).
- **Multi-scheme**: one JVM can transparently cache reads from several Hadoop FileSystems at once — register one decorator per `scheme://authority` (e.g. `hdfs://nn-a`, `s3a://bucket-x`, `bos://bucket-y`) and they share the RAM/SSD tiers without disturbing each other on close.
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
| `fs.cached.ssd.paths` | — | Comma-separated mount points (omit to disable SSD tier). |
| `fs.cached.ssd.shards` | `4` | SSD-tier shard count; independent of `ssd.paths` count. |
| `fs.cached.ssd.shard-prefix` | `ssd` | Filename prefix for per-shard `.data`/`.cpt`/`.log` files. |
| `fs.cached.ssd.regions-per-shard` | `16` | 64 MiB regions per shard. |
| `fs.cached.ssd.max-entries-per-shard` | `0` | Upper bound on cached entries per shard; `0` means unbounded. |
| `fs.cached.ssd.checksum.enabled` | `false` | Compute payload checksums on write. |
| `fs.cached.ssd.checksum.read-verify` | `false` | Verify checksums on read (requires `checksum.enabled`). |
| `fs.cached.load-quantum-bytes` | `8388608` | Read granularity on the cached path; supersedes Hadoop's `bufferSize`. |
| `fs.cached.handle-cache-capacity` | `1024` | Open-handle LRU capacity (per JVM, shared across schemes). |

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
