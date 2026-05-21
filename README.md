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
