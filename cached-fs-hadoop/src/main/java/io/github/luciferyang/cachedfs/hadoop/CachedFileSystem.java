/*
 * Copyright (c) 2026 The cached-fs Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.luciferyang.cachedfs.hadoop;

import io.github.luciferyang.cachedfs.core.handle.CachedFactory;
import io.github.luciferyang.cachedfs.core.handle.FileHandle;
import io.github.luciferyang.cachedfs.core.id.StringIdLease;
import io.github.luciferyang.cachedfs.core.id.StringIdMap;
import io.github.luciferyang.cachedfs.core.io.ReadFile;
import io.github.luciferyang.cachedfs.core.stats.IoStatistics;
import io.github.luciferyang.cachedfs.core.tracker.ScanTracker;
import io.github.luciferyang.cachedfs.core.tracker.TrackingId;
import io.github.luciferyang.cachedfs.core.util.Murmur3;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Transparent Hadoop {@link FileSystem} decorator that routes reads through the cached-fs cache
 * tiers. All non-read operations (create, rename, delete, listStatus, getFileStatus, …) inherit
 * unchanged from {@link FilterFileSystem} and delegate straight to the wrapped inner FS.
 *
 * <p><b>Wiring:</b> set {@code
 * fs.<scheme>.impl=io.github.luciferyang.cachedfs.hadoop.CachedFileSystem} plus {@code
 * fs.cached.inner.impl=<original-impl-class>}. The decorator instantiates the inner FS privately so
 * consumers that resolve the scheme via {@link FileSystem#get(URI, Configuration)} receive the
 * decorator, not the inner.
 *
 * <p><b>Toggle:</b> {@code fs.cached.enabled=false} makes {@link #open} delegate straight to the
 * inner without consulting the cache — useful for A/B benchmarking without redeploying.
 *
 * <p><b>Multi-scheme:</b> each decorator instance registers a {@link CacheBootstrap.HandleOpener}
 * keyed by {@code scheme://authority}, so a single JVM can transparently cache reads from {@code
 * hdfs://nn-a}, {@code s3a://bucket-x}, {@code bos://bucket-y}, and any other Hadoop {@link
 * FileSystem} side by side. Closing one decorator unregisters its endpoint and drains only its own
 * handles; sibling decorators keep their entries.
 *
 * <p><b>Cached vs passthrough APIs:</b> {@code open(Path)}, {@code open(Path, int)}, and {@code
 * openFile(Path).build()} all route through the cache — the builder-API override redirects to our
 * own {@code openFileWithOptions} so Spark vectorized Parquet/ORC readers, Iceberg, and S3A select
 * readers that prefer the modern builder still hit the cache. {@code openFile(PathHandle)} and
 * {@code open(PathHandle, int)} still pass through to the inner FS (PathHandle's opaque content tag
 * prevents the cache from keying on a stable file identity); document this gap to integrators using
 * PathHandle-based read APIs.
 */
public final class CachedFileSystem extends FilterFileSystem {

  // volatile: Hadoop's FileSystem.CACHE publishes instances via a synchronized map which gives
  // happens-before, but the test-only constructor below + tests that call initialize/open from
  // different threads bypass that path. Volatile here is cheap and removes the publication
  // assumption without measurable overhead.
  private volatile boolean enabled;
  private volatile URI uri;

  // Per-decorator binding into the bootstrap's registry. `endpoint` is derived from the INNER FS's
  // URI (which Hadoop may normalize — e.g. HDFS appending the default port), so that the same
  // endpoint string is computed when keys flow through dispatchOpen at open() time. `ownOpener`
  // holds the exact opener reference we installed so close() can use identity-aware removal and
  // not blow away a peer decorator that re-registered the same endpoint between our open and our
  // close.
  private volatile String endpoint;
  private volatile CacheBootstrap.HandleOpener ownOpener;

  public CachedFileSystem() {}

  /** Test-only constructor that pre-binds the inner FS. Production uses the no-arg form. */
  public CachedFileSystem(FileSystem inner) {
    super(inner);
  }

  @Override
  public String getScheme() {
    // We inherit the inner's scheme so callers see e.g. "s3a://" not "cached://".
    return fs == null ? null : fs.getScheme();
  }

  @Override
  public URI getUri() {
    return uri != null ? uri : super.getUri();
  }

  /**
   * Initializes the decorator: instantiates the inner FS (per {@link CachedFsConfig#INNER_IMPL}),
   * runs its {@code initialize}, then — when {@link CachedFsConfig#ENABLED} is true — installs this
   * decorator's {@code scheme://authority} opener into the bootstrap registry and finally flips
   * {@link #enabled} to true. Order is load-bearing: the opener MUST be visible before {@code
   * enabled} so a racing reader cannot observe {@code enabled=true} and miss the opener.
   *
   * <p>On any throw the partially-constructed inner FS is closed, preventing socket/thread leaks
   * since Hadoop's FileSystem.CACHE does not call {@link #close} on a failed initialize.
   */
  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    this.uri = name;
    setConf(conf);
    FileSystem inner = createInner(name, conf);
    // Hadoop's FileSystem.CACHE adds the instance to its map BEFORE calling initialize, and on
    // initialize failure it discards the instance without calling close(). Anything that throws
    // from here until the end of this method therefore leaks `inner` (sockets, threads, native
    // resources) for the JVM lifetime. Cover inner.initialize() inside the guard too — a thrown
    // initialize is the most likely failure point (bad config, missing credentials) and must
    // not orphan the partially-constructed inner FS.
    boolean initialized = false;
    try {
      inner.initialize(name, conf);
      this.fs = inner;
      super.initialize(name, conf); // FilterFileSystem records statistics + sets working dir
      // Read the toggle but do NOT publish enabled=true yet — a concurrent reader observing
      // enabled via volatile would race ahead and hit dispatchOpen before installOpener has run.
      // Install the opener first, then flip enabled.
      boolean enableNow = CachedFsConfig.isEnabled(conf);
      if (enableNow) {
        CacheBootstrap.installIfNeeded(conf);
        // Derive endpoint from the INNER FS's URI, not the caller-provided `name`: Hadoop FS
        // implementations may normalize their URI in initialize() (HDFS appends default port,
        // S3A lowercases bucket, etc.) and the same normalization happens to handle keys via
        // fs.makeQualified() in open(). Register and dispatch against the canonical form.
        this.endpoint = CacheBootstrap.endpointKey(inner.getUri());
        this.ownOpener = this::openHandleForKey;
        CacheBootstrap.installOpener(endpoint, ownOpener);
      }
      this.enabled = enableNow;
      initialized = true;
    } finally {
      if (!initialized) {
        try {
          inner.close();
        } catch (IOException ignored) {
          // Best-effort cleanup; the primary exception is propagating.
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p><b>Note on {@code bufferSize}:</b> when {@code fs.cached.enabled=true} the parameter is
   * forwarded to the inner FS only when the cache is bypassed; the caching path uses {@code
   * fs.cached.load-quantum-bytes} as the read granularity instead, so a Spark / MR job that tuned
   * {@code bufferSize} sees that knob effectively ignored. Document this in any consumer tuning
   * guide.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (!enabled) {
      return fs.open(f, bufferSize);
    }
    // Defensive: enabled was true at initialize but bootstrap got uninstalled (test path) —
    // fall back to direct read rather than NPE.
    CacheBootstrap b = CacheBootstrap.get().orElse(null);
    if (b == null) {
      return fs.open(f, bufferSize);
    }
    Path qualified = fs.makeQualified(f);
    String key = qualified.toUri().toString();
    CachedFactory.CachedPtr<String, FileHandle> ptr;
    try {
      ptr = b.handleFactory().open(key);
    } catch (java.io.UncheckedIOException ex) {
      // The opener wraps IOException as UncheckedIOException so single-flight cleanup runs.
      // Unwrap on the way back out so callers see the original cause.
      throw ex.getCause();
    } catch (RuntimeException ex) {
      // Anything else that escapes the generator — dispatchOpen's "no opener" /
      // "malformed key" IllegalStateException / IllegalArgumentException, an NPE from a
      // misbehaving inner FS client, etc. — gets wrapped as IOException so Hadoop / Spark retry
      // harnesses (which key off IOException) can see and react. A raw RuntimeException
      // would bubble up as an unhandled task failure and skip retry entirely.
      throw new IOException(ex);
    }
    // Ownership of `ptr` transfers to `cis` once CachingInputStream's constructor succeeds (its
    // close() releases the pin). Until then we own it ourselves, so any throw must release the
    // pin AND unwrap UncheckedIOException so callers see the declared IOException type instead
    // of a runtime exception. recordOpen is inside the guarded block because it can throw
    // under memory pressure (HashMap.Node allocation, etc.) and a leaked pin would keep this
    // file's cache entries un-evictable for the JVM lifetime.
    CachingInputStream cis = null;
    boolean ptrTransferred = false;
    try {
      // Register the file's open-time with the TTL controller. recordOpen returns true when it
      // installs (or refreshes a remove-in-progress) entry; false when an existing entry was
      // preserved. The return value is dropped — the hook is fire-and-forget on this path.
      b.ttlController().recordOpen(ptr.value().fileNum());
      // the wiring: resolve scanId via ThreadLocal → Configuration → "default", then look up
      // the tracker. When fs.cached.scan-tracker.enabled=false, substitute ScanTracker.DISABLED so
      // recordReference/recordRead are no-ops with zero map traffic. metricsEnabled likewise gates
      // between a fresh per-stream IoStatistics and the shared NO_OP sentinel.
      Configuration conf = getConf();
      String scanId = b.currentScanId();
      if (scanId == null || scanId.isBlank()) {
        scanId = conf.getTrimmed(CachedFsConfig.SCAN_ID, "default");
      }
      ScanTracker tracker =
          CachedFsConfig.scanTrackerEnabled(conf) ? b.trackerFor(scanId) : ScanTracker.DISABLED;
      IoStatistics ioStats =
          CachedFsConfig.metricsEnabled(conf) ? new IoStatistics() : IoStatistics.NO_OP;
      // per-(scanId, fileNumHash) TrackingId: Murmur3.fmix32 distributes sequential StringIdMap
      // ids uniformly across the 29-bit bucket space (a raw xor-fold would collide-zero until
      // ids exceed 2^29).
      int fileNumNode = Murmur3.fmix32((int) ptr.value().fileNum()) & ((1 << 29) - 1);
      TrackingId trackingId = TrackingId.of(fileNumNode, 0);
      // capture coalesce knobs at open time. Auto-disable on small caches per the
      // coalesce.enabled resolver (cap==2 AND always-on=false → off). totalRamBytes proxies via
      // the JVM heap budget — AsyncDataCache has no fixed capacity getter today; for the
      // auto-disable rule, heap-budget is a reasonable upper bound on what the cache can hold.
      long totalRamBytes = Runtime.getRuntime().maxMemory();
      boolean coalesceEnabled =
          CachedFsConfig.coalesceEnabled(conf, totalRamBytes, b.loadQuantumBytes());
      int maxGap = CachedFsConfig.coalesceMaxGapBytes(conf, b.loadQuantumBytes());
      int maxChunksPerGroup =
          CachedFsConfig.coalesceMaxChunksPerGroup(conf, totalRamBytes, b.loadQuantumBytes());
      int maxRestarts = CachedFsConfig.coalesceMaxRestarts(conf);
      // admission knobs.
      boolean prefetchEnabled = CachedFsConfig.prefetchEnabled(conf);
      boolean heapPressureCheck = CachedFsConfig.prefetchHeapPressureCheckEnabled(conf);
      double triggerTail = CachedFsConfig.prefetchTriggerTailFraction(conf);
      int densityPct = CachedFsConfig.prefetchDensityThresholdPct(conf);
      long rejectionBackoffNs =
          java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
              CachedFsConfig.prefetchRejectionBackoffMs(conf));
      cis =
          new CachingInputStream(
              ptr,
              b.ramCache(),
              b.loadQuantumBytes(),
              tracker,
              trackingId,
              ioStats,
              b.aggregateIoStats(),
              coalesceEnabled,
              maxGap,
              maxChunksPerGroup,
              maxRestarts,
              b,
              prefetchEnabled,
              heapPressureCheck,
              triggerTail,
              densityPct,
              rejectionBackoffNs);
      ptrTransferred = true;
      return new FSDataInputStream(cis);
    } catch (java.io.UncheckedIOException ex) {
      releaseOnFailure(ptr, cis, ptrTransferred, ex);
      throw ex.getCause();
    } catch (RuntimeException | Error ex) {
      releaseOnFailure(ptr, cis, ptrTransferred, ex);
      throw ex;
    }
  }

  /**
   * Override of {@link FilterFileSystem#getFileBlockLocations(FileStatus, long, long)}. Delegates
   * to the inner FS, then asks the optional {@link BlockLocationsProvider} (installed by the {@code
   * cached-fs-spark} module's Spark extension) to rewrite the result. When no provider is installed
   * or {@link #enabled} is false, returns the inner FS result unchanged.
   *
   * <p>Spark's planner consults this method on the driver during FileScan planning; the rewritten
   * locations flow into {@code FilePartition.preferredLocations()} and the DAGScheduler parses
   * {@code "executor_<host>_<execId>"} strings as {@link
   * org.apache.spark.scheduler.ExecutorCacheTaskLocation} for PROCESS_LOCAL scheduling.
   */
  @Override
  public org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(
      FileStatus file, long start, long len) throws IOException {
    BlockLocationsProvider provider = activeProvider();
    if (provider == null) {
      return fs.getFileBlockLocations(file, start, len);
    }
    org.apache.hadoop.fs.BlockLocation[] underlying = fs.getFileBlockLocations(file, start, len);
    org.apache.hadoop.fs.BlockLocation[] rewritten =
        provider.getBlockLocations(file, start, len, underlying);
    // The BlockLocationsProvider contract allows null as "no change" — fall back to underlying so
    // a deferring provider doesn't have to know about the original array.
    return rewritten != null ? rewritten : underlying;
  }

  /**
   * Spark 4 uses TWO listing entry points that consume locality info from {@link
   * org.apache.hadoop.fs.LocatedFileStatus} EMBEDDED block-locations directly (never calling our
   * {@link #getFileBlockLocations(FileStatus, long, long)} override on that path):
   *
   * <ul>
   *   <li>{@code HadoopFSUtils.listFiles} — Spark 4's default for {@code s3a} (controlled by {@code
   *       spark.sql.sources.useListFilesFileSystemList="s3a"}). Calls {@code fs.listFiles(path,
   *       true)} and consumes the {@code BlockLocation[]} on each emitted {@code LocatedFileStatus}
   *       directly via {@code PartitionedFileUtil.getBlockHosts}.
   *   <li>{@code HadoopFSUtils.parallelListLeafFiles} for non-HDFS / non-ViewFS schemes — falls
   *       back to {@code fs.listStatus + getFileBlockLocations} (DOES hit our override).
   * </ul>
   *
   * <p>To cover both paths we override {@link #listLocatedStatus(Path)} and {@link #listFiles(Path,
   * boolean)} below. Each wraps the inner iterator and rewrites each emitted entry's {@code
   * BlockLocation[]} through the provider — EXCEPT for HDFS-specific {@code LocatedFileStatus}
   * subclasses (detected via package-name match to avoid a compile-time dep on hadoop-hdfs-client).
   * HDFS subclasses carry {@code FileEncryptionInfo} + erasure-coding policy + fileId; copying them
   * through the {@code (FileStatus, BlockLocation[])} constructor flattens them to a plain {@code
   * LocatedFileStatus}, breaking Spark on HDFS Transparent Encryption clusters. For HDFS this is
   * acceptable because Spark uses {@code DistributedFileSystem}'s own locality directly so the
   * affinity hint isn't lost there.
   */
  private BlockLocationsProvider activeProvider() {
    if (!enabled) return null;
    CacheBootstrap b = CacheBootstrap.get().orElse(null);
    if (b == null) return null;
    return b.blockLocationsProvider();
  }

  @Override
  public org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus>
      listLocatedStatus(Path f) throws IOException {
    org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> inner =
        fs.listLocatedStatus(f);
    BlockLocationsProvider provider = activeProvider();
    if (provider == null) return inner;
    return rewriteLocatedIterator(inner, provider);
  }

  // Note: the protected {@code listLocatedStatus(Path, PathFilter)} overload is NOT overridden.
  // It would require calling {@code fs.listLocatedStatus(Path, PathFilter)} on the inner FS, but
  // that method is package-private to {@code org.apache.hadoop.fs} and inaccessible from this
  // package. {@link FilterFileSystem}'s default override (same package as {@code FileSystem}) does
  // delegate to the inner — so the unrewritten inner iterator is returned, bypassing the affinity
  // rewrite. In Spark 4 production paths nothing reaches the 2-arg variant (public 1-arg form is
  // what HadoopFSUtils + InMemoryFileIndex consume), so this is a documented future-caller gap.
  // Closing it would require a reflective invocation of the inner FS's protected method.

  @Override
  public org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> listFiles(
      Path f, boolean recursive) throws IOException {
    org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> inner =
        fs.listFiles(f, recursive);
    BlockLocationsProvider provider = activeProvider();
    if (provider == null) return inner;
    return rewriteLocatedIterator(inner, provider);
  }

  /**
   * Lazy {@link org.apache.hadoop.fs.RemoteIterator} wrapper that rewrites each emitted entry's
   * block-locations. {@link
   * org.apache.hadoop.util.functional.RemoteIterators#mappingRemoteIterator} preserves {@link
   * org.apache.hadoop.fs.statistics.IOStatisticsSource} and {@link java.io.Closeable} forwarding —
   * important so Spark 4's IOStatistics aggregation and Hadoop's S3A streaming-list-result cleanup
   * keep working through the wrapper. Per-entry preserve-subclass detection routes entries through
   * unchanged when flattening would drop subclass-specific data (HDFS encryption / EC, S3A / ABFS
   * {@link org.apache.hadoop.fs.EtagSource} eTag for Hadoop {@code ManifestCommitter} validation).
   */
  private static org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus>
      rewriteLocatedIterator(
          org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> inner,
          BlockLocationsProvider provider) {
    return org.apache.hadoop.util.functional.RemoteIterators.mappingRemoteIterator(
        inner, original -> rewriteOne(original, provider));
  }

  private static org.apache.hadoop.fs.LocatedFileStatus rewriteOne(
      org.apache.hadoop.fs.LocatedFileStatus original, BlockLocationsProvider provider)
      throws IOException {
    if (shouldPreserveSubclass(original)) {
      // Preserve subclass identity: HdfsLocatedFileStatus carries FileEncryptionInfo + EC
      // policy + fileId; S3ALocatedFileStatus / AbfsLocatedFileStatus carry eTag + versionId
      // consumed by Hadoop ManifestCommitter + Iceberg / Delta change detection via the
      // EtagSource interface. The (FileStatus, BlockLocation[]) copy constructor flattens these
      // to a plain LocatedFileStatus, silently dropping the subclass-only fields. Trade-off: for
      // these files the affinity hint does not propagate, but the connector-specific metadata
      // survives — which is the strictly safer default.
      return original;
    }
    org.apache.hadoop.fs.BlockLocation[] underlying = original.getBlockLocations();
    if (underlying == null || underlying.length == 0) {
      return original;
    }
    org.apache.hadoop.fs.BlockLocation[] rewritten =
        provider.getBlockLocations(original, 0L, original.getLen(), underlying);
    if (rewritten == null || rewritten == underlying) {
      return original;
    }
    return new org.apache.hadoop.fs.LocatedFileStatus(original, rewritten);
  }

  /**
   * True when {@code lfs} carries subclass-specific data that the {@code new
   * LocatedFileStatus(original, rewritten)} copy constructor would flatten away. Covers HDFS
   * subclasses (package-prefix match, no compile-time dep on hadoop-hdfs-client) AND any {@link
   * org.apache.hadoop.fs.EtagSource} implementer — which catches S3A's {@code S3ALocatedFileStatus}
   * and ABFS's {@code AbfsLocatedFileStatus} uniformly without requiring a per-connector match
   * list.
   */
  private static boolean shouldPreserveSubclass(org.apache.hadoop.fs.LocatedFileStatus lfs) {
    if (lfs instanceof org.apache.hadoop.fs.EtagSource) {
      return true;
    }
    return lfs.getClass().getName().startsWith("org.apache.hadoop.hdfs.");
  }

  /**
   * Override of {@link FilterFileSystem#openFile(Path)}. The inherited delegation hands the builder
   * to the inner FS, which means a Spark vectorized Parquet/ORC reader using {@code
   * openFile(p).build()} would silently bypass the cache. By returning the default {@link
   * FileSystem} builder bound to {@code this}, we keep ourselves on the call path so the builder's
   * {@code build()} routes into {@link #openFileWithOptions(Path,
   * org.apache.hadoop.fs.impl.OpenFileParameters)} below — which calls our {@link #open(Path, int)}
   * and goes through the cache.
   *
   * <p>{@code withFileStatus(FileStatus)} is accepted but ignored on the cached path because the
   * cache keys on {@code Path} (file identity is recomputed via {@code StringIdMap}); skipping the
   * status pre-fetch optimization for now is a documented trade-off.
   */
  @Override
  public org.apache.hadoop.fs.FutureDataInputStreamBuilder openFile(Path path) throws IOException {
    return new CachedFsInputStreamBuilder(this, path).getThisBuilder();
  }

  /**
   * Trivial concrete subclass of {@link org.apache.hadoop.fs.impl.FutureDataInputStreamBuilderImpl}
   * — only exists because Hadoop's default builder is a {@code private} nested class of {@link
   * FileSystem} and therefore unreachable. The inherited {@code build()} calls {@code
   * getFS().openFileWithOptions(...)}, which lands on our override below.
   */
  private static final class CachedFsInputStreamBuilder
      extends org.apache.hadoop.fs.impl.FutureDataInputStreamBuilderImpl {
    CachedFsInputStreamBuilder(CachedFileSystem fs, Path path) throws IOException {
      super(fs, path);
    }

    @Override
    public java.util.concurrent.CompletableFuture<FSDataInputStream> build() throws IOException {
      org.apache.hadoop.fs.impl.OpenFileParameters parameters =
          new org.apache.hadoop.fs.impl.OpenFileParameters()
              .withMandatoryKeys(getMandatoryKeys())
              .withOptionalKeys(getOptionalKeys())
              .withOptions(getOptions())
              .withStatus(super.getStatus())
              .withBufferSize(getBufferSize());
      CachedFileSystem cfs = (CachedFileSystem) getFS();
      return cfs.openFileWithOptions(getPath(), parameters);
    }
  }

  /**
   * Bypass: {@code PathHandle} carries an opaque content tag and (per Hadoop's contract) refers to
   * a specific file content version, not a path. The cache keys on {@code Path} → {@code
   * StringIdMap.fileNum}, so we cannot reliably resolve a PathHandle back to a cached entry without
   * an inner-FS round trip that re-canonicalizes. For now, delegate to the inner FS unchanged —
   * future work may key cached entries on PathHandle.contentHash for connectors that publish one.
   */
  @Override
  public org.apache.hadoop.fs.FutureDataInputStreamBuilder openFile(
      org.apache.hadoop.fs.PathHandle pathHandle) throws IOException {
    return fs.openFile(pathHandle);
  }

  /**
   * Path-based builder build(). Rejects unknown mandatory keys per the Hadoop contract, then routes
   * to our {@link #open(Path, int)} so the cache sees the read.
   */
  @Override
  protected java.util.concurrent.CompletableFuture<FSDataInputStream> openFileWithOptions(
      Path path, org.apache.hadoop.fs.impl.OpenFileParameters parameters) throws IOException {
    org.apache.hadoop.fs.impl.AbstractFSBuilderImpl.rejectUnknownMandatoryKeys(
        parameters.getMandatoryKeys(),
        org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_STANDARD_OPTIONS,
        "for " + path);
    return org.apache.hadoop.util.LambdaUtils.eval(
        new java.util.concurrent.CompletableFuture<>(),
        () -> open(path, parameters.getBufferSize()));
  }

  private static void releaseOnFailure(
      CachedFactory.CachedPtr<String, FileHandle> ptr,
      CachingInputStream cis,
      boolean ptrTransferred,
      Throwable primary) {
    if (ptrTransferred && cis != null) {
      try {
        cis.close();
      } catch (IOException | RuntimeException | Error suppressed) {
        primary.addSuppressed(suppressed);
      }
    } else {
      try {
        ptr.close();
      } catch (RuntimeException | Error suppressed) {
        primary.addSuppressed(suppressed);
      }
    }
  }

  /**
   * Closes the decorator in a strict order:
   *
   * <ol>
   *   <li>flip {@link #enabled} to false so racing readers either bypass or have already captured
   *       the bootstrap reference;
   *   <li>identity-aware {@link CacheBootstrap#removeOpener} so a peer decorator that re-registered
   *       the same endpoint keeps its registration;
   *   <li>drain only this decorator's handles via {@code closeMatching};
   *   <li>finally close the wrapped inner FS.
   * </ol>
   *
   * <p>Reversing steps 2 and 3 would leak handles: a mid-dispatch thread that already captured the
   * opener could install a fresh handle into the LRU after drain returned and before the opener was
   * removed; {@link #close} would then leave a stale handle whose stream gets invalidated by the
   * inner-FS close.
   */
  @Override
  public void close() throws IOException {
    // Order is load-bearing:
    //   1. enabled = false       — racing readers either see false (bypass) or have already
    //                              captured the bootstrap reference.
    //   2. removeOpener          — fail-fast for any reader that DID capture the bootstrap and
    //                              tries to dispatch a fresh open; dispatchOpen throws
    //                              IllegalStateException, which open() wraps as IOException.
    //   3. closeMatching         — drain whatever in-flight `pending` generators (which already
    //                              captured the opener BEFORE step 2) had time to settle.
    //   4. super.close()         — close the inner FS last, AFTER every handle that referenced
    //                              it has been drained and closed.
    // Reversing 2/3 would leave a window where a thread mid-dispatch installs a fresh handle
    // into the LRU after drain returned and before the opener is removed; super.close() then
    // invalidates that handle's stream and the entry leaks.
    this.enabled = false;
    IOException primary = null;
    String localEndpoint = this.endpoint;
    CacheBootstrap.HandleOpener localOpener = this.ownOpener;
    CacheBootstrap b = CacheBootstrap.get().orElse(null);
    if (b != null && localEndpoint != null) {
      if (localOpener != null) {
        // Identity-aware removal so a peer decorator that re-registered the same endpoint
        // between our open and our close keeps its registration intact.
        CacheBootstrap.removeOpener(localEndpoint, localOpener);
      }
      try {
        // Match handles whose URI parses to the same endpoint (scheme + authority) as this
        // decorator. Parsing the key handles edge cases that raw startsWith can't (empty
        // authority — file:// endpoint vs file:/path keys; authorities with `:` or `-`).
        // Catch Exception, not just URISyntaxException, because endpointKey throws
        // IllegalArgumentException for a schemeless URI — an IAE escaping mid-iteration would
        // half-drain the LRU and skip super.close() entirely.
        String captured = localEndpoint;
        b.handleFactory()
            .closeMatching(
                k -> {
                  try {
                    return captured.equals(CacheBootstrap.endpointKey(new URI(k)));
                  } catch (Exception ex) {
                    return false;
                  }
                });
      } catch (IOException ex) {
        primary = ex;
      } catch (RuntimeException ex) {
        // closeMatching contract is IOException, but a bug in the predicate or in a future
        // CachedFactory refactor could still leak a RuntimeException. Wrap so super.close() runs
        // and the inner FS is released — otherwise a defensive-coding regression here would
        // silently leak the inner FS for the JVM lifetime.
        primary = new IOException(ex);
      }
    }
    try {
      super.close();
    } catch (IOException ex) {
      if (primary == null) primary = ex;
      else primary.addSuppressed(ex);
    }
    if (primary != null) {
      throw primary;
    }
  }

  /** Returns the wrapped inner FS — escape hatch for tests and tooling. */
  public FileSystem innerFileSystem() {
    return fs;
  }

  // --- internals ----------------------------------------------------------

  private FileSystem createInner(URI name, Configuration conf) throws IOException {
    String innerImpl = conf.getTrimmed(CachedFsConfig.INNER_IMPL, "");
    if (innerImpl.isEmpty()) {
      throw new IOException(
          "CachedFileSystem requires "
              + CachedFsConfig.INNER_IMPL
              + " to identify the wrapped FileSystem implementation");
    }
    try {
      Class<?> cls = conf.getClassByName(innerImpl);
      if (!FileSystem.class.isAssignableFrom(cls)) {
        throw new IOException(
            CachedFsConfig.INNER_IMPL + "=" + innerImpl + " is not a FileSystem subclass");
      }
      @SuppressWarnings("unchecked")
      Class<? extends FileSystem> fsCls = (Class<? extends FileSystem>) cls;
      return ReflectionUtils.newInstance(fsCls, conf);
    } catch (ClassNotFoundException ex) {
      throw new IOException("Inner FS class not found: " + innerImpl, ex);
    }
  }

  private FileHandle openHandleForKey(String key) throws IOException {
    URI fileUri = URI.create(key);
    Path p = new Path(fileUri);
    FileStatus status = fs.getFileStatus(p);
    long size = status.getLen();
    // route through the bootstrap's ReadFileFactory so tests can swap in a counting
    // wrapper via setReadFileFactoryForTesting without subclassing CachedFileSystem. Production
    // path: HadoopReadFile::new, unchanged behavior from prior `new HadoopReadFile(fs, p, key,
    // size)`.
    ReadFile rf =
        CacheBootstrap.get()
            .orElseThrow(() -> new IllegalStateException("CacheBootstrap missing"))
            .readFileFactory()
            .create(fs, p, key, size);
    try {
      StringIdMap ids =
          CacheBootstrap.get()
              .orElseThrow(() -> new IllegalStateException("CacheBootstrap missing"))
              .stringIds();
      // Mint uuid first, then groupId — if either lease constructor throws (StringIdMap
      // exhausted, capacity error, IllegalStateException above) the outer catch closes rf.
      StringIdLease uuid = new StringIdLease(ids, key);
      try {
        StringIdLease groupId = parentLease(ids, p);
        try {
          return new FileHandle(rf, uuid, groupId);
        } catch (RuntimeException | Error ex) {
          groupId.close();
          throw ex;
        }
      } catch (RuntimeException | Error ex) {
        uuid.close();
        throw ex;
      }
    } catch (RuntimeException | Error ex) {
      try {
        rf.close();
      } catch (IOException suppressed) {
        ex.addSuppressed(suppressed);
      }
      throw ex;
    }
  }

  private static StringIdLease parentLease(StringIdMap ids, Path p) {
    Path parent = p.getParent();
    if (parent == null) {
      return StringIdLease.empty(ids);
    }
    return new StringIdLease(ids, parent.toUri().toString());
  }
}
