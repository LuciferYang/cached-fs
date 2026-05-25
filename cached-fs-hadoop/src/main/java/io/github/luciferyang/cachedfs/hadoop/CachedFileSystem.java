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
 * <p><b>Cached vs passthrough APIs:</b> only the legacy {@code open(Path)} / {@code open(Path,
 * int)} entry points route through the cache. The modern builder API ({@link
 * FilterFileSystem#openFile(Path)}, {@code openFile(PathHandle)}) and {@code open(PathHandle, int)}
 * delegate to the inner FS unchanged — they bypass the cache. Readers that prefer the builder API
 * (S3A select reads, some Iceberg / Parquet code paths) will see zero cache hits. Routing those
 * through the cache is a future task; configure your reader to use {@code open(Path, int)} until
 * then.
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
      cis = new CachingInputStream(ptr, b.ramCache(), b.loadQuantumBytes());
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
    HadoopReadFile rf = new HadoopReadFile(fs, p, key, size);
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
