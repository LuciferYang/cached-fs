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

import io.github.luciferyang.cachedfs.core.AsyncDataCache;
import io.github.luciferyang.cachedfs.core.handle.FileHandle;
import io.github.luciferyang.cachedfs.core.handle.FileHandleFactory;
import io.github.luciferyang.cachedfs.core.id.FileIds;
import io.github.luciferyang.cachedfs.core.id.StringIdLease;
import io.github.luciferyang.cachedfs.core.id.StringIdMap;
import io.github.luciferyang.cachedfs.core.ssd.SsdCache;
import io.github.luciferyang.cachedfs.core.stats.AggregatedIoStatistics;
import io.github.luciferyang.cachedfs.core.tracker.ScanTracker;
import io.github.luciferyang.cachedfs.core.ttl.CacheTTLController;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-JVM singleton holder for the cache tiers. Hadoop's {@link
 * org.apache.hadoop.fs.FileSystem#initialize} fires once per (URI, conf) pair and may be invoked
 * concurrently from multiple executors / connectors in the same JVM; this class makes the
 * initialization race-free and idempotent by guarding with a single lock.
 *
 * <p>Layout once installed:
 *
 * <ul>
 *   <li>{@link AsyncDataCache} singleton via {@link AsyncDataCache#setInstance}
 *   <li>{@link StringIdMap} singleton via {@link FileIds#setInstance}
 *   <li>{@link SsdCache} held here (no global setter in core; the {@link
 *       io.github.luciferyang.cachedfs.hadoop.CachedFileSystem} reads from {@link #ssdCache()})
 *   <li>{@link FileHandleFactory} sized by {@link CachedFsConfig#HANDLE_CACHE_CAPACITY}
 *   <li>Per-endpoint {@link HandleOpener} registry keyed by {@code scheme://authority} (one entry
 *       per live {@link CachedFileSystem} instance), so a single JVM can cache reads from {@code
 *       hdfs://nn-a}, {@code s3a://bucket-x}, and {@code bos://bucket-y} side by side
 * </ul>
 *
 * <p>The first {@link #installIfNeeded} call wins; subsequent calls are no-ops regardless of the
 * Configuration they see. Operators that need to change cache settings must restart the JVM.
 * Openers are registered via {@link #installOpener} after the tiers are up and removed via {@link
 * #removeOpener} when the owning decorator closes.
 */
public final class CacheBootstrap {

  private static final Logger LOG = LoggerFactory.getLogger(CacheBootstrap.class);
  private static final ReentrantLock LOCK = new ReentrantLock();
  private static volatile CacheBootstrap installed;

  /**
   * Pending {@link BlockLocationsProvider} set by a downstream module BEFORE the bootstrap has been
   * installed. {@link #installIfNeeded(Configuration)} picks it up at the end of install so the
   * provider is in place by the time the first {@code getFileBlockLocations} call lands. Volatile
   * for cross-thread publication (Spark extension typically runs on the driver's session-start
   * thread; first {@code installIfNeeded} runs on whichever thread first opens a cached file).
   */
  private static volatile BlockLocationsProvider pendingBlockLocationsProvider;

  /**
   * Cap on the number of WARN-level stale-slot-recovery log lines per JVM. Beyond this, recoveries
   * are logged at DEBUG (the {@link AggregatedIoStatistics#staleScanIdRecoveries()} counter keeps
   * ticking regardless). 16 is enough to surface a new failure mode in logs without flooding
   * long-lived JVMs (Spark drivers, YARN NMs) where executor-thread reuse legitimately produces
   * recoveries across many tasks.
   */
  private static final int STALE_SLOT_WARN_CAP = 16;

  private final AsyncDataCache ramCache;
  private final SsdCache ssdCache;
  private final StringIdMap stringIds;
  private final FileHandleFactory handleFactory;
  private final int loadQuantumBytes;
  private final CacheTTLController ttlController;

  /**
   * prefetch executor. Null when {@code fs.cached.prefetch.enabled=false} (the consumer checks for
   * null before submitting). Built once at {@link #installIfNeeded} and shut down in {@link
   * #uninstallForTesting} with a bounded await + {@code shutdownNow} fallback to bound the pin-leak
   * window per phase-5c.md §step 1.
   */
  private final java.util.concurrent.ThreadPoolExecutor prefetchExecutor;

  /** admission gate — JVM-wide byte budget for in-flight prefetch. */
  private final long maxPendingPrefetchBytes;

  /**
   * Heap-pressure cache TTL (nanos); {@link #isHeapPressureHigh} refreshes at most once per TTL.
   */
  private final long heapPressureTtlNs;

  /**
   * Last-refresh timestamp (nanoTime) for the heap-pressure check. Single-CAS-winner pattern: at
   * most one thread per TTL window calls the MemoryMXBean; concurrent callers read the cached
   * {@link #heapPressureActive} bit until the window expires.
   */
  private final java.util.concurrent.atomic.AtomicLong heapPressureLastCheckedNs;

  /** Cached heap-pressure bit; refreshed by {@link #isHeapPressureHigh}. */
  private volatile boolean heapPressureActive;

  /**
   * Per-scan {@link ScanTracker} registry. Lifetime entries are inserted lazily by {@link
   * #trackerFor(String)} and evicted by {@link #withScanId(String)} close or explicit {@link
   * #removeScanTracker(String)}. See phase-5a doc for the long-lived-JVM eviction rationale.
   */
  private final ConcurrentMap<String, ScanTracker> scanTrackers = new ConcurrentHashMap<>();

  /**
   * Per-thread scanId slot. Read precedence: {@code currentScanId()} → {@code
   * conf.getTrimmed(SCAN_ID)} → {@code "default"}. Single-thread close contract — see {@link
   * #withScanId(String)}.
   */
  private final ThreadLocal<String> currentScanId = new ThreadLocal<>();

  /** Bootstrap-level aggregated IO stats; populated by per-stream {@code close()} merges. */
  private final AggregatedIoStatistics aggregateIoStats = new AggregatedIoStatistics();

  /** WARN-cap counter for stale-slot recovery; see {@link #STALE_SLOT_WARN_CAP}. */
  private final AtomicInteger staleSlotWarnCount = new AtomicInteger();

  /**
   * seam — production default is {@code HadoopReadFile::new}. Volatile so a test-only swap via
   * {@link #setReadFileFactoryForTesting(ReadFileFactory)} is visible to the next {@code
   * openHandleForKey} on any thread. Single-writer-at-a-time in practice (Surefire default
   * forkCount=1, reuseForks=true); tests with parallel execution should serialize on this slot via
   * {@code @Execution(SAME_THREAD)}.
   */
  private volatile ReadFileFactory readFileFactory = HadoopReadFile::new;

  /**
   * Optional block-locations override. Defaults to {@code null} (decorator returns the inner FS's
   * locations verbatim). The {@code cached-fs-spark} module installs an affinity-aware provider
   * here so Spark's planner sees executor-shaped task locations.
   */
  private volatile BlockLocationsProvider blockLocationsProvider;

  /**
   * Live opener registry, keyed by {@code scheme://authority}. Each {@link CachedFileSystem}
   * instance owns exactly one entry for the lifetime of its initialize/close pair. The {@link
   * FileHandleFactory}'s generator dispatches into this map at handle-open time.
   */
  private final ConcurrentMap<String, HandleOpener> openersByEndpoint = new ConcurrentHashMap<>();

  private CacheBootstrap(
      AsyncDataCache ramCache,
      SsdCache ssdCache,
      StringIdMap stringIds,
      int loadQuantumBytes,
      int handleCapacity,
      java.util.concurrent.ThreadPoolExecutor prefetchExecutor,
      long maxPendingPrefetchBytes,
      long heapPressureTtlNs) {
    this.ramCache = ramCache;
    this.ssdCache = ssdCache;
    this.stringIds = stringIds;
    this.loadQuantumBytes = loadQuantumBytes;
    // FileHandleFactory's generator captures `this` so it can route each key through the registry.
    this.handleFactory = new FileHandleFactory(handleCapacity, this::dispatchOpen);
    // TTL controller is always constructed; embedders opt in by calling applyTTL externally.
    // Cheap to construct (empty map) and harmless when no applyTTL caller exists.
    this.ttlController = new CacheTTLController(ramCache, ssdCache, Clock.systemUTC());
    this.prefetchExecutor = prefetchExecutor;
    this.maxPendingPrefetchBytes = maxPendingPrefetchBytes;
    this.heapPressureTtlNs = heapPressureTtlNs;
    // Initialize timestamp so the first call always refreshes regardless of nanoTime's origin.
    this.heapPressureLastCheckedNs =
        new java.util.concurrent.atomic.AtomicLong(System.nanoTime() - heapPressureTtlNs - 1L);
  }

  /**
   * Builds the {@code scheme://authority} endpoint key used by the opener registry. Returns just
   * the scheme (with trailing {@code ://}) when no authority is present — matches Hadoop's
   * convention for schemes that don't use one (e.g. {@code file:///}).
   */
  static String endpointKey(URI uri) {
    String scheme = uri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException("URI has no scheme: " + uri);
    }
    String authority = uri.getAuthority();
    return scheme + "://" + (authority == null ? "" : authority);
  }

  /** Returns the installed bootstrap, or empty if {@link #installIfNeeded} has not run. */
  public static Optional<CacheBootstrap> get() {
    return Optional.ofNullable(installed);
  }

  /**
   * Installs the cache singletons if no installation exists yet. Idempotent: a second concurrent
   * call observes the existing installation and returns it unchanged. Tiers (RAM, SSD, ID map,
   * handle factory) are created here; per-scheme {@link HandleOpener} instances are NOT — those are
   * registered separately via {@link #installOpener} after each decorator's inner FS is built.
   */
  public static CacheBootstrap installIfNeeded(Configuration conf) throws IOException {
    CacheBootstrap snapshot = installed;
    if (snapshot != null) {
      return snapshot;
    }
    LOCK.lock();
    try {
      if (installed != null) {
        return installed;
      }
      // Defer global singleton publication until every sub-component has been constructed
      // successfully. Otherwise, a throw mid-install (e.g. SsdCache failing to open a mount)
      // would leave AsyncDataCache.setInstance pointing at an orphaned cache that a retry would
      // overwrite, leaking the first instance for the JVM lifetime.
      AsyncDataCache ram = new AsyncDataCache(CachedFsConfig.ramOptions(conf));
      SsdCache ssd = null;
      // Hoisted out of the inner try so the rollback catch can shut it down on a late failure.
      java.util.concurrent.ThreadPoolExecutor prefetchExec = null;
      try {
        StringIdMap ids = new StringIdMap();
        SsdCache.Config ssdCfg = CachedFsConfig.ssdConfig(conf);
        if (ssdCfg != null) {
          ssd = new SsdCache(ssdCfg, ids);
        }
        int handleCap = CachedFsConfig.handleCacheCapacity(conf);
        int quantum = CachedFsConfig.loadQuantumBytes(conf);
        // executor: built only when the master toggle is on. Null when disabled — the
        // hot read path checks for null before submitting a prefetch task.
        long maxPendingBytes = 0L;
        if (CachedFsConfig.prefetchEnabled(conf)) {
          int threads = CachedFsConfig.prefetchThreads(conf);
          prefetchExec =
              PrefetchExecutorFactory.create(threads, CachedFsConfig.prefetchQueue(conf));
          maxPendingBytes = CachedFsConfig.prefetchMaxPendingBytes(conf, quantum, threads);
        }
        long heapPressureTtlNs =
            java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                CachedFsConfig.prefetchHeapPressureTtlMs(conf));
        CacheBootstrap b =
            new CacheBootstrap(
                ram,
                ssd,
                ids,
                quantum,
                handleCap,
                prefetchExec,
                maxPendingBytes,
                heapPressureTtlNs);
        // All pieces ready — now publish singletons atomically (w.r.t. LOCK) and commit.
        // Order matters because FileIds has no clearInstance API (test-only singleton):
        // publish AsyncDataCache FIRST so that if FileIds.setInstance somehow throws (it can
        // only throw on null today, which we don't pass) we can still call
        // AsyncDataCache.clearInstance to roll back. Doing FileIds first would leave a
        // permanently-orphaned StringIdMap singleton if AsyncDataCache.setInstance later threw.
        AsyncDataCache.setInstance(ram);
        try {
          FileIds.setInstance(ids);
        } catch (RuntimeException | Error setIdsEx) {
          AsyncDataCache.clearInstance();
          throw setIdsEx;
        }
        // Pick up a provider staged before the bootstrap came online (typical for Spark
        // extensions that fire at session start, before any FileSystem.get(...) call).
        BlockLocationsProvider pending = pendingBlockLocationsProvider;
        if (pending != null) {
          b.blockLocationsProvider = pending;
        }
        installed = b;
        return b;
      } catch (IOException | RuntimeException | Error ex) {
        // Roll back partial construction; the JVM stays in the pre-install state so a retry
        // gets a clean slate. The prefetch executor — when present — must be shut down too:
        // its worker threads are daemon, but each holds a native stack + queued task graph
        // that survives until JVM exit, and repeated install-failure retries compound the
        // leak. shutdownNow is safe here because we have not yet returned the bootstrap to
        // any caller, so no read path has submitted a prefetch task.
        if (prefetchExec != null) {
          try {
            prefetchExec.shutdownNow();
          } catch (RuntimeException suppressed) {
            ex.addSuppressed(suppressed);
          }
        }
        if (ssd != null) {
          try {
            ssd.close();
          } catch (IOException suppressed) {
            ex.addSuppressed(suppressed);
          }
        }
        try {
          ram.close();
        } catch (RuntimeException suppressed) {
          ex.addSuppressed(suppressed);
        }
        throw ex;
      }
    } finally {
      LOCK.unlock();
    }
  }

  /**
   * Registers the {@link HandleOpener} that services keys whose URI matches {@code endpoint} (a
   * {@code scheme://authority} string). Throws if the bootstrap has not been installed yet ({@link
   * #installIfNeeded} must run first) or if {@code endpoint} is already registered — the registry
   * enforces one live decorator per endpoint, matching the comment on the openers field. Silently
   * overwriting would let a peer's closeMatching drain handles owned by the new decorator (the
   * close predicate matches by endpoint, not by opener identity). Lock-synchronized vs. {@link
   * #uninstallForTesting} so a teardown racing with this call cannot strand the entry on an
   * orphaned bootstrap.
   */
  public static void installOpener(String endpoint, HandleOpener opener) {
    Objects.requireNonNull(endpoint, "endpoint");
    Objects.requireNonNull(opener, "opener");
    LOCK.lock();
    try {
      CacheBootstrap b = installed;
      if (b == null) {
        throw new IllegalStateException(
            "CacheBootstrap is not installed; call installIfNeeded() before installOpener()");
      }
      HandleOpener prior = b.openersByEndpoint.putIfAbsent(endpoint, opener);
      if (prior != null && prior != opener) {
        throw new IllegalStateException(
            "Opener already registered for endpoint "
                + endpoint
                + "; close the prior CachedFileSystem before registering another");
      }
    } finally {
      LOCK.unlock();
    }
  }

  /**
   * Removes the opener for {@code endpoint} only if its current value is {@code expected} (identity
   * compare). Lets a decorator close-and-remove its own registration without nuking a sibling
   * decorator that re-registered the same endpoint between the two events. Returns true if the
   * entry was removed.
   */
  public static boolean removeOpener(String endpoint, HandleOpener expected) {
    Objects.requireNonNull(endpoint, "endpoint");
    Objects.requireNonNull(expected, "expected");
    LOCK.lock();
    try {
      CacheBootstrap b = installed;
      if (b == null) {
        return false;
      }
      return b.openersByEndpoint.remove(endpoint, expected);
    } finally {
      LOCK.unlock();
    }
  }

  /**
   * Force-removes the opener for {@code endpoint} regardless of which instance is registered.
   * Intended for tests and shutdown paths that don't track their original opener reference. Returns
   * true if an entry was removed.
   */
  public static boolean removeOpener(String endpoint) {
    Objects.requireNonNull(endpoint, "endpoint");
    LOCK.lock();
    try {
      CacheBootstrap b = installed;
      if (b == null) {
        return false;
      }
      return b.openersByEndpoint.remove(endpoint) != null;
    } finally {
      LOCK.unlock();
    }
  }

  /**
   * Returns true if an opener is currently registered for {@code endpoint}. Visible to tests so
   * they can assert that close() actually unregistered the decorator.
   */
  public boolean hasOpener(String endpoint) {
    return openersByEndpoint.containsKey(endpoint);
  }

  /** Dispatches a key (full file URI string) to the opener whose endpoint matches. */
  private FileHandle dispatchOpen(String key) {
    URI uri;
    try {
      uri = new URI(key);
    } catch (java.net.URISyntaxException ex) {
      throw new IllegalArgumentException("Invalid handle key: " + key, ex);
    }
    String endpoint = endpointKey(uri);
    HandleOpener opener = openersByEndpoint.get(endpoint);
    if (opener == null) {
      throw new IllegalStateException(
          "No opener registered for endpoint "
              + endpoint
              + "; did the owning CachedFileSystem initialize() succeed?");
    }
    return openHandle(opener, stringIds, key);
  }

  /** Tears down the installed bootstrap. Test-only — production JVMs install once and keep. */
  public static void uninstallForTesting() throws IOException {
    LOCK.lock();
    try {
      CacheBootstrap b = installed;
      installed = null;
      if (b == null) {
        return;
      }
      IOException primary = null;
      // executor shutdown — happens BEFORE handle drain so any in-flight prefetch task
      // finishes its run-finally (decrementPendingPrefetch + clearPendingPrefetchIf) against a
      // still-live RAM cache. Bounded await: 5s then shutdownNow + another 5s; persistent
      // stragglers are logged at ERROR. Bounds the pin-leak window per phase-5c.md §step 1.
      if (b.prefetchExecutor != null) {
        b.prefetchExecutor.shutdown();
        try {
          if (!b.prefetchExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
            b.prefetchExecutor.shutdownNow();
            if (!b.prefetchExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
              LOG.error(
                  "prefetch executor failed to terminate within 10s after uninstall; some"
                      + " in-flight tasks may leak pins until the JVM exits");
            }
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          b.prefetchExecutor.shutdownNow();
          // Re-await after shutdownNow so in-flight prefetch workers run their run-finally
          // (decrement + clearPendingPrefetchIf) BEFORE we close the RAM cache below. Skipping
          // this allowed a post-interrupt PrefetchTask to call cache.findOrCreate on a closed
          // shard, producing NPEs in ITs that reuse the JVM (MiniDFSCluster suite). The
          // interrupt flag was already restored, so suppress further interrupts here — we are
          // bounded by 5s regardless.
          try {
            if (!b.prefetchExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
              LOG.error(
                  "prefetch executor failed to terminate within 5s after interrupted uninstall;"
                      + " in-flight tasks may race ramCache.close()");
            }
          } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
          }
        }
      }
      // Drain handle factory — each FileHandle owns a lazily-opened inner-FS input stream that
      // the LRU would otherwise leak when the bootstrap reference is dropped. Runs AFTER the
      // prefetch executor settles so handle close-paths can't race in-flight prefetch tasks.
      try {
        b.handleFactory.closeAll();
      } catch (IOException ex) {
        primary = ex;
      }
      // Drop any remaining endpoint registrations (production decorators close via removeOpener,
      // but a test that forgot to close its CachedFileSystem would otherwise leak stale openers
      // into the next test's installIfNeeded — only relevant when the same JVM is reused).
      b.openersByEndpoint.clear();
      // Same rationale: a test that forgot to close its withScanId scope would leak a tracker
      // into the next test. Also drops any forgotten ThreadLocal slot on the current thread.
      b.scanTrackers.clear();
      b.currentScanId.remove();
      if (b.ssdCache != null) {
        try {
          b.ssdCache.close();
        } catch (IOException ex) {
          if (primary == null) primary = ex;
          else primary.addSuppressed(ex);
        }
      }
      try {
        b.ramCache.close();
      } catch (RuntimeException ex) {
        if (primary == null) primary = new IOException(ex);
        else primary.addSuppressed(ex);
      }
      AsyncDataCache.clearInstance();
      // Clear any staged BlockLocationsProvider so a subsequent test that does NOT install the
      // Spark extension doesn't inherit the previous test's affinity wiring.
      pendingBlockLocationsProvider = null;
      // FileIds has no clear API; the singleton remains but is reusable since StringIdMap holds no
      // OS resources. Test-only path; production never uninstalls.
      if (primary != null) {
        throw primary;
      }
    } finally {
      LOCK.unlock();
    }
  }

  /**
   * @return the RAM-tier cache; never null.
   */
  public AsyncDataCache ramCache() {
    return ramCache;
  }

  /**
   * @return the SSD-tier cache, or empty when {@code fs.cached.ssd.paths} is unset (RAM-only
   *     deployment).
   */
  public Optional<SsdCache> ssdCache() {
    return Optional.ofNullable(ssdCache);
  }

  public StringIdMap stringIds() {
    return stringIds;
  }

  public FileHandleFactory handleFactory() {
    return handleFactory;
  }

  public int loadQuantumBytes() {
    return loadQuantumBytes;
  }

  /**
   * prefetch executor, or {@code null} when {@code fs.cached.prefetch.enabled=false}. Callers MUST
   * null-check before submitting; the {@code CachingInputStream} admission gate does this and falls
   * back to the synchronous per-chunk path when null.
   */
  public java.util.concurrent.ThreadPoolExecutor prefetchExecutor() {
    return prefetchExecutor;
  }

  /** JVM-wide byte budget for in-flight prefetches; checked by the admission gate. */
  public long maxPendingPrefetchBytes() {
    return maxPendingPrefetchBytes;
  }

  /**
   * Live-view bootstrap-level {@link org.apache.hadoop.fs.statistics.IOStatistics} surface
   * aggregating the merged-from-stream {@code aggregateIoStats} totals AND registry-snapshot gauges
   * ({@code scanTrackerEntries}, {@code scanTrackerMaxEntries}, {@code pendingPrefetchBytes}).
   * Operators wire this into a JMX / Prometheus exporter for the cache-wide observability surface;
   * per-stream stats remain on {@code FSDataInputStream .getIOStatistics()}.
   */
  public org.apache.hadoop.fs.statistics.IOStatistics getIOStatistics() {
    return IoStatisticsAdapter.forBootstrap(this);
  }

  /**
   * Returns the cached heap-pressure bit (true ⇔ heap is &gt;90% full). Refreshes at most once per
   * {@link CachedFsConfig#PREFETCH_HEAP_PRESSURE_TTL_MS} via a single-CAS-winner pattern: only one
   * thread per TTL window calls {@link java.lang.management.MemoryMXBean#getHeapMemoryUsage};
   * concurrent callers read the cached bit. Bounds the MBean call rate to {@code 1 / TTL} across
   * the entire JVM regardless of concurrent stream count.
   */
  public boolean isHeapPressureHigh() {
    long now = System.nanoTime();
    long prev = heapPressureLastCheckedNs.get();
    if (now - prev > heapPressureTtlNs && heapPressureLastCheckedNs.compareAndSet(prev, now)) {
      java.lang.management.MemoryUsage u =
          java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
      long max = u.getMax();
      if (max <= 0L) {
        // No defined heap limit (e.g. -Xmx unset on some JVMs): treat as never-pressured.
        heapPressureActive = false;
      } else {
        heapPressureActive = u.getUsed() * 10L > max * 9L; // >90% used
      }
    }
    return heapPressureActive;
  }

  /**
   * Returns the current {@link ReadFileFactory}. Called by {@code
   * CachedFileSystem.openHandleForKey} on every {@code FileHandleFactory} miss to materialize the
   * underlying {@code ReadFile}.
   */
  public ReadFileFactory readFileFactory() {
    return readFileFactory;
  }

  /**
   * Test-only mutator: swaps the production {@link ReadFileFactory} for a stub and returns an
   * {@link AutoCloseable} that restores the prior factory on close. Use as {@code try (var ignored
   * = bootstrap.setReadFileFactoryForTesting(stub)) { … }} so a test crash inside the block does
   * not leave a stale factory installed for the next test.
   *
   * <p><b>NOT thread-safe.</b> Concurrent test threads invoking this seam can race on the captured
   * {@code prior} reference. Repository's Surefire default ({@code forkCount=1, reuseForks=true},
   * NO {@code parallel}) already serializes test execution; tests that opt into Surefire {@code
   * parallel=classes} or {@code threadCount>1} MUST annotate the seam-using test class with
   * {@code @Execution(SAME_THREAD)}.
   */
  AutoCloseable setReadFileFactoryForTesting(ReadFileFactory factory) {
    Objects.requireNonNull(factory, "factory");
    ReadFileFactory prior = this.readFileFactory;
    this.readFileFactory = factory;
    return () -> this.readFileFactory = prior;
  }

  /**
   * Returns the active {@link BlockLocationsProvider}, or {@code null} when none is installed (the
   * decorator returns inner-FS locations unchanged).
   */
  public BlockLocationsProvider blockLocationsProvider() {
    return blockLocationsProvider;
  }

  /**
   * Installs a {@link BlockLocationsProvider}. Pass {@code null} to revert to the default
   * passthrough. Called by the {@code cached-fs-spark} module's Spark extension at startup via
   * {@link #stageBlockLocationsProvider(BlockLocationsProvider)}; direct callers should prefer the
   * static stage method so the install/stage race is centrally locked.
   *
   * <p>The setter itself acquires {@link #LOCK} for ordering against the install-time read in
   * {@link #installIfNeeded(Configuration)} — so a write through this method and a write through
   * the staging path can never interleave to leave pending+installed in inconsistent states.
   */
  public void setBlockLocationsProvider(BlockLocationsProvider provider) {
    LOCK.lock();
    try {
      this.blockLocationsProvider = provider;
    } finally {
      LOCK.unlock();
    }
  }

  /**
   * Stages a {@link BlockLocationsProvider} for installation by the NEXT {@link
   * #installIfNeeded(Configuration)} call. Used by the {@code cached-fs-spark} module's Spark
   * extension when it fires BEFORE any {@code CachedFileSystem.initialize} — the staged provider is
   * picked up the moment the bootstrap comes online so block-location rewrites apply from the very
   * first file open.
   *
   * <p>If the bootstrap is already installed, this delegates straight to {@link
   * #setBlockLocationsProvider(BlockLocationsProvider)} on the live instance. Idempotent across
   * staging-then-install sequences.
   */
  public static void stageBlockLocationsProvider(BlockLocationsProvider provider) {
    // Take LOCK around the stage-vs-install branch so installIfNeeded cannot publish the
    // bootstrap between our `installed` read and our pendingBlockLocationsProvider write. Without
    // the lock, a stage that interleaves with the very first installIfNeeded loses the provider
    // permanently — the bootstrap installs without a provider AND every subsequent stage call
    // takes the early-return branch since `installed` is now non-null.
    LOCK.lock();
    try {
      CacheBootstrap snapshot = installed;
      if (snapshot != null) {
        snapshot.setBlockLocationsProvider(provider);
        return;
      }
      pendingBlockLocationsProvider = provider;
    } finally {
      LOCK.unlock();
    }
  }

  /**
   * Bootstrap-level aggregate IO stats. Per-stream {@code IoStatistics} snapshots are merged here
   * on {@code CachingInputStream.close()} (the close-merge wiring). The aggregate is intentionally
   * lossy under crashes — streams that never close don't contribute, matching velox parity.
   */
  public AggregatedIoStatistics aggregateIoStats() {
    return aggregateIoStats;
  }

  // --- scan-tracker registry ----------------------------------

  /**
   * Returns the (possibly newly-created) {@link ScanTracker} for {@code scanId}. {@code null},
   * empty, and blank inputs are normalized to {@code "default"}. Returned tracker lives until the
   * matching {@link #withScanId(String)} close OR an explicit {@link #removeScanTracker(String)}.
   *
   * <p>When the master toggle {@code fs.cached.scan-tracker.enabled=false} is set at install time,
   * the caller's {@code CachedFileSystem.open()} resolves the toggle and substitutes {@link
   * ScanTracker#DISABLED}; this method itself ignores the toggle.
   */
  public ScanTracker trackerFor(String scanId) {
    String normalized = normalizeScanId(scanId);
    return scanTrackers.computeIfAbsent(normalized, k -> new ScanTracker(k, this.loadQuantumBytes));
  }

  /**
   * Removes the tracker entry for {@code scanId}, making its inner {@code TrackingData} map
   * GC-eligible. Idempotent: missing/already-removed entries return cleanly without throwing.
   * Thread-safe — may be called from any thread (e.g. Spark {@code TaskCompletionListener}).
   */
  public void removeScanTracker(String scanId) {
    scanTrackers.remove(normalizeScanId(scanId));
  }

  /** Number of distinct scanIds currently tracked. Used by tests and a future dynamic gauge. */
  public int scanTrackerCount() {
    return scanTrackers.size();
  }

  /**
   * Total {@code TrackingId} entries summed across all live trackers. O(active scans). Used by
   * tests and a future dynamic gauge.
   */
  public long scanTrackerEntries() {
    long total = 0;
    for (ScanTracker t : scanTrackers.values()) {
      total += t.size();
    }
    return total;
  }

  /** Max entry count across all live trackers; pairs with {@link #scanTrackerEntries()}. */
  public long scanTrackerMaxEntries() {
    long max = 0;
    for (ScanTracker t : scanTrackers.values()) {
      int s = t.size();
      if (s > max) max = s;
    }
    return max;
  }

  // --- scanId ThreadLocal -------------------------------------

  /**
   * Returns the scanId currently set on this thread (via {@link #withScanId(String)}), or {@code
   * null} if no scope is active. Used by {@code CachedFileSystem.open()} as the highest precedence
   * in the scanId resolution chain.
   */
  public String currentScanId() {
    return currentScanId.get();
  }

  /**
   * Sets the per-thread scanId for the duration of the returned scope. On close: clears the
   * ThreadLocal slot AND calls {@link #removeScanTracker(String)} so the tracker is GC-eligible.
   *
   * <p><b>Stale-slot recovery.</b> If the thread already has a slot set on entry (a prior task
   * crashed before {@code close()}, common on Spark executor thread reuse), this method logs WARN
   * (first {@value #STALE_SLOT_WARN_CAP} occurrences per JVM, DEBUG thereafter), removes the stale
   * tracker, and bumps {@link AggregatedIoStatistics#staleScanIdRecoveries()}. Recovery preserves
   * the benign-leak property of the dangling-tracker design.
   *
   * <p><b>Single-thread close contract.</b> The returned {@code AutoCloseable}'s {@code close()}
   * MUST run on the same thread that called {@code withScanId}. Do NOT wire it into {@code
   * TaskContext.addTaskCompletionListener} — close may fire on a different thread and either no-op
   * the wrong slot or leak a wrong-thread orphan. For Spark integrators using the Configuration-key
   * path: {@link #removeScanTracker(String)} IS the right TCL payload (not ThreadLocal-bound;
   * thread-safe).
   */
  public AutoCloseable withScanId(String scanId) {
    String normalized = normalizeScanId(scanId);
    String stale = currentScanId.get();
    if (stale != null) {
      int cnt = staleSlotWarnCount.incrementAndGet();
      if (cnt <= STALE_SLOT_WARN_CAP) {
        LOG.warn(
            "stale scanId slot '{}' detected on thread {}; auto-recovering — likely a prior task"
                + " crashed between withScanId and close",
            stale,
            Thread.currentThread().getName());
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(
            "stale scanId slot '{}' (cap of {} WARN logs reached; counter still ticking)",
            stale,
            STALE_SLOT_WARN_CAP);
      }
      scanTrackers.remove(stale);
      aggregateIoStats.incStaleScanIdRecoveries();
    }
    currentScanId.set(normalized);
    Thread owner = Thread.currentThread();
    return () -> {
      // Single-thread close contract: wrong-thread close is a benign no-op. An orphan close on a
      // different thread (caller wired the AutoCloseable into Spark TaskCompletionListener
      // despite the javadoc warning) would otherwise evict a tracker that the owner thread is
      // still actively using via currentScanId.get() → trackerFor(); the owner thread would
      // silently get a fresh tracker and lose all density/readPct state. Same-thread close still
      // evicts the tracker unconditionally, preserving the releaseCurrentScanId nesting workflow
      // (which clears the ThreadLocal but expects scope close to evict the tracker).
      if (Thread.currentThread() != owner) {
        return;
      }
      if (normalized.equals(currentScanId.get())) {
        currentScanId.remove();
      }
      scanTrackers.remove(normalized);
    };
  }

  /**
   * Clears the per-thread scanId slot WITHOUT evicting any tracker. Use for intentional nesting:
   * {@code try (var outer = withScanId("a")) { releaseCurrentScanId(); try (var inner =
   * withScanId("b")) {…} }}. Outer's close still evicts "a" via {@link #removeScanTracker} (the
   * AutoCloseable captures scanId at construction time). Idempotent: no-op on missing slot.
   */
  public void releaseCurrentScanId() {
    currentScanId.remove();
  }

  private static String normalizeScanId(String scanId) {
    if (scanId == null) return "default";
    String trimmed = scanId.trim();
    return trimmed.isEmpty() ? "default" : trimmed;
  }

  /**
   * Returns the per-JVM TTL controller. Operators / schedulers call {@link
   * CacheTTLController#applyTTL} on this instance to drive aging. The controller is always
   * constructed; if no caller ever invokes {@code applyTTL}, the only overhead is a per-file
   * tracking map entry.
   */
  public CacheTTLController ttlController() {
    return ttlController;
  }

  /**
   * Strategy injected by {@link CachedFileSystem} to actually open the underlying Hadoop file —
   * keeps {@link CacheBootstrap} free of {@code org.apache.hadoop.fs} types so the core module can
   * test it without pulling Hadoop into its classpath.
   */
  @FunctionalInterface
  public interface HandleOpener {
    /** Opens a handle for the given URI key; called at most once per key in the LRU. */
    FileHandle open(String key) throws IOException;
  }

  private static FileHandle openHandle(HandleOpener opener, StringIdMap ids, String key) {
    try {
      // Generator runs without the cache lock (per CachedFactory contract). Side effects on the
      // returned handle's StringIdLease must be done by the opener itself, not here.
      FileHandle h = opener.open(key);
      // Defensive: the returned handle's uuid lease MUST point at `key` in the StringIdMap; if the
      // opener forgot to mint it, callers downstream would see a 0 fileNum. The interface contract
      // is that the opener mints the lease via `new StringIdLease(ids, key)`.
      if (h.uuid().id() == StringIdLease.EMPTY_ID) {
        try {
          h.close();
        } catch (IOException ignored) {
          // best-effort cleanup
        }
        throw new IllegalStateException("HandleOpener returned a handle with no uuid lease");
      }
      return h;
    } catch (IOException ex) {
      // CachedFactory expects RuntimeException from its generator. Wrap so single-flight cleanup
      // still runs and waiters are released.
      throw new java.io.UncheckedIOException(ex);
    }
  }
}
