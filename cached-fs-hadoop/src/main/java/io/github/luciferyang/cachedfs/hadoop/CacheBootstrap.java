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
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;

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

  private static final ReentrantLock LOCK = new ReentrantLock();
  private static volatile CacheBootstrap installed;

  private final AsyncDataCache ramCache;
  private final SsdCache ssdCache;
  private final StringIdMap stringIds;
  private final FileHandleFactory handleFactory;
  private final int loadQuantumBytes;

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
      int handleCapacity) {
    this.ramCache = ramCache;
    this.ssdCache = ssdCache;
    this.stringIds = stringIds;
    this.loadQuantumBytes = loadQuantumBytes;
    // FileHandleFactory's generator captures `this` so it can route each key through the registry.
    this.handleFactory = new FileHandleFactory(handleCapacity, this::dispatchOpen);
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
      try {
        StringIdMap ids = new StringIdMap();
        SsdCache.Config ssdCfg = CachedFsConfig.ssdConfig(conf);
        if (ssdCfg != null) {
          ssd = new SsdCache(ssdCfg, ids);
        }
        int handleCap = CachedFsConfig.handleCacheCapacity(conf);
        int quantum = CachedFsConfig.loadQuantumBytes(conf);
        CacheBootstrap b = new CacheBootstrap(ram, ssd, ids, quantum, handleCap);
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
        installed = b;
        return b;
      } catch (IOException | RuntimeException | Error ex) {
        // Roll back partial construction; the JVM stays in the pre-install state so a retry
        // gets a clean slate.
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
      // Drain handle factory FIRST — each FileHandle owns a lazily-opened inner-FS input stream
      // that the LRU would otherwise leak when the bootstrap reference is dropped. Run before tier
      // shutdown so handle close-paths still see a live RAM/SSD cache if they touch it.
      try {
        b.handleFactory.closeAll();
      } catch (IOException ex) {
        primary = ex;
      }
      // Drop any remaining endpoint registrations (production decorators close via removeOpener,
      // but a test that forgot to close its CachedFileSystem would otherwise leak stale openers
      // into the next test's installIfNeeded — only relevant when the same JVM is reused).
      b.openersByEndpoint.clear();
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
      // FileIds has no clear API; the singleton remains but is reusable since StringIdMap holds no
      // OS resources. Test-only path; production never uninstalls.
      if (primary != null) {
        throw primary;
      }
    } finally {
      LOCK.unlock();
    }
  }

  public AsyncDataCache ramCache() {
    return ramCache;
  }

  public SsdCache ssdCache() {
    return ssdCache;
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
