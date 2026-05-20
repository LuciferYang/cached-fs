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
 * </ul>
 *
 * <p>The first {@link #installIfNeeded} call wins; subsequent calls are no-ops regardless of the
 * Configuration they see. Operators that need to change cache settings must restart the JVM.
 */
public final class CacheBootstrap {

  private static final ReentrantLock LOCK = new ReentrantLock();
  private static volatile CacheBootstrap installed;

  private final AsyncDataCache ramCache;
  private final SsdCache ssdCache;
  private final StringIdMap stringIds;
  private final FileHandleFactory handleFactory;
  private final int loadQuantumBytes;

  private CacheBootstrap(
      AsyncDataCache ramCache,
      SsdCache ssdCache,
      StringIdMap stringIds,
      FileHandleFactory handleFactory,
      int loadQuantumBytes) {
    this.ramCache = ramCache;
    this.ssdCache = ssdCache;
    this.stringIds = stringIds;
    this.handleFactory = handleFactory;
    this.loadQuantumBytes = loadQuantumBytes;
  }

  /** Returns the installed bootstrap, or {@code null} if {@link #installIfNeeded} has not run. */
  public static CacheBootstrap get() {
    return installed;
  }

  /**
   * Installs the cache singletons if no installation exists yet. Idempotent: a second concurrent
   * call observes the existing installation and returns it unchanged. The {@code openerFactory} is
   * used to manufacture {@link FileHandle} instances for new URIs; in production this is wired to
   * the inner Hadoop {@link org.apache.hadoop.fs.FileSystem}.
   */
  public static CacheBootstrap installIfNeeded(
      Configuration conf, HandleOpener opener) throws IOException {
    CacheBootstrap snapshot = installed;
    if (snapshot != null) {
      return snapshot;
    }
    LOCK.lock();
    try {
      if (installed != null) {
        return installed;
      }
      AsyncDataCache ram = new AsyncDataCache(CachedFsConfig.ramOptions(conf));
      AsyncDataCache.setInstance(ram);

      StringIdMap ids = new StringIdMap();
      FileIds.setInstance(ids);

      SsdCache ssd = null;
      SsdCache.Config ssdCfg = CachedFsConfig.ssdConfig(conf);
      if (ssdCfg != null) {
        ssd = new SsdCache(ssdCfg, ids);
      }

      int handleCap = CachedFsConfig.handleCacheCapacity(conf);
      FileHandleFactory hf = new FileHandleFactory(handleCap, key -> openHandle(opener, ids, key));

      int quantum = CachedFsConfig.loadQuantumBytes(conf);
      CacheBootstrap b = new CacheBootstrap(ram, ssd, ids, hf, quantum);
      installed = b;
      return b;
    } finally {
      LOCK.unlock();
    }
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
      if (b.ssdCache != null) {
        try {
          b.ssdCache.close();
        } catch (IOException ex) {
          primary = ex;
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
