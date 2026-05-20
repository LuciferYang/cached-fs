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
 * <p><b>Wiring:</b> set {@code fs.<scheme>.impl=io.github.luciferyang.cachedfs.hadoop.CachedFileSystem}
 * plus {@code fs.cached.inner.impl=<original-impl-class>}. The decorator instantiates the inner FS
 * privately so consumers that resolve the scheme via {@link FileSystem#get(URI, Configuration)}
 * receive the decorator, not the inner.
 *
 * <p><b>Toggle:</b> {@code fs.cached.enabled=false} makes {@link #open} delegate straight to the
 * inner without consulting the cache — useful for A/B benchmarking without redeploying.
 *
 * <p><b>Phase 3b limitation:</b> the JVM-wide {@link CacheBootstrap} captures the FIRST decorator
 * instance's inner FS as the source of newly-opened files. Multi-scheme caching (e.g. caching both
 * {@code s3a://} and {@code hdfs://} in the same process) is deferred — Phase 4 will introduce a
 * scheme-keyed opener registry.
 */
public class CachedFileSystem extends FilterFileSystem {

  private boolean enabled;
  private URI uri;

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

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    this.uri = name;
    setConf(conf);
    FileSystem inner = createInner(name, conf);
    inner.initialize(name, conf);
    this.fs = inner;
    super.initialize(name, conf); // FilterFileSystem records statistics + sets working dir
    this.enabled = CachedFsConfig.isEnabled(conf);
    if (enabled) {
      CacheBootstrap.installIfNeeded(conf, this::openHandleForKey);
    }
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (!enabled) {
      return fs.open(f, bufferSize);
    }
    CacheBootstrap b = CacheBootstrap.get();
    if (b == null) {
      // Defensive: enabled was true at initialize but bootstrap got uninstalled (test path) —
      // fall back to direct read rather than NPE.
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
    }
    CachingInputStream cis = new CachingInputStream(ptr, b.ramCache(), b.loadQuantumBytes());
    return new FSDataInputStream(cis);
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
    StringIdMap ids = CacheBootstrap.get().stringIds();
    StringIdLease uuid = new StringIdLease(ids, key);
    StringIdLease groupId = parentLease(ids, p);
    return new FileHandle(rf, uuid, groupId);
  }

  private static StringIdLease parentLease(StringIdMap ids, Path p) {
    Path parent = p.getParent();
    if (parent == null) {
      return StringIdLease.empty(ids);
    }
    return new StringIdLease(ids, parent.toUri().toString());
  }
}
