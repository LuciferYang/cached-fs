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
package io.github.luciferyang.cachedfs.core.handle;

import io.github.luciferyang.cachedfs.core.id.StringIdLease;
import io.github.luciferyang.cachedfs.core.io.ReadFile;
import java.util.Objects;

/**
 * Per-file handle bundling the open {@link ReadFile} with its identity leases. Mirrors velox {@code
 * FileHandle} (FileHandle.h:38-50).
 *
 * <ul>
 *   <li>{@code uuid} — lease for the file path; its {@code id()} is the {@code fileNum} cache key
 *   <li>{@code groupId} — lease for the directory / Hive partition; used by group-level admission
 *       hooks (currently a stub in OSS — see velox §2.2)
 * </ul>
 *
 * <p>Closing a handle releases both leases and the underlying {@link ReadFile}.
 */
public final class FileHandle implements AutoCloseable {

  private final ReadFile readFile;
  private final StringIdLease uuid;
  private final StringIdLease groupId;

  public FileHandle(ReadFile readFile, StringIdLease uuid, StringIdLease groupId) {
    this.readFile = Objects.requireNonNull(readFile, "readFile");
    this.uuid = Objects.requireNonNull(uuid, "uuid");
    this.groupId = Objects.requireNonNull(groupId, "groupId");
  }

  public ReadFile readFile() {
    return readFile;
  }

  public long fileNum() {
    return uuid.id();
  }

  public long groupId() {
    return groupId.id();
  }

  public StringIdLease uuid() {
    return uuid;
  }

  public StringIdLease groupIdLease() {
    return groupId;
  }

  /**
   * Closes the underlying {@link ReadFile} and releases both string-id leases. All three are
   * attempted; if any throw, the others still run and any exception is propagated as suppressed.
   */
  @Override
  public void close() throws java.io.IOException {
    java.io.IOException primary = null;
    try {
      readFile.close();
    } catch (java.io.IOException ex) {
      primary = ex;
    } catch (RuntimeException ex) {
      primary = new java.io.IOException(ex);
    }
    try {
      uuid.close();
    } catch (RuntimeException ex) {
      if (primary == null) primary = new java.io.IOException(ex);
      else primary.addSuppressed(ex);
    }
    try {
      groupId.close();
    } catch (RuntimeException ex) {
      if (primary == null) primary = new java.io.IOException(ex);
      else primary.addSuppressed(ex);
    }
    if (primary != null) {
      throw primary;
    }
  }
}
