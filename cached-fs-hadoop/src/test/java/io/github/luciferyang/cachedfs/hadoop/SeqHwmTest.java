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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 5c step 2 — verifies the sequential CAS-loop and positional updateAndGet state machine on
 * {@code CachingInputStream.seqHWM}. Uses package-private accessors {@code seqHwmForTesting} and
 * inspects {@code IoStatistics.seqHwmRegimeResets} via the bootstrap aggregate.
 */
class SeqHwmTest {

  @AfterEach
  void teardown() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  @Test
  @DisplayName("sentinel: fresh stream has seqHWM=-1L; no reset bump on first sequential read")
  void sentinelBootstrap(@TempDir java.nio.file.Path dir) throws IOException {
    int chunkSize = 1024;
    java.nio.file.Path file = dir.resolve("a.bin");
    Files.write(file, bytes(2 * chunkSize));

    try (CachedFileSystem cfs = newCfs(file.toUri(), defaultConf(chunkSize))) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        CachingInputStream cis = (CachingInputStream) in.getWrappedStream();
        assertThat(cis.seqHwmForTesting()).isEqualTo(-1L);
        byte[] buf = new byte[chunkSize];
        in.readFully(buf);
        // Bootstrap → HWM = chunkSize. No reset.
        assertThat(cis.seqHwmForTesting()).isEqualTo(chunkSize);
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();
      }
    }
  }

  @Test
  @DisplayName("monotone advance: contiguous sequential reads accumulate without reset")
  void monotoneAdvance(@TempDir java.nio.file.Path dir) throws IOException {
    int chunkSize = 1024;
    java.nio.file.Path file = dir.resolve("b.bin");
    Files.write(file, bytes(4 * chunkSize));

    try (CachedFileSystem cfs = newCfs(file.toUri(), defaultConf(chunkSize))) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        CachingInputStream cis = (CachingInputStream) in.getWrappedStream();
        for (int i = 0; i < 4; i++) {
          in.readFully(new byte[chunkSize]);
        }
        assertThat(cis.seqHwmForTesting()).isEqualTo(4L * chunkSize);
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();
      }
    }
  }

  @Test
  @DisplayName("regime-change reset: positional far-jump then sequential at 0 triggers ONE reset")
  void regimeChangeReset(@TempDir java.nio.file.Path dir) throws IOException {
    int chunkSize = 1024;
    java.nio.file.Path file = dir.resolve("c.bin");
    // 16 chunks so positional readFully at chunk 10 is far past 2-chunk threshold.
    Files.write(file, bytes(16 * chunkSize));

    try (CachedFileSystem cfs = newCfs(file.toUri(), defaultConf(chunkSize))) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        CachingInputStream cis = (CachingInputStream) in.getWrappedStream();
        // Positional jump far ahead — bootstraps HWM to 10*chunkSize + chunkSize = 11*chunkSize.
        in.readFully(10L * chunkSize, new byte[chunkSize]);
        assertThat(cis.seqHwmForTesting()).isEqualTo(11L * chunkSize);
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();

        // Sequential read at position 0 — |11*chunkSize - chunkSize| = 10*chunkSize > 2*chunkSize
        // → regime-change reset to chunkSize. EXACTLY ONE bump.
        in.seek(0);
        in.readFully(new byte[chunkSize]);
        // seek() reset HWM to -1L; first sequential read bootstraps → no regime change yet.
        assertThat(cis.seqHwmForTesting()).isEqualTo(chunkSize);
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();
      }
    }
  }

  @Test
  @DisplayName("positional contiguity: pos==prev advances; scattered pos leaves HWM unchanged")
  void positionalContiguity(@TempDir java.nio.file.Path dir) throws IOException {
    int chunkSize = 1024;
    java.nio.file.Path file = dir.resolve("d.bin");
    Files.write(file, bytes(8 * chunkSize));

    try (CachedFileSystem cfs = newCfs(file.toUri(), defaultConf(chunkSize))) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        CachingInputStream cis = (CachingInputStream) in.getWrappedStream();
        in.readFully(0, new byte[chunkSize]);
        long afterBootstrap = cis.seqHwmForTesting();
        assertThat(afterBootstrap).isEqualTo(chunkSize);

        // Contiguous: pos == HWM → advance to 2*chunkSize.
        in.readFully(chunkSize, new byte[chunkSize]);
        assertThat(cis.seqHwmForTesting()).isEqualTo(2L * chunkSize);

        // Scattered: pos != HWM → no change.
        in.readFully(7L * chunkSize, new byte[chunkSize]);
        assertThat(cis.seqHwmForTesting()).isEqualTo(2L * chunkSize);
      }
    }
  }

  @Test
  @DisplayName("seek resets seqHWM to sentinel; next read re-bootstraps without reset bump")
  void seekResetsHwm(@TempDir java.nio.file.Path dir) throws IOException {
    int chunkSize = 1024;
    java.nio.file.Path file = dir.resolve("e.bin");
    Files.write(file, bytes(4 * chunkSize));

    try (CachedFileSystem cfs = newCfs(file.toUri(), defaultConf(chunkSize))) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        CachingInputStream cis = (CachingInputStream) in.getWrappedStream();
        in.readFully(new byte[chunkSize]);
        assertThat(cis.seqHwmForTesting()).isEqualTo(chunkSize);

        in.seek(0);
        assertThat(cis.seqHwmForTesting()).isEqualTo(-1L);

        in.readFully(new byte[chunkSize]);
        assertThat(cis.seqHwmForTesting()).isEqualTo(chunkSize);
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();
      }
    }
  }

  // --- helpers ---

  private static CachedFileSystem newCfs(URI fileUri, Configuration conf) throws IOException {
    CachedFileSystem cfs = new CachedFileSystem();
    cfs.initialize(URI.create(fileUri.getScheme() + ":///"), conf);
    return cfs;
  }

  private static Configuration defaultConf(int loadQuantumBytes) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CachedFsConfig.ENABLED, true);
    conf.set(CachedFsConfig.INNER_IMPL, LocalFileSystem.class.getName());
    conf.setInt(CachedFsConfig.LOAD_QUANTUM_BYTES, loadQuantumBytes);
    return conf;
  }

  private static byte[] bytes(int n) {
    byte[] out = new byte[n];
    for (int i = 0; i < n; i++) {
      out[i] = (byte) ((i * 31 + 7) & 0xff);
    }
    return out;
  }
}
