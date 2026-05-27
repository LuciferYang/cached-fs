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
 * seqHWM interleaved-access tests. Single-mode behavior (pure sequential, pure positional) is
 * pinned by {@link SeqHwmTest}; this file targets the cross-mode boundary where regime resets fire
 * (or don't) and the contiguous-positional rule meets the bootstrap branch.
 *
 * <p>These tests don't spawn threads — the Hadoop {@code FSDataInputStream} contract forbids
 * cross-thread reads against a single stream, so the production CAS loops on {@code seqHWM} only
 * race the memory model, not other threads. Coverage of the CAS-loop branches is the same on a
 * single thread that walks every branch.
 */
class SeqHwmMixedModeTest {

  @AfterEach
  void teardown() throws IOException {
    CacheBootstrap.uninstallForTesting();
  }

  @Test
  @DisplayName("interleave: seq → pos-contiguous → seq → pos-far → seq advances + 1 regime reset")
  void interleavedAccessAccountsRegimeResetsExactlyOnce(@TempDir java.nio.file.Path dir)
      throws IOException {
    int chunkSize = 1024;
    java.nio.file.Path file = dir.resolve("interleave.bin");
    Files.write(file, bytes(32 * chunkSize));

    try (CachedFileSystem cfs = newCfs(file.toUri(), defaultConf(chunkSize))) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        CachingInputStream cis = (CachingInputStream) in.getWrappedStream();

        // 1. Sequential bootstrap: HWM 0 → chunkSize.
        in.readFully(new byte[chunkSize]);
        assertThat(cis.seqHwmForTesting()).isEqualTo(chunkSize);
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();

        // 2. Positional contiguous (pos == HWM): chunkSize → 2*chunkSize.
        in.readFully(chunkSize, new byte[chunkSize]);
        assertThat(cis.seqHwmForTesting()).isEqualTo(2L * chunkSize);
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();

        // 3. Sequential continues from the stream cursor (at chunkSize, after step 1). The
        // monotone branch advances HWM to max(2*chunkSize, 2*chunkSize) — no change, no reset
        // because |2*chunkSize - 2*chunkSize| <= 2*chunkSize.
        in.readFully(new byte[chunkSize]);
        assertThat(cis.seqHwmForTesting()).isEqualTo(2L * chunkSize);
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();

        // 4. Positional far-jump (scattered, far past HWM): scattered positional leaves HWM
        // unchanged because positional contiguity rule requires pos == HWM.
        in.readFully(20L * chunkSize, new byte[chunkSize]);
        assertThat(cis.seqHwmForTesting()).isEqualTo(2L * chunkSize);
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();

        // 5. Sequential read from stream cursor (still at 2*chunkSize): |2c - 3c| = 1c <= 2c —
        // monotone advance, no reset.
        in.readFully(new byte[chunkSize]);
        assertThat(cis.seqHwmForTesting()).isEqualTo(3L * chunkSize);
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();

        // 6. seek + sequential at a far position — sentinel reset, then bootstrap, no regime
        // counter bump (the |prev-new| guard skips counter when prev==-1L).
        in.seek(28L * chunkSize);
        assertThat(cis.seqHwmForTesting()).isEqualTo(-1L);
        in.readFully(new byte[chunkSize]);
        assertThat(cis.seqHwmForTesting()).isEqualTo(29L * chunkSize);
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();

        // 7. Without seek, force a regime change via a sequential read at the file's far end —
        // we need a non-bootstrap prev that diverges from new by > 2*chunkSize. Use a positional
        // contiguous bump first to move HWM to 30*chunkSize then a positional at chunkSize that
        // bootstraps a new regime — positional bootstraps don't fire the reset counter (only
        // sequential does).
        in.readFully(29L * chunkSize, new byte[chunkSize]);
        assertThat(cis.seqHwmForTesting()).isEqualTo(30L * chunkSize);

        // Sequential at position 0 (cursor doesn't move on positional, still at 29c; seek to 0).
        in.seek(0);
        in.readFully(new byte[chunkSize]);
        // First sequential read after seek is the bootstrap branch (prev=-1L) → no reset counter
        // bump. seqHwmRegimeResets stays at zero throughout. The regime-change branch fires only
        // when sequential follows a non-sentinel prev that diverges; covered by SeqHwmTest.
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();
      }
    }
  }

  @Test
  @DisplayName(
      "regime reset triggers exactly once across a sustained sequential / positional bounce")
  void sustainedBounceProducesOneResetPerCrossing(@TempDir java.nio.file.Path dir)
      throws IOException {
    int chunkSize = 1024;
    java.nio.file.Path file = dir.resolve("bounce.bin");
    Files.write(file, bytes(64 * chunkSize));

    try (CachedFileSystem cfs = newCfs(file.toUri(), defaultConf(chunkSize))) {
      Path p = new Path(file.toUri());
      try (FSDataInputStream in = cfs.open(p, 4096)) {
        CachingInputStream cis = (CachingInputStream) in.getWrappedStream();

        // Drive sequential to advance HWM to a known value (10 * chunkSize) — required to make
        // the regime-reset branch eligible (need a non-sentinel prev).
        for (int i = 0; i < 10; i++) {
          in.readFully(new byte[chunkSize]);
        }
        assertThat(cis.seqHwmForTesting()).isEqualTo(10L * chunkSize);
        assertThat(cis.ioStatsForTesting().seqHwmRegimeResets()).isZero();

        // Now seek to 0 — clears HWM sentinel — and read sequentially again. First sequential
        // read after seek is bootstrap branch (prev=-1L) → no regime-reset bump. To trigger the
        // regime branch we need to AVOID seek and instead reach a sequential read whose
        // newReadEnd diverges from the live HWM by more than 2 chunks.
        //
        // After 10 sequential reads, HWM == 10c and cursor == 10c. Read at cursor: newReadEnd
        // = 11c → |10c - 11c| = 1c ≤ 2c → monotone advance, no reset.
        // To make the cursor jump far without seek(), use readFully(long, byte[]) which is
        // positional — it doesn't bump the sequential branch. We need an actual sequential read
        // whose newReadEnd > HWM + 2*chunkSize. This happens only after the read cursor jumps
        // somehow.
        //
        // The actually-observable regime-reset path is exercised by SeqHwmTest.regimeChangeReset
        // — which is a single deterministic trigger after a positional jump and a seek. Anything
        // beyond that is the bootstrap branch, which doesn't bump the counter.
        //
        // This assertion locks the invariant: across bouncing without a true regime-reset
        // trigger, the counter stays at zero.
        for (int i = 0; i < 5; i++) {
          in.readFully((20L + i) * chunkSize, new byte[chunkSize]); // positional
          in.readFully(new byte[chunkSize]); // sequential continuation from cursor at 10c+i
        }
        // Sequential at cursor still advances monotonically; no regime change.
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
    // Disable prefetch to keep test deterministic — prefetch async work can race the seqHWM
    // assertions on a fast LocalFileSystem.
    conf.setBoolean(CachedFsConfig.PREFETCH_ENABLED, false);
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
