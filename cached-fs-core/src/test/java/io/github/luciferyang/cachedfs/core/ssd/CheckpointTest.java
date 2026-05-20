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
package io.github.luciferyang.cachedfs.core.ssd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointTest {

  @Test
  @DisplayName("write/read roundtrip preserves all fields (with checksum)")
  void roundtripWithChecksum(@TempDir Path dir) throws IOException {
    Path cpt = dir.resolve("a.cpt");
    Path tmp = dir.resolve("a.cpt.tmp");
    // Scores array sized at maxRegions (8); only the first numRegions (3) carry meaningful data.
    double[] scores = new double[8];
    scores[0] = 1.5;
    scores[1] = 2.25;
    scores[2] = 3.0;
    var snap =
        new Checkpoint.Snapshot(
            8,
            3,
            scores,
            Map.of(10L, "file://x", 11L, "file://y"),
            List.of(
                new Checkpoint.Entry(10L, 0L, SsdRun.of(0L, 4096).fileBits(), 0xDEADBEEF),
                new Checkpoint.Entry(
                    11L, 4096L, SsdRun.of(67108864L, 8192).fileBits(), 0xCAFEBABE)));
    Checkpoint.write(cpt, tmp, snap, /* checksumEnabled= */ true);

    Checkpoint.Snapshot read = Checkpoint.read(cpt);
    assertThat(read.maxRegions()).isEqualTo(8);
    assertThat(read.numRegions()).isEqualTo(3);
    assertThat(read.regionScores()).hasSize(8);
    assertThat(read.regionScores()[0]).isEqualTo(1.5);
    assertThat(read.regionScores()[2]).isEqualTo(3.0);
    assertThat(read.fileIdToName()).containsEntry(10L, "file://x").containsEntry(11L, "file://y");
    assertThat(read.entries()).hasSize(2);
    assertThat(read.entries().get(0).checksum()).isEqualTo(0xDEADBEEF);
  }

  @Test
  @DisplayName("write/read roundtrip without checksum (CFS1 magic)")
  void roundtripPlain(@TempDir Path dir) throws IOException {
    Path cpt = dir.resolve("b.cpt");
    Path tmp = dir.resolve("b.cpt.tmp");
    double[] scores = new double[4];
    scores[0] = 7.0;
    var snap =
        new Checkpoint.Snapshot(
            4,
            1,
            scores,
            Map.of(),
            List.of(new Checkpoint.Entry(1L, 0L, SsdRun.of(0L, 1024).fileBits(), 0)));
    Checkpoint.write(cpt, tmp, snap, /* checksumEnabled= */ false);
    Checkpoint.Snapshot read = Checkpoint.read(cpt);
    assertThat(read.regionScores()).hasSize(4);
    assertThat(read.entries()).hasSize(1);
    assertThat(read.entries().get(0).checksum()).isZero();
  }

  @Test
  @DisplayName("read fails on bad magic")
  void badMagic(@TempDir Path dir) throws IOException {
    Path cpt = dir.resolve("bogus.cpt");
    try (FileChannel ch =
        FileChannel.open(cpt, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      ByteBuffer header = ByteBuffer.allocate(4).putInt(0xBADBADBA);
      header.flip();
      ch.write(header);
    }
    assertThatThrownBy(() -> Checkpoint.read(cpt))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Bad checkpoint magic");
  }

  @Test
  @DisplayName("eviction log roundtrip — appended indices read back")
  void evictionLogRoundtrip(@TempDir Path dir) throws IOException {
    Path log = dir.resolve("a.log");
    Checkpoint.truncateLog(log);
    Checkpoint.appendEvictions(log, new int[] {2, 5});
    Checkpoint.appendEvictions(log, new int[] {7});
    List<Integer> evicted = Checkpoint.readEvictedRegions(log);
    assertThat(evicted).containsExactly(2, 5, 7);
  }

  @Test
  @DisplayName("readEvictedRegions on missing log returns empty")
  void evictionLogMissing(@TempDir Path dir) throws IOException {
    Path log = dir.resolve("missing.log");
    assertThat(Checkpoint.readEvictedRegions(log)).isEmpty();
  }

  @Test
  @DisplayName("readEvictedRegions fails on bad log magic")
  void evictionLogBadMagic(@TempDir Path dir) throws IOException {
    Path log = dir.resolve("badmagic.log");
    Files.write(log, new byte[] {0, 0, 0, 0});
    assertThatThrownBy(() -> Checkpoint.readEvictedRegions(log))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Bad eviction-log magic");
  }

  @Test
  @DisplayName("read fails when numRegions exceeds maxRegions")
  void invalidRegionCounts(@TempDir Path dir) throws IOException {
    Path cpt = dir.resolve("invalid.cpt");
    try (FileChannel ch =
        FileChannel.open(cpt, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      ByteBuffer header = ByteBuffer.allocate(12);
      header.putInt(0x43465331); // CFS1
      header.putInt(2); // maxRegions
      header.putInt(5); // numRegions > maxRegions
      header.flip();
      ch.write(header);
    }
    assertThatThrownBy(() -> Checkpoint.read(cpt))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Invalid region counts");
  }

  @Test
  @DisplayName("read fails on missing MAP_MARKER")
  void missingMapMarker(@TempDir Path dir) throws IOException {
    Path cpt = dir.resolve("nomarker.cpt");
    try (FileChannel ch =
        FileChannel.open(cpt, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      // 4 (magic) + 4 (maxRegions) + 4 (numRegions) + 8 (one score for maxRegions=1) + 4 (numFiles)
      // + 8 (wrong marker)
      ByteBuffer buf = ByteBuffer.allocate(4 + 4 + 4 + 8 + 4 + 8);
      buf.putInt(0x43465331); // CFS1
      buf.putInt(1); // maxRegions
      buf.putInt(0); // numRegions = 0
      buf.putDouble(0.0); // single score (maxRegions doubles)
      buf.putInt(0); // numFiles = 0
      buf.putLong(0xDEADBEEFL); // wrong marker
      buf.flip();
      ch.write(buf);
    }
    assertThatThrownBy(() -> Checkpoint.read(cpt))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Missing map marker");
  }

  @Test
  @DisplayName("empty snapshot (numRegions=0) roundtrips with maxRegions-length scores")
  void emptySnapshotRoundtrip(@TempDir Path dir) throws IOException {
    Path cpt = dir.resolve("empty.cpt");
    Path tmp = dir.resolve("empty.cpt.tmp");
    var snap = new Checkpoint.Snapshot(4, 0, new double[4], Map.of(), List.of());
    Checkpoint.write(cpt, tmp, snap, false);
    Checkpoint.Snapshot read = Checkpoint.read(cpt);
    assertThat(read.numRegions()).isZero();
    assertThat(read.regionScores()).hasSize(4);
    assertThat(read.entries()).isEmpty();
    assertThat(read.fileIdToName()).isEmpty();
  }
}
