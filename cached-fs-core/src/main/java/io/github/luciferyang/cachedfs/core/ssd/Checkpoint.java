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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * On-disk checkpoint and eviction-log format for a {@link SsdFile} shard. Mirrors velox's {@code
 * .cpt} / {@code .log} pair (velox §4.6).
 *
 * <p>This is <strong>NOT</strong> wire-compatible with velox's format — we use Java big-endian
 * primitives via {@link DataInputStream}/{@link DataOutputStream} and our own magic header. The
 * field semantics match velox closely enough to support the same recovery semantics.
 *
 * <p>Format:
 *
 * <pre>{@code
 * [4-byte magic CFS1 (no checksum) or CFS2 (with checksum)]
 * [int32 maxRegions]
 * [int32 numRegions]
 * [double regionScores[numRegions]]
 * [int32 numFiles]
 * [{int64 fileId, utf-string name}+ × numFiles]
 * [int64 MAP_MARKER]
 * [{int64 fileNum, int64 offset, int64 fileBits[, int32 checksum]}+]
 * [int64 END_MARKER]
 * }</pre>
 *
 * <p>The eviction log is much simpler: a header magic followed by repeated {@code int32
 * regionIndex} appends. Each {@link SsdFile#evictRegions} call appends the evicted region indices
 * and {@code fsync}s before any region bytes are reused. On recovery, the log entries tell us which
 * entries from the checkpoint were already evicted and must not be replayed.
 */
final class Checkpoint {

  /** Magic for "no checksum" mode. */
  static final int CPT_MAGIC_PLAIN = 0x43465331; // "CFS1"

  /** Magic for "checksum" mode (per-entry CRC32C). */
  static final int CPT_MAGIC_CHECKSUM = 0x43465332; // "CFS2"

  /** Eviction-log magic. */
  static final int LOG_MAGIC = 0x43465345; // "CFSE"

  /** Marker between fileNum→name table and entries. Values reserved by StringIdMap allocation. */
  static final long MAP_MARKER = 0xFFFFFFFFFFFFFFFEL;

  /** Marker after the last entry; terminates the entries section. */
  static final long END_MARKER = 0xCBEDF11EL;

  private Checkpoint() {}

  /** Snapshot of cache state to be persisted to a checkpoint file. */
  record Snapshot(
      int maxRegions,
      int numRegions,
      double[] regionScores,
      Map<Long, String> fileIdToName,
      List<Entry> entries) {
    Snapshot {
      regionScores = regionScores.clone();
      fileIdToName = Map.copyOf(fileIdToName);
      entries = List.copyOf(entries);
    }
  }

  /** Per-entry record persisted in the checkpoint. */
  record Entry(long fileNum, long offset, long fileBits, int checksum) {}

  /**
   * Atomically writes {@code snap} to {@code cptPath} using {@code tmpPath} as the staging file.
   * Forces the staging file to disk via {@code fsync} before the atomic rename so a crash between
   * the rename and the next checkpoint can never surface a torn or zero-length file. The eviction
   * log {@code logPath} is truncated only after the checkpoint succeeds (caller's responsibility —
   * see {@link #truncateLog}).
   */
  static void write(Path cptPath, Path tmpPath, Snapshot snap, boolean checksumEnabled)
      throws IOException {
    try (FileChannel ch =
        FileChannel.open(
            tmpPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      // DataOutputStream is intentionally NOT in try-with-resources — closing it would close the
      // underlying Channels.newOutputStream and then the channel, defeating the explicit force().
      DataOutputStream out = new DataOutputStream(Channels.newOutputStream(ch));
      out.writeInt(checksumEnabled ? CPT_MAGIC_CHECKSUM : CPT_MAGIC_PLAIN);
      out.writeInt(snap.maxRegions());
      out.writeInt(snap.numRegions());
      for (double s : snap.regionScores()) {
        out.writeDouble(s);
      }
      out.writeInt(snap.fileIdToName().size());
      for (var e : snap.fileIdToName().entrySet()) {
        out.writeLong(e.getKey());
        out.writeUTF(e.getValue());
      }
      out.writeLong(MAP_MARKER);
      for (Entry e : snap.entries()) {
        out.writeLong(e.fileNum());
        out.writeLong(e.offset());
        out.writeLong(e.fileBits());
        if (checksumEnabled) {
          out.writeInt(e.checksum());
        }
      }
      out.writeLong(END_MARKER);
      out.flush();
      // fsync staging file BEFORE atomic rename — crash recovery never sees a zero-length .cpt.
      ch.force(true);
    }
    Files.move(
        tmpPath, cptPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
  }

  /** Reads the checkpoint from {@code cptPath}. Throws on parse failure. */
  static Snapshot read(Path cptPath) throws IOException {
    try (var in = new DataInputStream(Files.newInputStream(cptPath))) {
      int magic = in.readInt();
      boolean withChecksum;
      if (magic == CPT_MAGIC_PLAIN) withChecksum = false;
      else if (magic == CPT_MAGIC_CHECKSUM) withChecksum = true;
      else throw new IOException("Bad checkpoint magic: 0x" + Integer.toHexString(magic));
      int maxRegions = in.readInt();
      int numRegions = in.readInt();
      if (maxRegions < 0 || numRegions < 0 || numRegions > maxRegions) {
        throw new IOException("Invalid region counts: max=" + maxRegions + " num=" + numRegions);
      }
      // Scores are persisted at length maxRegions (matches velox SsdFile.cpp:848 and 1088),
      // not numRegions. Tail entries beyond numRegions are zero in normal operation.
      double[] scores = new double[maxRegions];
      for (int i = 0; i < maxRegions; i++) {
        scores[i] = in.readDouble();
      }
      int numFiles = in.readInt();
      Map<Long, String> fileIdToName = new HashMap<>(Math.max(16, numFiles));
      for (int i = 0; i < numFiles; i++) {
        long id = in.readLong();
        String name = in.readUTF();
        fileIdToName.put(id, name);
      }
      long mapMarker = in.readLong();
      if (mapMarker != MAP_MARKER) {
        throw new IOException("Missing map marker; got 0x" + Long.toHexString(mapMarker));
      }
      List<Entry> entries = new ArrayList<>();
      while (true) {
        long fileNum = in.readLong();
        if (fileNum == END_MARKER) break;
        long offset = in.readLong();
        long fileBits = in.readLong();
        int checksum = withChecksum ? in.readInt() : 0;
        entries.add(new Entry(fileNum, offset, fileBits, checksum));
      }
      return new Snapshot(maxRegions, numRegions, scores, fileIdToName, entries);
    }
  }

  /** Replaces the eviction log with a fresh, empty log file (header magic only). */
  static void truncateLog(Path logPath) throws IOException {
    try (FileChannel ch =
        FileChannel.open(
            logPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      DataOutputStream out = new DataOutputStream(Channels.newOutputStream(ch));
      out.writeInt(LOG_MAGIC);
      out.flush();
      ch.force(true);
    }
  }

  /**
   * Synchronously appends evicted region indices to the log and forces them to durable storage.
   * Called from {@code SsdFile.evictRegions} BEFORE the region bytes are reused.
   */
  static void appendEvictions(Path logPath, int[] regionIndices) throws IOException {
    try (FileChannel ch =
        FileChannel.open(logPath, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
      ByteBuffer buf = ByteBuffer.allocate(4 * regionIndices.length);
      for (int idx : regionIndices) buf.putInt(idx);
      buf.flip();
      while (buf.hasRemaining()) ch.write(buf);
      ch.force(false); // synchronous — log must be durable before region overwrite
    }
  }

  /**
   * Reads evicted region indices from the log. Returns the bag of region indices that have been
   * cleared since the last checkpoint; recovery uses this to skip stale entries from the checkpoint
   * that fall in those regions.
   */
  static List<Integer> readEvictedRegions(Path logPath) throws IOException {
    if (!Files.exists(logPath)) return List.of();
    try (var in = new DataInputStream(Files.newInputStream(logPath))) {
      int magic = in.readInt();
      if (magic != LOG_MAGIC) {
        throw new IOException("Bad eviction-log magic: 0x" + Integer.toHexString(magic));
      }
      List<Integer> out = new ArrayList<>();
      while (true) {
        try {
          out.add(in.readInt());
        } catch (EOFException eof) {
          return out;
        }
      }
    }
  }
}
