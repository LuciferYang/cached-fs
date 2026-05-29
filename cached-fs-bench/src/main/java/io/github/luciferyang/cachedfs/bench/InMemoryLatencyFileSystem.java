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
package io.github.luciferyang.cachedfs.bench;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.locks.LockSupport;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * Synthetic Hadoop {@link org.apache.hadoop.fs.FileSystem} used by the prefetch benchmarks. Serves
 * a single in-memory file of {@link #fileSize} bytes and injects a {@link #readLatencyNanos}
 * first-byte park on reads that open a fresh region (see {@link #latencyStrideBytes}), modelling
 * storage round-trip latency (S3/HDFS first-byte) without touching disk or network.
 *
 * <p>The injected latency is the whole point: with a zero-latency backing store every prefetch
 * multiplier performs identically, because there is nothing to overlap. The latency models exactly
 * what prefetch exists to hide, so a larger in-flight budget (higher multiplier) can keep more
 * chunk loads outstanding and shorten the wall-clock scan — up to the point where the prefetch
 * thread pool, not the budget, becomes the bottleneck.
 *
 * <p>Configuration travels through {@code static} fields because Hadoop instantiates the inner FS
 * reflectively ({@code ReflectionUtils.newInstance}) with no constructor args. A benchmark fork
 * runs one configuration at a time, so static state is safe here; it would NOT be in production
 * code.
 *
 * <p>Extends {@link RawLocalFileSystem} only to inherit the {@code create}/{@code rename}/{@code
 * listStatus}/etc. plumbing that {@code initialize} touches; every read-path method is overridden
 * to serve the in-memory buffer instead of the local disk.
 */
public final class InMemoryLatencyFileSystem extends RawLocalFileSystem {

  /** File length the synthetic store reports and serves. Set before the FS is constructed. */
  public static volatile long fileSize = 256L << 20; // 256 MiB default

  /** First-byte latency park, in nanoseconds. {@code 0} parks not at all (clean baseline). */
  public static volatile long readLatencyNanos = 0L;

  /**
   * Granularity at which {@link #readLatencyNanos} is charged, in bytes. The cache fills each load
   * quantum as ~{@code quantum/4KiB} separate page-sized positioned reads (see {@code CacheEntry}'s
   * paged storage), but against real object storage only the FIRST read into a fresh region pays
   * first-byte latency — the rest stream cheaply from the already-open connection's read-ahead
   * buffer. Modelling that, latency is charged only when a read's offset lands on a multiple of
   * this stride (the chunk's leading page); sequential follow-on pages are free.
   *
   * <p>Set this to the load quantum so each chunk load costs exactly one round-trip, independent of
   * how many pages it is scattered across. {@code 0} (or negative) reverts to charging every read —
   * a worst-case "every page is a cold round-trip" model. Position-based and stateless, so it stays
   * correct when the consumer and prefetch threads interleave reads on the shared stream.
   */
  public static volatile long latencyStrideBytes = 0L;

  @Override
  public FSDataInputStream open(Path f, int bufferSize) {
    return new FSDataInputStream(
        new InMemoryStream(fileSize, readLatencyNanos, latencyStrideBytes));
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (fileSize < 0) {
      throw new FileNotFoundException("synthetic file size not configured: " + f);
    }
    // blockSize 64 MiB, replication 1; mtime/atime 0 — none of these influence the read path the
    // benchmark exercises, only length matters to the cache's chunk math.
    return new FileStatus(fileSize, false, 1, 64L << 20, 0L, f);
  }

  /**
   * {@link FSInputStream} over a virtual buffer. Bytes are synthesized on read ({@code (offset +
   * i)} mod 251, a prime, so adjacent chunks differ) rather than materialized, so the store costs
   * O(1) memory regardless of {@link #fileSize}. {@link FSInputStream}'s default {@code readFully}
   * delegates to {@link #read(long, byte[], int, int)}, so injecting latency there covers the
   * positioned-read path {@code HadoopReadFile} uses.
   */
  private static final class InMemoryStream extends FSInputStream {
    private final long size;
    private final long latencyNanos;
    private final long latencyStride;
    private long pos;

    InMemoryStream(long size, long latencyNanos, long latencyStride) {
      this.size = size;
      this.latencyNanos = latencyNanos;
      this.latencyStride = latencyStride;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) {
      if (position >= size) {
        return -1;
      }
      // Charge first-byte latency only when this read opens a fresh region (offset on a stride
      // boundary). Sequential follow-on page reads within the same chunk are free, modelling a
      // real connection's read-ahead. stride <= 0 → charge every read (worst-case model).
      if (latencyNanos > 0L && (latencyStride <= 0L || position % latencyStride == 0L)) {
        LockSupport.parkNanos(latencyNanos);
      }
      int n = (int) Math.min(length, size - position);
      for (int i = 0; i < n; i++) {
        buffer[offset + i] = (byte) ((position + i) % 251);
      }
      return n;
    }

    @Override
    public synchronized int read() {
      if (pos >= size) {
        return -1;
      }
      byte[] one = new byte[1];
      int n = read(pos, one, 0, 1);
      if (n <= 0) {
        return -1;
      }
      pos++;
      return one[0] & 0xff;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) {
      int n = read(pos, b, off, len);
      if (n > 0) {
        pos += n;
      }
      return n;
    }

    @Override
    public synchronized void seek(long p) {
      this.pos = p;
    }

    @Override
    public synchronized long getPos() {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }
  }
}
