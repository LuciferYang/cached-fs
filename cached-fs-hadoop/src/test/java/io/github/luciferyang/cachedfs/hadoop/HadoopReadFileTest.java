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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HadoopReadFileTest {

  @Test
  @DisplayName("pread reads bytes from arbitrary offsets")
  void preadAtOffsets(@TempDir java.nio.file.Path dir) throws IOException {
    byte[] payload = bytes(4096);
    java.nio.file.Path file = dir.resolve("data.bin");
    Files.write(file, payload);

    try (FileSystem fs = FileSystem.getLocal(new Configuration(false))) {
      Path p = new Path(file.toUri());
      HadoopReadFile rf = new HadoopReadFile(fs, p, p.toUri().toString(), payload.length);
      try {
        byte[] got = rf.pread(100, 200);
        assertThat(got).hasSize(200);
        byte[] expected = new byte[200];
        System.arraycopy(payload, 100, expected, 0, 200);
        assertThat(got).isEqualTo(expected);

        byte[] dst = new byte[300];
        rf.pread(3000, dst, 0, 300);
        for (int i = 0; i < 300; i++) {
          assertThat(dst[i]).isEqualTo(payload[3000 + i]);
        }
      } finally {
        rf.close();
      }
    }
  }

  @Test
  @DisplayName("preadv populates heap and direct ByteBuffers in order")
  void preadVectored(@TempDir java.nio.file.Path dir) throws IOException {
    byte[] payload = bytes(1024);
    java.nio.file.Path file = dir.resolve("vec.bin");
    Files.write(file, payload);

    try (FileSystem fs = FileSystem.getLocal(new Configuration(false))) {
      Path p = new Path(file.toUri());
      HadoopReadFile rf = new HadoopReadFile(fs, p, p.toUri().toString(), payload.length);
      try {
        ByteBuffer heap = ByteBuffer.allocate(128);
        ByteBuffer direct = ByteBuffer.allocateDirect(256);
        rf.preadv(64, List.of(heap, direct));
        byte[] heapBytes = new byte[128];
        heap.flip();
        heap.get(heapBytes);
        for (int i = 0; i < 128; i++) {
          assertThat(heapBytes[i]).isEqualTo(payload[64 + i]);
        }
        byte[] directBytes = new byte[256];
        direct.flip();
        direct.get(directBytes);
        for (int i = 0; i < 256; i++) {
          assertThat(directBytes[i]).isEqualTo(payload[64 + 128 + i]);
        }
      } finally {
        rf.close();
      }
    }
  }

  @Test
  @DisplayName("pread rejects reads past EOF")
  void preadPastEof(@TempDir java.nio.file.Path dir) throws IOException {
    java.nio.file.Path file = dir.resolve("small.bin");
    Files.write(file, bytes(100));

    try (FileSystem fs = FileSystem.getLocal(new Configuration(false))) {
      Path p = new Path(file.toUri());
      HadoopReadFile rf = new HadoopReadFile(fs, p, p.toUri().toString(), 100);
      try {
        assertThatThrownBy(() -> rf.pread(90, 20))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("past EOF");
      } finally {
        rf.close();
      }
    }
  }

  @Test
  @DisplayName("size() returns the value passed at construction without restat")
  void sizeReturnsConstructorValue(@TempDir java.nio.file.Path dir) throws IOException {
    java.nio.file.Path file = dir.resolve("size.bin");
    Files.write(file, bytes(42));
    try (FileSystem fs = FileSystem.getLocal(new Configuration(false))) {
      Path p = new Path(file.toUri());
      HadoopReadFile rf = new HadoopReadFile(fs, p, p.toUri().toString(), 42);
      try {
        assertThat(rf.size()).isEqualTo(42);
      } finally {
        rf.close();
      }
    }
  }

  private static byte[] bytes(int n) {
    byte[] out = new byte[n];
    for (int i = 0; i < n; i++) {
      out[i] = (byte) (i & 0xff);
    }
    return out;
  }
}
