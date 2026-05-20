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

import io.github.luciferyang.cachedfs.core.RawFileCacheKey;
import io.github.luciferyang.cachedfs.core.id.StringIdMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SsdPinTest {

  @Test
  @DisplayName("empty pin close is a no-op")
  void emptyClose() {
    SsdPin empty = SsdPin.empty();
    empty.close();
    empty.close();
    assertThat(empty.isEmpty()).isTrue();
  }

  @Test
  @DisplayName(
      "close is idempotent — pin counter decremented exactly once even under concurrent close")
  void concurrentCloseDecrementsExactlyOnce(@TempDir Path dir) throws Exception {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://pin");
    SsdFile.Config cfg =
        new SsdFile.Config(
            dir.resolve("data"),
            dir.resolve("cpt"),
            dir.resolve("log"),
            dir.resolve("cpt.tmp"),
            /* maxRegions= */ 1,
            /* maxEntries= */ 0,
            /* checkpointIntervalBytes= */ 0L,
            /* checksumEnabled= */ false,
            /* checksumReadVerificationEnabled= */ false);
    try (SsdFile f = new SsdFile(cfg, ids)) {
      f.open();
      f.write(new RawFileCacheKey(fn, 0L), ByteBuffer.wrap(new byte[4096]));

      SsdPin pin = f.find(fn, 0L);
      assertThat(pin.isEmpty()).isFalse();
      assertThat(f.testingRegionPins(0)).isEqualTo(1);

      // Race many threads on close(); only the first should decrement the pin counter.
      int threads = 16;
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      for (int i = 0; i < threads; i++) {
        new Thread(
                () -> {
                  try {
                    start.await();
                    pin.close();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    done.countDown();
                  }
                })
            .start();
      }
      start.countDown();
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

      assertThat(f.testingRegionPins(0)).isZero();
      assertThat(pin.isEmpty()).isTrue();
    } finally {
      ids.release(fn);
    }
  }

  @Test
  @DisplayName("isEmpty reflects post-close state")
  void isEmptyAfterClose(@TempDir Path dir) throws IOException {
    StringIdMap ids = new StringIdMap();
    long fn = ids.makeId("file://pin2");
    SsdFile.Config cfg =
        new SsdFile.Config(
            dir.resolve("data"),
            dir.resolve("cpt"),
            dir.resolve("log"),
            dir.resolve("cpt.tmp"),
            1,
            0,
            0L,
            false,
            false);
    try (SsdFile f = new SsdFile(cfg, ids)) {
      f.open();
      f.write(new RawFileCacheKey(fn, 0L), ByteBuffer.wrap(new byte[1024]));
      SsdPin pin = f.find(fn, 0L);
      assertThat(pin.isEmpty()).isFalse();
      pin.close();
      assertThat(pin.isEmpty()).isTrue();
      assertThat(pin.run()).isNull();
    } finally {
      ids.release(fn);
    }
  }
}
