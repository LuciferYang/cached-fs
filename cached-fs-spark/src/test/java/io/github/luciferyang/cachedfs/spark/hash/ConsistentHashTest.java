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
package io.github.luciferyang.cachedfs.spark.hash;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConsistentHashTest {

  private record TestNode(String id) implements ConsistentHash.Node {
    @Override
    public String key() {
      return id;
    }
  }

  @Test
  @DisplayName("empty ring returns empty allocation")
  void emptyRing() {
    ConsistentHash<TestNode> ring = new ConsistentHash<>(10);
    assertThat(ring.allocateNodes("any", 3)).isEmpty();
    assertThat(ring.nodeCount()).isZero();
  }

  @Test
  @DisplayName("allocateNodes returns at most `count` distinct nodes; clamps to ring size")
  void allocationClampsToRingSize() {
    ConsistentHash<TestNode> ring = new ConsistentHash<>(50);
    ring.addNode(new TestNode("a"));
    ring.addNode(new TestNode("b"));
    ring.addNode(new TestNode("c"));

    assertThat(ring.allocateNodes("file-1", 2)).hasSize(2);
    assertThat(ring.allocateNodes("file-1", 5)).hasSize(3); // ring only has 3 nodes
    assertThat(ring.allocateNodes("file-1", 0)).isEmpty();
  }

  @Test
  @DisplayName("same key returns same primary node across calls (deterministic)")
  void deterministicLookup() {
    ConsistentHash<TestNode> ring = new ConsistentHash<>(50);
    ring.addNode(new TestNode("a"));
    ring.addNode(new TestNode("b"));
    ring.addNode(new TestNode("c"));

    String key = "hdfs://nn/tbl/part-0001.parquet";
    TestNode first = ring.allocateNodes(key, 1).get(0);
    for (int i = 0; i < 100; i++) {
      assertThat(ring.allocateNodes(key, 1)).first().isEqualTo(first);
    }
  }

  @Test
  @DisplayName("removeNode rebalances: keys move only when their primary is removed")
  void removeRebalances() {
    ConsistentHash<TestNode> ring = new ConsistentHash<>(100);
    TestNode a = new TestNode("a");
    TestNode b = new TestNode("b");
    TestNode c = new TestNode("c");
    ring.addNode(a);
    ring.addNode(b);
    ring.addNode(c);

    // Sample many keys; record their primary before + after removing `b`.
    Map<String, String> before = new HashMap<>();
    for (int i = 0; i < 500; i++) {
      String k = "key-" + i;
      before.put(k, ring.allocateNodes(k, 1).get(0).id());
    }
    ring.removeNode(b);

    int moved = 0;
    int bAssigned = 0;
    for (var e : before.entrySet()) {
      String primaryAfter = ring.allocateNodes(e.getKey(), 1).get(0).id();
      if (!primaryAfter.equals(e.getValue())) moved++;
      if ("b".equals(e.getValue())) bAssigned++;
      // Removed node never returns.
      assertThat(primaryAfter).isNotEqualTo("b");
    }
    // Consistent-hash guarantee: only keys originally assigned to `b` should re-map.
    assertThat(moved).isEqualTo(bAssigned);
  }

  @Test
  @DisplayName("addNode is idempotent (adding the same node twice does not duplicate)")
  void addNodeIdempotent() {
    ConsistentHash<TestNode> ring = new ConsistentHash<>(10);
    assertThat(ring.addNode(new TestNode("a"))).isTrue();
    assertThat(ring.addNode(new TestNode("a"))).isFalse();
    assertThat(ring.nodeCount()).isEqualTo(1);
  }

  @Test
  @DisplayName("removeNode on missing node returns false (idempotent)")
  void removeNodeIdempotent() {
    ConsistentHash<TestNode> ring = new ConsistentHash<>(10);
    assertThat(ring.removeNode(new TestNode("a"))).isFalse();
    ring.addNode(new TestNode("a"));
    assertThat(ring.removeNode(new TestNode("a"))).isTrue();
    assertThat(ring.removeNode(new TestNode("a"))).isFalse();
  }

  @Test
  @DisplayName("higher virtual-node count produces more uniform key distribution")
  void uniformityScalesWithVirtualNodes() {
    int keys = 5_000;
    int sparse = countUnbalanced(keys, /* virtual= */ 1);
    int dense = countUnbalanced(keys, /* virtual= */ 200);
    assertThat(dense)
        .as("denser ring (200 virtual nodes) should be more uniform than 1 virtual node")
        .isLessThan(sparse);
  }

  /**
   * Returns a crude unbalance metric: max-over-min ratio of key counts per node. Lower is more
   * uniform.
   */
  private static int countUnbalanced(int keys, int virtual) {
    ConsistentHash<TestNode> ring = new ConsistentHash<>(virtual);
    ring.addNode(new TestNode("n1"));
    ring.addNode(new TestNode("n2"));
    ring.addNode(new TestNode("n3"));
    Map<String, Integer> counts = new HashMap<>();
    for (int i = 0; i < keys; i++) {
      List<TestNode> picks = ring.allocateNodes("k-" + i, 1);
      counts.merge(picks.get(0).id(), 1, Integer::sum);
    }
    int max = counts.values().stream().max(Integer::compareTo).orElse(0);
    int min = counts.values().stream().min(Integer::compareTo).orElse(0);
    return max - min;
  }
}
