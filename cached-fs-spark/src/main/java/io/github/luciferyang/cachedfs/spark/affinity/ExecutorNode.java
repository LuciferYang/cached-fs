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
package io.github.luciferyang.cachedfs.spark.affinity;

import io.github.luciferyang.cachedfs.spark.hash.ConsistentHash;
import java.util.Objects;

/**
 * Executor identity on the affinity ring. Two physically-distinct executors on the same host MUST
 * map to different ring positions, so the hash key includes both the executor id and the host —
 * matching Apache Gluten's {@code ExecutorNode} contract.
 */
public record ExecutorNode(String executorId, String host) implements ConsistentHash.Node {

  public ExecutorNode {
    Objects.requireNonNull(executorId, "executorId");
    Objects.requireNonNull(host, "host");
  }

  @Override
  public String key() {
    return executorId + "-" + host;
  }

  /**
   * Spark's {@code TaskLocation.apply} parses any {@code "executor_<host>_<execId>"} string into an
   * {@code ExecutorCacheTaskLocation}, which the DAGScheduler treats as PROCESS_LOCAL preferred
   * placement.
   *
   * <p>Spark recovers {@code host} and {@code execId} by stripping the {@code "executor_"} prefix
   * and {@code split("_", 2)} on the remainder, so any underscore in {@code host} silently corrupts
   * the parse. {@link CachedFsSoftAffinityManager#handleExecutorAdded} rejects underscore-bearing
   * hosts at the listener boundary — this method does NOT re-validate.
   */
  public String toCacheTaskLocation() {
    return "executor_" + host + "_" + executorId;
  }
}
