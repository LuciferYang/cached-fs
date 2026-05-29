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
package io.github.luciferyang.cachedfs.hadoop.mbean;

import io.github.luciferyang.cachedfs.core.stats.AggregatedIoStatistics;
import io.github.luciferyang.cachedfs.core.stats.CacheStats;
import io.github.luciferyang.cachedfs.core.stats.IoStatisticsSnapshot;
import io.github.luciferyang.cachedfs.core.tracker.ScanTracker;
import io.github.luciferyang.cachedfs.hadoop.CacheBootstrap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Production {@link CachedFsBootstrapMXBean} implementation. Holds a reference to the singleton
 * {@link CacheBootstrap} and renders multi-line reports for the read methods + delegates mutating
 * methods to the bootstrap's existing close primitives.
 *
 * <p>Reports use a stable line-based format ({@code key: value} per line; sections separated by a
 * blank line + a {@code ---} divider header) so the CLI can echo them straight to stdout AND
 * downstream tooling can grep specific fields without depending on a JSON parser.
 */
public final class CachedFsBootstrapMBean implements CachedFsBootstrapMXBean {

  private final CacheBootstrap bootstrap;

  public CachedFsBootstrapMBean(CacheBootstrap bootstrap) {
    this.bootstrap = Objects.requireNonNull(bootstrap, "bootstrap");
  }

  @Override
  public String inspect(String key) {
    Objects.requireNonNull(key, "key");
    StringBuilder sb = new StringBuilder(256);
    sb.append("=== cached-fs inspect ===\n");
    sb.append("key: ").append(key).append('\n');
    boolean hasHandle = bootstrap.handleFactory().containsHandle(key);
    sb.append("open-handle: ").append(hasHandle).append('\n');
    sb.append("handles-total: ").append(bootstrap.handleFactory().size()).append('\n');

    // Per-tracker entries for this key. Trackers are keyed by scanId, and each tracker keys its
    // inner data by fileNum. There is no direct path → fileNum lookup that avoids a side-effecting
    // makeId on the StringIdMap, so the user-facing report is "is the handle open?" plus the
    // tracker scanIds that have ever opened ANYTHING (operators can correlate visually).
    sb.append("--- trackers ---\n");
    for (ScanTracker t : bootstrap.scanTrackersView()) {
      sb.append("scan: ")
          .append(t.scanId())
          .append(" entries=")
          .append(t.size())
          .append(" rejected=")
          .append(t.entriesRejected())
          .append('\n');
    }
    return sb.toString();
  }

  @Override
  public String stats() {
    StringBuilder sb = new StringBuilder(1024);
    sb.append("=== cached-fs stats ===\n");

    sb.append("--- aggregate-io ---\n");
    AggregatedIoStatistics agg = bootstrap.aggregateIoStats();
    sb.append("ram-hit: ").append(agg.ramHit()).append('\n');
    sb.append("ram-hit-bytes: ").append(agg.ramHitBytes()).append('\n');
    sb.append("read: ").append(agg.read()).append('\n');
    sb.append("read-bytes: ").append(agg.readBytes()).append('\n');
    sb.append("prefetch: ").append(agg.prefetch()).append('\n');
    sb.append("prefetch-bytes: ").append(agg.prefetchBytes()).append('\n');
    sb.append("ssd-read: ").append(agg.ssdRead()).append('\n');
    sb.append("ssd-read-bytes: ").append(agg.ssdReadBytes()).append('\n');
    sb.append("raw-overread-bytes: ").append(agg.rawOverreadBytes()).append('\n');
    sb.append("stale-scan-id-recoveries: ").append(agg.staleScanIdRecoveries()).append('\n');
    sb.append("prefetch-evicted-before-use: ").append(agg.prefetchEvictedBeforeUse()).append('\n');
    sb.append("prefetch-eligible-suppressed-bytes: ")
        .append(agg.prefetchEligibleSuppressedBytes())
        .append('\n');
    sb.append("seq-hwm-regime-resets: ").append(agg.seqHwmRegimeResets()).append('\n');
    sb.append("prefetch-skipped-queue-full: ")
        .append(agg.prefetchSkipped("queue_full"))
        .append('\n');
    sb.append("prefetch-skipped-budget: ").append(agg.prefetchSkipped("budget")).append('\n');
    sb.append("prefetch-skipped-heap-pressure: ")
        .append(agg.prefetchSkipped("heap_pressure"))
        .append('\n');
    sb.append("prefetch-skipped-other: ").append(agg.prefetchSkipped("other")).append('\n');

    sb.append("--- ram-cache ---\n");
    CacheStats ram = bootstrap.ramCache().refreshStats();
    sb.append("num-entries: ").append(ram.numEntries()).append('\n');
    sb.append("tiny-size: ").append(ram.tinySize()).append('\n');
    sb.append("large-size: ").append(ram.largeSize()).append('\n');
    sb.append("num-hit: ").append(ram.numHit()).append('\n');
    sb.append("hit-bytes: ").append(ram.hitBytes()).append('\n');
    sb.append("num-new: ").append(ram.numNew()).append('\n');
    sb.append("num-evict: ").append(ram.numEvict()).append('\n');
    sb.append("num-aged-out: ").append(ram.numAgedOut()).append('\n');
    sb.append("num-stales: ").append(ram.numStales()).append('\n');

    bootstrap
        .ssdCache()
        .ifPresent(ssd -> sb.append("--- ssd-cache ---\n").append("present: true\n"));

    sb.append("--- handles ---\n");
    sb.append("size: ").append(bootstrap.handleFactory().size()).append('\n');

    sb.append("--- trackers ---\n");
    sb.append("scan-tracker-count: ").append(bootstrap.scanTrackerCount()).append('\n');
    sb.append("scan-tracker-entries: ").append(bootstrap.scanTrackerEntries()).append('\n');
    sb.append("scan-tracker-max-entries: ").append(bootstrap.scanTrackerMaxEntries()).append('\n');
    sb.append("scan-tracker-entries-rejected: ")
        .append(bootstrap.scanTrackerEntriesRejected())
        .append('\n');
    sb.append("scan-trackers-rejected: ").append(bootstrap.scanTrackersRejected()).append('\n');

    sb.append("--- recent-streams ---\n");
    sb.append("capacity: ").append(bootstrap.recentStreams().capacity()).append('\n');
    sb.append("added-total: ").append(bootstrap.recentStreams().addedTotal()).append('\n');

    return sb.toString();
  }

  @Override
  public Map<String, Long> counters() {
    // Insertion-ordered + namespaced keys (io. / ram. / handles. / trackers. / recent-streams.) so
    // the flat map stays collision-free and groups read naturally. The CLI computes window deltas
    // from this; cumulative counters yield per-second rates, gauges yield change-over-window.
    Map<String, Long> m = new LinkedHashMap<>();
    AggregatedIoStatistics agg = bootstrap.aggregateIoStats();
    m.put("io.ram-hit", agg.ramHit());
    m.put("io.ram-hit-bytes", agg.ramHitBytes());
    m.put("io.read", agg.read());
    m.put("io.read-bytes", agg.readBytes());
    m.put("io.prefetch", agg.prefetch());
    m.put("io.prefetch-bytes", agg.prefetchBytes());
    m.put("io.ssd-read", agg.ssdRead());
    m.put("io.ssd-read-bytes", agg.ssdReadBytes());
    m.put("io.raw-overread-bytes", agg.rawOverreadBytes());
    m.put("io.stale-scan-id-recoveries", agg.staleScanIdRecoveries());
    m.put("io.prefetch-evicted-before-use", agg.prefetchEvictedBeforeUse());
    m.put("io.prefetch-eligible-suppressed-bytes", agg.prefetchEligibleSuppressedBytes());
    m.put("io.seq-hwm-regime-resets", agg.seqHwmRegimeResets());
    m.put("io.prefetch-skipped-queue-full", agg.prefetchSkipped("queue_full"));
    m.put("io.prefetch-skipped-budget", agg.prefetchSkipped("budget"));
    m.put("io.prefetch-skipped-heap-pressure", agg.prefetchSkipped("heap_pressure"));
    m.put("io.prefetch-skipped-other", agg.prefetchSkipped("other"));

    CacheStats ram = bootstrap.ramCache().refreshStats();
    m.put("ram.num-entries", (long) ram.numEntries());
    m.put("ram.tiny-size", ram.tinySize());
    m.put("ram.large-size", ram.largeSize());
    m.put("ram.num-hit", ram.numHit());
    m.put("ram.hit-bytes", ram.hitBytes());
    m.put("ram.num-new", ram.numNew());
    m.put("ram.num-evict", ram.numEvict());
    m.put("ram.num-aged-out", ram.numAgedOut());
    m.put("ram.num-stales", ram.numStales());

    m.put("handles.size", (long) bootstrap.handleFactory().size());

    m.put("trackers.scan-tracker-count", (long) bootstrap.scanTrackerCount());
    m.put("trackers.scan-tracker-entries", bootstrap.scanTrackerEntries());
    m.put("trackers.scan-tracker-max-entries", bootstrap.scanTrackerMaxEntries());
    m.put("trackers.scan-tracker-entries-rejected", bootstrap.scanTrackerEntriesRejected());
    m.put("trackers.scan-trackers-rejected", bootstrap.scanTrackersRejected());

    m.put("recent-streams.capacity", (long) bootstrap.recentStreams().capacity());
    m.put("recent-streams.added-total", bootstrap.recentStreams().addedTotal());
    return m;
  }

  @Override
  public String recentStreams(int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException("limit must be >= 0: " + limit);
    }
    StringBuilder sb = new StringBuilder(512);
    sb.append("=== cached-fs recent-streams ===\n");
    List<IoStatisticsSnapshot> snaps = bootstrap.recentStreams().snapshot();
    sb.append("count: ").append(snaps.size()).append('\n');
    int n = (limit == 0) ? snaps.size() : Math.min(limit, snaps.size());
    sb.append("returned: ").append(n).append('\n');
    sb.append("--- entries ---\n");
    for (int i = 0; i < n; i++) {
      IoStatisticsSnapshot s = snaps.get(i);
      sb.append('#')
          .append(i)
          .append(" closed-at-nanos=")
          .append(s.closedAtNanos())
          .append(" read=")
          .append(s.read())
          .append(" read-bytes=")
          .append(s.readBytes())
          .append(" ram-hit=")
          .append(s.ramHit())
          .append(" ram-hit-bytes=")
          .append(s.ramHitBytes())
          .append(" prefetch=")
          .append(s.prefetch())
          .append(" prefetch-bytes=")
          .append(s.prefetchBytes())
          .append('\n');
    }
    return sb.toString();
  }

  @Override
  public long drain() {
    int size = bootstrap.handleFactory().size();
    try {
      bootstrap.handleFactory().closeAll();
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    return size;
  }

  @Override
  public long purgeMatching(String regex) {
    Objects.requireNonNull(regex, "regex");
    // Compile the pattern eagerly so an invalid regex surfaces as a PatternSyntaxException to the
    // JMX caller (wrapped as MBeanException) instead of an opaque exception inside closeMatching.
    Pattern pattern = Pattern.compile(regex);
    long matched =
        bootstrap.handleFactory().keys().stream().filter(k -> pattern.matcher(k).matches()).count();
    try {
      bootstrap.handleFactory().closeMatching(k -> pattern.matcher(k).matches());
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    return matched;
  }

  // --- Optional accessor for tests / direct access ----------------------

  /** Visible for tests; production callers should hold a {@link CachedFsBootstrapMXBean}. */
  public Optional<CacheBootstrap> bootstrap() {
    return Optional.of(bootstrap);
  }
}
