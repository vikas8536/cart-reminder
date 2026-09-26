package com.quince.cartrecovery.app;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;

/**
 * The detector's latest end-offset snapshots (spec §5.4). A snapshot (T, E) says every record appended
 * to partition p before Redis time T lies below offset E[p]; once the committed position reaches E[p],
 * the watermark for p may be set to T. A snapshot older than {@code maxAge} (by the caller's nanoTime at
 * capture) no longer counts, so a detector that cannot take new snapshots stops writing and its entry goes
 * stale. Used by the poll thread only.
 */
final class WatermarkSnapshots {
    private record Snapshot(Instant time, Map<TopicPartition, Long> ends, long capturedNanos) {}

    private final int keep;
    private final long maxAgeNanos;
    private final Deque<Snapshot> newestFirst = new ArrayDeque<>();

    WatermarkSnapshots(int keep, Duration maxAge) {
        if (keep < 1) throw new IllegalArgumentException("keep must be >= 1");
        this.keep = keep;
        this.maxAgeNanos = maxAge.toNanos();
    }

    /** {@code capturedNanos}: nanoTime read before T, so the age is never understated. */
    void add(Instant time, Map<TopicPartition, Long> ends, long capturedNanos) {
        newestFirst.addFirst(new Snapshot(time, Map.copyOf(ends), capturedNanos));
        while (newestFirst.size() > keep) newestFirst.removeLast();
    }

    /** T of the newest snapshot, at most maxAge old, whose end offset for p is at or below committed; empty if none. */
    Optional<Instant> satisfied(TopicPartition p, long committed, long nowNanos) {
        for (Snapshot s : newestFirst) {
            if (nowNanos - s.capturedNanos() > maxAgeNanos) break;   // older ones are older still
            Long end = s.ends().get(p);
            if (end != null && committed >= end) return Optional.of(s.time());
        }
        return Optional.empty();
    }

    Optional<Instant> newest() {
        Snapshot s = newestFirst.peekFirst();
        return s == null ? Optional.empty() : Optional.of(s.time());
    }
}
