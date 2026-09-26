package com.quince.cartrecovery.app;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;

/**
 * The detector's latest end-offset snapshots (spec §5.4). A snapshot (T, E) says every record appended
 * to partition p before Redis time T lies below offset E[p]; once the committed position reaches E[p],
 * the watermark for p may be set to T. Used by the poll thread only.
 */
final class WatermarkSnapshots {
    private record Snapshot(Instant time, Map<TopicPartition, Long> ends) {}

    private final int keep;
    private final Deque<Snapshot> newestFirst = new ArrayDeque<>();

    WatermarkSnapshots(int keep) {
        if (keep < 1) throw new IllegalArgumentException("keep must be >= 1");
        this.keep = keep;
    }

    void add(Instant time, Map<TopicPartition, Long> ends) {
        newestFirst.addFirst(new Snapshot(time, Map.copyOf(ends)));
        while (newestFirst.size() > keep) newestFirst.removeLast();
    }

    /** T of the newest snapshot whose end offset for p is at or below the committed position; empty if none. */
    Optional<Instant> satisfied(TopicPartition p, long committed) {
        for (Snapshot s : newestFirst) {
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
