package com.quince.cartrecovery.app;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;

/**
 * The detector's end-offset snapshots (spec §5.4, review fix 1). A snapshot (T, E) says every record appended to
 * partition p before Redis time T lies below offset E[p]; once the committed position reaches E[p], the watermark
 * for p may be set to T. Two rings, newest first: dense holds the latest {@code dense} snapshots (one per 250 ms
 * attempt), and sparse holds the first snapshot of each Redis-time second. Both drop snapshots older than
 * {@code history} before the newest. Nothing ages out while no snapshot arrives, so a detector cut off from the
 * broker keeps satisfying from what it has. Poll thread only.
 */
final class WatermarkSnapshots {
    private record Snapshot(Instant time, Map<TopicPartition, Long> ends) {}

    private final int dense;
    private final Duration history;
    private final Deque<Snapshot> denseRing = new ArrayDeque<>();
    private final Deque<Snapshot> sparseRing = new ArrayDeque<>();

    WatermarkSnapshots(int dense, Duration history) {
        if (dense < 1) throw new IllegalArgumentException("dense must be >= 1");
        this.dense = dense;
        this.history = history;
    }

    void add(Instant time, Map<TopicPartition, Long> ends) {
        Snapshot s = new Snapshot(time, Map.copyOf(ends));
        denseRing.addFirst(s);
        while (denseRing.size() > dense) denseRing.removeLast();
        Snapshot newestSparse = sparseRing.peekFirst();
        if (newestSparse == null || time.getEpochSecond() > newestSparse.time().getEpochSecond()) sparseRing.addFirst(s);
        Instant horizon = time.minus(history);
        for (Deque<Snapshot> ring : List.of(denseRing, sparseRing)) {
            while (!ring.isEmpty() && ring.peekLast().time().isBefore(horizon)) ring.removeLast();
        }
    }

    /** T of the newest retained snapshot, dense first then sparse, whose end offset for p is at or below committed. */
    Optional<Instant> satisfied(TopicPartition p, long committed) {
        for (Deque<Snapshot> ring : List.of(denseRing, sparseRing)) {
            for (Snapshot s : ring) {
                Long end = s.ends().get(p);
                if (end != null && committed >= end) return Optional.of(s.time());
            }
        }
        return Optional.empty();
    }

    Optional<Instant> newest() {
        Snapshot s = denseRing.peekFirst();
        return s == null ? Optional.empty() : Optional.of(s.time());
    }
}
