package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class WatermarkSnapshotsTest {
    static final TopicPartition P0 = new TopicPartition("cart-events", 0);
    static final TopicPartition P1 = new TopicPartition("cart-events", 1);
    static final Instant T1 = Instant.parse("2026-01-01T00:00:01Z");
    static final Instant T2 = Instant.parse("2026-01-01T00:00:02Z");
    static final Instant T3 = Instant.parse("2026-01-01T00:00:03Z");
    static final Duration MAX_AGE = Duration.ofSeconds(2);
    static final long SEC = Duration.ofSeconds(1).toNanos();

    @Test
    void noSnapshotSatisfiesNothing() {
        assertEquals(Optional.empty(), new WatermarkSnapshots(4, MAX_AGE).satisfied(P0, 100, 0));
    }

    @Test
    void committedBelowEndIsNotSatisfied() {
        WatermarkSnapshots s = new WatermarkSnapshots(4, MAX_AGE);
        s.add(T1, Map.of(P0, 10L), 0);
        assertEquals(Optional.empty(), s.satisfied(P0, 9, 0));
    }

    @Test
    void committedAtEndIsSatisfied() {
        WatermarkSnapshots s = new WatermarkSnapshots(4, MAX_AGE);
        s.add(T1, Map.of(P0, 10L), 0);
        assertEquals(Optional.of(T1), s.satisfied(P0, 10, 0));
    }

    @Test
    void emptyPartitionIsSatisfiedAtOnce() {
        WatermarkSnapshots s = new WatermarkSnapshots(4, MAX_AGE);
        s.add(T1, Map.of(P0, 0L), 0);
        assertEquals(Optional.of(T1), s.satisfied(P0, 0, 0));
    }

    @Test
    void newestSatisfiedSnapshotWins() {
        WatermarkSnapshots s = new WatermarkSnapshots(4, MAX_AGE);
        s.add(T1, Map.of(P0, 5L), 0);
        s.add(T2, Map.of(P0, 10L), 0);
        s.add(T3, Map.of(P0, 15L), 0);
        assertEquals(Optional.of(T2), s.satisfied(P0, 12, 0));
        assertEquals(Optional.of(T3), s.satisfied(P0, 15, 0));
        assertEquals(Optional.empty(), s.satisfied(P0, 4, 0));
    }

    @Test
    void snapshotWithoutThePartitionIsIgnored() {
        WatermarkSnapshots s = new WatermarkSnapshots(4, MAX_AGE);
        s.add(T1, Map.of(P0, 0L), 0);
        assertEquals(Optional.empty(), s.satisfied(P1, 100, 0));
    }

    @Test
    void keepsOnlyTheLatestSnapshots() {
        WatermarkSnapshots s = new WatermarkSnapshots(2, MAX_AGE);
        s.add(T1, Map.of(P0, 5L), 0);
        s.add(T2, Map.of(P0, 10L), 0);
        s.add(T3, Map.of(P0, 15L), 0);
        assertEquals(Optional.empty(), s.satisfied(P0, 7, 0));
        assertEquals(Optional.of(T2), s.satisfied(P0, 10, 0));
    }

    @Test
    void newestIsTheLastAdded() {
        WatermarkSnapshots s = new WatermarkSnapshots(4, MAX_AGE);
        assertEquals(Optional.empty(), s.newest());
        s.add(T1, Map.of(P0, 5L), 0);
        s.add(T2, Map.of(P0, 10L), 0);
        assertEquals(Optional.of(T2), s.newest());
    }

    @Test
    void snapshotsOlderThanMaxAgeAreIgnored() {
        WatermarkSnapshots s = new WatermarkSnapshots(4, MAX_AGE);
        s.add(T1, Map.of(P0, 0L), 0);
        assertEquals(Optional.of(T1), s.satisfied(P0, 0, 2 * SEC));      // exactly maxAge: still counts
        assertEquals(Optional.empty(), s.satisfied(P0, 0, 2 * SEC + 1));
    }

    @Test
    void behindPartitionUsesAnOlderSnapshotWithinMaxAge() {
        WatermarkSnapshots s = new WatermarkSnapshots(4, MAX_AGE);
        s.add(T1, Map.of(P0, 5L), 0);
        s.add(T2, Map.of(P0, 10L), SEC);
        assertEquals(Optional.of(T1), s.satisfied(P0, 7, SEC + SEC / 2));
        assertEquals(Optional.empty(), s.satisfied(P0, 7, 3 * SEC));      // T1 aged out; T2 not reached
    }
}
