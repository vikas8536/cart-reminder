package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

    @Test
    void noSnapshotSatisfiesNothing() {
        assertEquals(Optional.empty(), new WatermarkSnapshots(4).satisfied(P0, 100));
    }

    @Test
    void committedBelowEndIsNotSatisfied() {
        WatermarkSnapshots s = new WatermarkSnapshots(4);
        s.add(T1, Map.of(P0, 10L));
        assertEquals(Optional.empty(), s.satisfied(P0, 9));
    }

    @Test
    void committedAtEndIsSatisfied() {
        WatermarkSnapshots s = new WatermarkSnapshots(4);
        s.add(T1, Map.of(P0, 10L));
        assertEquals(Optional.of(T1), s.satisfied(P0, 10));
    }

    @Test
    void emptyPartitionIsSatisfiedAtOnce() {
        WatermarkSnapshots s = new WatermarkSnapshots(4);
        s.add(T1, Map.of(P0, 0L));
        assertEquals(Optional.of(T1), s.satisfied(P0, 0));
    }

    @Test
    void newestSatisfiedSnapshotWins() {
        WatermarkSnapshots s = new WatermarkSnapshots(4);
        s.add(T1, Map.of(P0, 5L));
        s.add(T2, Map.of(P0, 10L));
        s.add(T3, Map.of(P0, 15L));
        assertEquals(Optional.of(T2), s.satisfied(P0, 12));
        assertEquals(Optional.of(T3), s.satisfied(P0, 15));
        assertEquals(Optional.empty(), s.satisfied(P0, 4));
    }

    @Test
    void snapshotWithoutThePartitionIsIgnored() {
        WatermarkSnapshots s = new WatermarkSnapshots(4);
        s.add(T1, Map.of(P0, 0L));
        assertEquals(Optional.empty(), s.satisfied(P1, 100));
    }

    @Test
    void keepsOnlyTheLatestSnapshots() {
        WatermarkSnapshots s = new WatermarkSnapshots(2);
        s.add(T1, Map.of(P0, 5L));
        s.add(T2, Map.of(P0, 10L));
        s.add(T3, Map.of(P0, 15L));
        assertEquals(Optional.empty(), s.satisfied(P0, 7));
        assertEquals(Optional.of(T2), s.satisfied(P0, 10));
    }

    @Test
    void newestIsTheLastAdded() {
        WatermarkSnapshots s = new WatermarkSnapshots(4);
        assertEquals(Optional.empty(), s.newest());
        s.add(T1, Map.of(P0, 5L));
        s.add(T2, Map.of(P0, 10L));
        assertEquals(Optional.of(T2), s.newest());
    }
}
