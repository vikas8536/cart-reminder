package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class WatermarkSnapshotsTest {
    static final TopicPartition P0 = new TopicPartition("cart-events", 0);
    static final TopicPartition P1 = new TopicPartition("cart-events", 1);
    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    static final Duration HISTORY = Duration.ofSeconds(35);

    static Instant at(long millis) { return T0.plusMillis(millis); }

    static WatermarkSnapshots snapshots() { return new WatermarkSnapshots(8, HISTORY); }

    /** Twenty attempts 250 ms apart, end offset k at attempt k: dense keeps k = 12..19, sparse keeps k = 0, 4, 8, 12, 16. */
    static WatermarkSnapshots fiveSecondsOfAttempts() {
        WatermarkSnapshots s = snapshots();
        for (int k = 0; k < 20; k++) s.add(at(k * 250L), Map.of(P0, (long) k));
        return s;
    }

    @Test
    void noSnapshotSatisfiesNothing() {
        assertEquals(Optional.empty(), snapshots().satisfied(P0, 100));
    }

    @Test
    void committedBelowEndIsNotSatisfied() {
        WatermarkSnapshots s = snapshots();
        s.add(at(0), Map.of(P0, 10L));
        assertEquals(Optional.empty(), s.satisfied(P0, 9));
    }

    @Test
    void committedAtEndIsSatisfied() {
        WatermarkSnapshots s = snapshots();
        s.add(at(0), Map.of(P0, 10L));
        assertEquals(Optional.of(at(0)), s.satisfied(P0, 10));
    }

    @Test
    void emptyPartitionIsSatisfiedAtOnce() {
        WatermarkSnapshots s = snapshots();
        s.add(at(0), Map.of(P0, 0L));
        assertEquals(Optional.of(at(0)), s.satisfied(P0, 0));
    }

    @Test
    void newestSatisfiedSnapshotWins() {
        WatermarkSnapshots s = snapshots();
        s.add(at(0), Map.of(P0, 5L));
        s.add(at(250), Map.of(P0, 10L));
        s.add(at(500), Map.of(P0, 15L));
        assertEquals(Optional.of(at(250)), s.satisfied(P0, 12));
        assertEquals(Optional.of(at(500)), s.satisfied(P0, 15));
        assertEquals(Optional.empty(), s.satisfied(P0, 4));
    }

    @Test
    void snapshotWithoutThePartitionIsIgnored() {
        WatermarkSnapshots s = snapshots();
        s.add(at(0), Map.of(P0, 0L));
        assertEquals(Optional.empty(), s.satisfied(P1, 100));
    }

    @Test
    void newestIsTheLastAdded() {
        WatermarkSnapshots s = snapshots();
        assertEquals(Optional.empty(), s.newest());
        s.add(at(0), Map.of(P0, 5L));
        s.add(at(250), Map.of(P0, 10L));
        assertEquals(Optional.of(at(250)), s.newest());
    }

    @Test
    void aBehindPartitionIsSatisfiedByASparseSnapshotOlderThan2s() {
        assertEquals(Optional.of(at(0)), fiveSecondsOfAttempts().satisfied(P0, 0));
    }

    @Test
    void theSparseRingKeepsTheFirstSnapshotOfEachSecond() {
        WatermarkSnapshots s = fiveSecondsOfAttempts();
        assertEquals(Optional.of(at(0)), s.satisfied(P0, 3));       // k = 3 is not first in second 0; k = 0 is
        assertEquals(Optional.of(at(1000)), s.satisfied(P0, 5));    // second 1 starts at k = 4
        assertEquals(Optional.of(at(2000)), s.satisfied(P0, 11));   // second 2 starts at k = 8; dense starts at 12
    }

    @Test
    void theDenseRingKeepsTheLatestEightAttempts() {
        WatermarkSnapshots s = fiveSecondsOfAttempts();
        assertEquals(Optional.of(at(19 * 250)), s.satisfied(P0, 19));
        assertEquals(Optional.of(at(13 * 250)), s.satisfied(P0, 13));   // not a sparse entry: only dense has it
    }

    @Test
    void snapshotsOlderThanTheHistoryAreEvictedFromBothRings() {
        WatermarkSnapshots s = snapshots();
        s.add(at(0), Map.of(P0, 0L));
        s.add(at(35_000), Map.of(P0, 10L));
        assertEquals(Optional.of(at(0)), s.satisfied(P0, 0), "exactly the history back: kept");
        s.add(at(35_001), Map.of(P0, 10L));
        assertEquals(Optional.empty(), s.satisfied(P0, 0), "older than the history: evicted");
    }

    @Test
    void rejectsAnEmptyDenseRing() {
        assertThrows(IllegalArgumentException.class, () -> new WatermarkSnapshots(0, HISTORY));
    }
}
