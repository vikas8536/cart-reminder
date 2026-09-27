package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.Watermark;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

class DetectorWatermarkHooksTest {
    static final TopicPartition P0 = new TopicPartition("cart-events", 0);
    static final TopicPartition P1 = new TopicPartition("cart-events", 1);
    static final Instant T1 = Instant.parse("2026-01-01T00:00:01Z");
    static final Instant T2 = Instant.parse("2026-01-01T00:00:02Z");
    static final Duration HISTORY = Duration.ofSeconds(35);

    final List<String> calls = new ArrayList<>();
    final RecordingWatermark watermark = new RecordingWatermark();
    final FakeConsumer broker = new FakeConsumer();
    final Metrics metrics = new Metrics();
    final long[] nanos = {0};
    final Health health = new Health();
    final DetectorWatermarkHooks hooks = new DetectorWatermarkHooks(watermark, health, metrics, () -> nanos[0], HISTORY);

    /** Moves nanoTime and Redis TIME forward together, then runs one iteration's beforePoll. */
    void tick(Duration d) {
        nanos[0] += d.toNanos();
        watermark.now = watermark.now.plus(d);
        hooks.beforePoll(broker.proxy());
    }

    String lag() { return health.readiness().get("watermark.lag_ms.p0"); }

    @Test
    void historyIsTheLargestLatenessBoundPlusClockSkew() {
        assertEquals(Duration.ofMinutes(30).plusSeconds(5),
            DetectorWatermarkHooks.history(RecoveryConfig.defaults(), Duration.ofSeconds(5)));
        RecoveryConfig demo = RecoveryConfig.defaults()
            .withLatenessBounds(List.of(Duration.ofSeconds(20), Duration.ofSeconds(20), Duration.ofSeconds(30)));
        assertEquals(Duration.ofSeconds(35), DetectorWatermarkHooks.history(demo, Duration.ofSeconds(5)));
    }

    @Test
    void readyShowsStaleUntilASnapshotIsSatisfiedThenTheLagBehindRedisTime() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 5L);
        broker.committed.put(P0, 3L);
        hooks.beforePoll(broker.proxy());                  // (T1, 5), committed 3
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals("stale", lag());

        hooks.afterCommit(broker.proxy(), Map.of(P0, 5L), 1);
        assertEquals("0", lag());

        broker.ends.put(P0, 10L);
        tick(Duration.ofSeconds(1));                       // (T2, 10): one second behind
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals("1000", lag());
        assertEquals(List.of("0|1|" + T1, "0|1|" + T1), watermark.published);
    }

    @Test
    void aPartitionBehindTheWholeHistoryGoesStale() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 5L);
        broker.committed.put(P0, 5L);
        hooks.beforePoll(broker.proxy());
        hooks.afterCommit(broker.proxy(), Map.of(), 1);   // publishes T1
        broker.ends.put(P0, 10L);
        for (int i = 0; i < 30; i++) tick(Duration.ofSeconds(1));
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals("30000", lag(), "30 s behind: still inside the 35 s history");

        for (int i = 0; i < 10; i++) tick(Duration.ofSeconds(1));
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals("stale", lag(), "40 s behind: T1 evicted");
        assertEquals(2, watermark.published.size());
    }

    @Test
    void idlePartitionPublishesAtOnceEvenWithoutANewCommit() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 5L);
        broker.committed.put(P0, 5L);
        hooks.beforePoll(broker.proxy());
        hooks.afterCommit(broker.proxy(), Map.of(), 3);
        assertEquals(List.of("0|3|" + T1), watermark.published);
    }

    @Test
    void behindPartitionWritesNothingUntilCommittedReachesTheSnapshot() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 10L);
        broker.committed.put(P0, 5L);
        hooks.beforePoll(broker.proxy());
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals(List.of(), watermark.published);
        hooks.afterCommit(broker.proxy(), Map.of(P0, 10L), 1);
        assertEquals(List.of("0|1|" + T1), watermark.published);
    }

    @Test
    void failedSnapshotTakesNothing() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 0L);
        broker.endOffsetsFailure = new TimeoutException("broker unreachable");
        hooks.beforePoll(broker.proxy());
        hooks.afterCommit(broker.proxy(), Map.of(P0, 0L), 1);
        assertEquals(List.of(), watermark.published);
        assertEquals(1, metrics.get("watermark.snapshot_failed"));
    }

    @Test
    void newestSatisfiedSnapshotWins() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 0L);
        broker.ends.put(P0, 5L);
        hooks.beforePoll(broker.proxy());                  // (T1, 5)
        nanos[0] += Duration.ofMillis(300).toNanos();
        watermark.now = T2;
        broker.ends.put(P0, 10L);
        hooks.beforePoll(broker.proxy());                  // (T2, 10)
        hooks.afterCommit(broker.proxy(), Map.of(P0, 7L), 1);
        hooks.afterCommit(broker.proxy(), Map.of(P0, 10L), 1);
        assertEquals(List.of("0|1|" + T1, "0|1|" + T2), watermark.published);
    }

    @Test
    void aCutOffDetectorKeepsRepublishingItsLastSatisfiedTimeAndItsLagGrows() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 5L);
        broker.committed.put(P0, 5L);
        hooks.beforePoll(broker.proxy());                  // (T1, 5)
        broker.endOffsetsFailure = new TimeoutException("broker unreachable");
        for (int i = 0; i < 12; i++) {                     // 3 s of failing snapshots, 250 ms apart
            tick(Duration.ofMillis(250));
            hooks.afterCommit(broker.proxy(), Map.of(), 1);
        }
        assertEquals(12, watermark.published.size());
        assertEquals(List.of("0|1|" + T1), watermark.published.stream().distinct().toList());
        assertEquals("3000", lag());
        assertEquals(12, metrics.get("watermark.snapshot_failed"));
    }

    @Test
    void aCutOffDetectorStopsPublishingOnceItsFrozenTimeIsOlderThanTheHistory() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 5L);
        broker.committed.put(P0, 5L);
        hooks.beforePoll(broker.proxy());                  // (T1, 5)
        broker.endOffsetsFailure = new TimeoutException("broker unreachable");
        for (int i = 0; i < 35; i++) tick(Duration.ofSeconds(1));
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals(List.of("0|1|" + T1), watermark.published, "35 s behind: still within the history");
        assertEquals("35000", lag());

        tick(Duration.ofSeconds(1));
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals(1, watermark.published.size(), "36 s behind: past the history, so the entry is left to go stale");
        assertEquals("stale", lag());
    }

    @Test
    void aBehindPartitionPublishesAnOldTimeRatherThanNothing() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 0L);
        broker.ends.put(P0, 5L);
        hooks.beforePoll(broker.proxy());                  // (T1, 5)
        broker.ends.put(P0, 10L);
        tick(Duration.ofSeconds(10));                      // (T1 + 10 s, 10)
        hooks.afterCommit(broker.proxy(), Map.of(P0, 7L), 1);
        assertEquals(List.of("0|1|" + T1), watermark.published);
        assertEquals("10000", lag());
    }

    @Test
    void snapshotsAtMostEvery250ms() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 0L);
        broker.ends.put(P0, 0L);
        hooks.beforePoll(broker.proxy());
        nanos[0] += Duration.ofMillis(100).toNanos();
        hooks.beforePoll(broker.proxy());
        assertEquals(1, broker.endOffsetsCalls);
        nanos[0] += Duration.ofMillis(150).toNanos();
        hooks.beforePoll(broker.proxy());
        assertEquals(2, broker.endOffsetsCalls);
    }

    @Test
    void readsRedisTimeBeforeEndOffsets() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 0L);
        broker.ends.put(P0, 0L);
        hooks.beforePoll(broker.proxy());
        assertEquals(List.of("committed", "now", "endOffsets"), calls);
    }

    @Test
    void revokedPartitionIsNotPublished() {
        broker.assignment = Set.of(P0, P1);
        broker.ends.putAll(Map.of(P0, 0L, P1, 0L));
        broker.committed.putAll(Map.of(P0, 0L, P1, 0L));
        hooks.beforePoll(broker.proxy());
        broker.assignment = Set.of(P1);
        hooks.afterCommit(broker.proxy(), Map.of(P0, 0L), 2);
        assertEquals(List.of("1|2|" + T1), watermark.published);
    }

    final class RecordingWatermark implements Watermark {
        final List<String> published = new ArrayList<>();
        Instant now = T1;

        @Override public void publish(int partition, long generation, Instant eventTime) {
            published.add(partition + "|" + generation + "|" + eventTime);
        }
        @Override public Instant current(int srcPartition) { return Instant.EPOCH; }
        @Override public Instant now() { calls.add("now"); return now; }
    }

    final class FakeConsumer {
        Set<TopicPartition> assignment = Set.of();
        final Map<TopicPartition, Long> ends = new HashMap<>();
        final Map<TopicPartition, Long> committed = new HashMap<>();
        RuntimeException endOffsetsFailure;
        int endOffsetsCalls;

        @SuppressWarnings("unchecked")
        Consumer<String, byte[]> proxy() {
            return (Consumer<String, byte[]>) Proxy.newProxyInstance(Consumer.class.getClassLoader(),
                new Class<?>[] {Consumer.class}, (self, method, args) -> switch (method.getName()) {
                    case "assignment" -> Set.copyOf(assignment);
                    case "endOffsets" -> {
                        calls.add("endOffsets");
                        endOffsetsCalls++;
                        if (endOffsetsFailure != null) throw endOffsetsFailure;
                        yield Map.copyOf(ends);
                    }
                    case "committed" -> {
                        calls.add("committed");
                        Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
                        committed.forEach((tp, offset) -> out.put(tp, new OffsetAndMetadata(offset)));
                        yield out;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        }
    }
}
