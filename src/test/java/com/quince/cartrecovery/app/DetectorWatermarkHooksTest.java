package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.core.Metrics;
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

    final List<String> calls = new ArrayList<>();
    final RecordingWatermark watermark = new RecordingWatermark();
    final FakeConsumer broker = new FakeConsumer();
    final Metrics metrics = new Metrics();
    final long[] nanos = {0};
    final DetectorWatermarkHooks hooks = new DetectorWatermarkHooks(watermark, new Health(), metrics, () -> nanos[0]);

    @Test
    void idlePartitionPublishesAtOnceEvenWithoutANewCommit() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 5L);
        broker.committed.put(P0, 5L);
        hooks.beforePoll(broker.proxy());
        hooks.afterCommit(broker.proxy(), Map.of(), 3);   // an empty poll or a backoff iteration commits nothing
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
