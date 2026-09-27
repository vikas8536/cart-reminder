package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Proxy;
import java.time.Duration;
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

class StuckPartitionHooksTest {
    static final TopicPartition P0 = new TopicPartition("reminder-intents-fast", 0);
    static final TopicPartition P1 = new TopicPartition("reminder-intents-fast", 1);

    final List<String> delegateCalls = new ArrayList<>();
    final FakeConsumer broker = new FakeConsumer();
    final Health health = new Health();
    final long[] nanos = {0};
    final StuckPartitionHooks<byte[]> hooks =
        new StuckPartitionHooks<>(new BatchConsumerLoop.Hooks<byte[]>() {
            @Override public void beforePoll(Consumer<String, byte[]> c) { delegateCalls.add("beforePoll"); }
            @Override public void afterCommit(Consumer<String, byte[]> c, Map<TopicPartition, Long> committed, int gen) {
                delegateCalls.add("afterCommit");
            }
        }, health, () -> nanos[0]);

    void tick(Duration by) {
        nanos[0] += by.toNanos();
    }

    @Test
    void composesWithTheDelegateHooks() {
        broker.assignment = Set.of(P0);
        hooks.beforePoll(broker.proxy());
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals(List.of("beforePoll", "afterCommit"), delegateCalls);
    }

    @Test
    void committedUnchangedFor5MinutesWhileEndAheadIsStuck() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 5L);
        broker.ends.put(P0, 10L);
        hooks.beforePoll(broker.proxy());   // first sighting: not yet stuck
        assertEquals("false", health.readiness().get("stuck.reminder-intents-fast-0"));

        tick(Duration.ofSeconds(30));
        hooks.beforePoll(broker.proxy());   // still unchanged, only 30 s elapsed
        assertEquals("false", health.readiness().get("stuck.reminder-intents-fast-0"));

        tick(Duration.ofMinutes(5));
        hooks.beforePoll(broker.proxy());   // unchanged for 5 min while end > committed
        assertEquals("true", health.readiness().get("stuck.reminder-intents-fast-0"));
    }

    @Test
    void aCommittedAdvanceClearsTheStuckSignal() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 5L);
        broker.ends.put(P0, 10L);
        hooks.beforePoll(broker.proxy());
        tick(Duration.ofMinutes(6));
        hooks.beforePoll(broker.proxy());
        assertEquals("true", health.readiness().get("stuck.reminder-intents-fast-0"));

        broker.committed.put(P0, 8L);
        tick(Duration.ofSeconds(30));
        hooks.beforePoll(broker.proxy());
        assertEquals("false", health.readiness().get("stuck.reminder-intents-fast-0"));
    }

    @Test
    void caughtUpPartitionIsNeverStuck() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 10L);
        broker.ends.put(P0, 10L);
        hooks.beforePoll(broker.proxy());
        tick(Duration.ofMinutes(6));
        hooks.beforePoll(broker.proxy());
        assertEquals("false", health.readiness().get("stuck.reminder-intents-fast-0"));
    }

    @Test
    void checksAtMostEvery30Seconds() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 5L);
        broker.ends.put(P0, 10L);
        hooks.beforePoll(broker.proxy());
        assertEquals(1, broker.committedCalls);
        tick(Duration.ofSeconds(10));
        hooks.beforePoll(broker.proxy());
        assertEquals(1, broker.committedCalls);   // cheap: no broker call before the interval elapses
        tick(Duration.ofSeconds(25));
        hooks.beforePoll(broker.proxy());
        assertEquals(2, broker.committedCalls);
    }

    @Test
    void aFailedCheckLeavesThePreviousReadyValue() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 5L);
        broker.ends.put(P0, 10L);
        hooks.beforePoll(broker.proxy());
        assertEquals("false", health.readiness().get("stuck.reminder-intents-fast-0"));

        broker.endOffsetsFailure = new TimeoutException("broker unreachable");
        tick(Duration.ofMinutes(6));
        hooks.beforePoll(broker.proxy());
        assertEquals("false", health.readiness().get("stuck.reminder-intents-fast-0"));
    }

    @Test
    void aRevokedPartitionIsForgottenAndItsStaleReadyValueIsNotUpdatedAgain() {
        broker.assignment = Set.of(P0, P1);
        broker.committed.putAll(Map.of(P0, 5L, P1, 0L));
        broker.ends.putAll(Map.of(P0, 10L, P1, 0L));
        hooks.beforePoll(broker.proxy());   // P0 last known: not yet stuck
        assertEquals("false", health.readiness().get("stuck.reminder-intents-fast-0"));

        broker.assignment = Set.of(P1);   // P0 revoked: no longer tracked or read from the broker
        broker.committed.remove(P0);
        broker.ends.remove(P0);
        tick(Duration.ofMinutes(6));
        hooks.beforePoll(broker.proxy());
        // Health has no "unset": the stale value from before the revoke is what /ready still shows.
        assertEquals("false", health.readiness().get("stuck.reminder-intents-fast-0"));
    }

    final class FakeConsumer {
        Set<TopicPartition> assignment = Set.of();
        final Map<TopicPartition, Long> ends = new HashMap<>();
        final Map<TopicPartition, Long> committed = new HashMap<>();
        RuntimeException endOffsetsFailure;
        int committedCalls;

        @SuppressWarnings("unchecked")
        Consumer<String, byte[]> proxy() {
            return (Consumer<String, byte[]>) Proxy.newProxyInstance(Consumer.class.getClassLoader(),
                new Class<?>[] {Consumer.class}, (self, method, args) -> switch (method.getName()) {
                    case "assignment" -> Set.copyOf(assignment);
                    case "committed" -> {
                        committedCalls++;
                        Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
                        for (TopicPartition tp : assignment) {
                            Long o = committed.get(tp);
                            if (o != null) out.put(tp, new OffsetAndMetadata(o));
                        }
                        yield out;
                    }
                    case "endOffsets" -> {
                        if (endOffsetsFailure != null) throw endOffsetsFailure;
                        yield Map.copyOf(ends);
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        }
    }
}
