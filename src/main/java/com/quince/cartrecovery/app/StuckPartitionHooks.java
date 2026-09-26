package com.quince.cartrecovery.app;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Composes with a role's existing hooks to add a /ready stuck-partition signal (spec §7.4, controller ruling F5b):
 * every CHECK_INTERVAL, for each assigned partition, compares the committed offset with the end offset; ready key
 * {@code stuck.<topic>-<partition>} is "true" once the committed offset has not moved for STUCK_AFTER while end
 * stays above it, "false" otherwise. Reads only assignment/committed/endOffsets on the poll thread; never polls.
 */
final class StuckPartitionHooks<V> implements BatchConsumerLoop.Hooks<V> {
    static final Duration CHECK_INTERVAL = Duration.ofSeconds(30);
    static final Duration STUCK_AFTER = Duration.ofMinutes(5);
    static final Duration BROKER_TIMEOUT = Duration.ofSeconds(1);

    private final BatchConsumerLoop.Hooks<V> delegate;
    private final Health health;
    private final LongSupplier nanoTime;
    private final Map<TopicPartition, Long> lastCommitted = new HashMap<>();
    private final Map<TopicPartition, Long> unchangedSinceNanos = new HashMap<>();
    private boolean checked;
    private long lastCheckNanos;

    StuckPartitionHooks(BatchConsumerLoop.Hooks<V> delegate, Health health, LongSupplier nanoTime) {
        this.delegate = delegate;
        this.health = health;
        this.nanoTime = nanoTime;
    }

    @Override
    public void beforePoll(Consumer<String, V> consumer) {
        delegate.beforePoll(consumer);
        long now = nanoTime.getAsLong();
        if (checked && now - lastCheckNanos < CHECK_INTERVAL.toNanos()) return;
        checked = true;
        lastCheckNanos = now;
        Set<TopicPartition> assigned = consumer.assignment();
        lastCommitted.keySet().retainAll(assigned);
        unchangedSinceNanos.keySet().retainAll(assigned);
        if (assigned.isEmpty()) return;
        try {
            Map<TopicPartition, OffsetAndMetadata> committedNow = consumer.committed(assigned, BROKER_TIMEOUT);
            Map<TopicPartition, Long> ends = consumer.endOffsets(assigned, BROKER_TIMEOUT);
            for (TopicPartition tp : assigned) {
                check(tp, committedNow.get(tp), ends.get(tp), now);
            }
        } catch (RuntimeException e) {
            // broker unreachable: leave the previous ready values, try again next interval
        }
    }

    private void check(TopicPartition tp, OffsetAndMetadata committedOffset, Long endOffset, long now) {
        long committed = committedOffset == null ? 0L : committedOffset.offset();
        long end = endOffset == null ? committed : endOffset;
        Long previous = lastCommitted.put(tp, committed);
        long since = previous != null && previous == committed ? unchangedSinceNanos.getOrDefault(tp, now) : now;
        unchangedSinceNanos.put(tp, since);
        boolean stuck = end > committed && now - since >= STUCK_AFTER.toNanos();
        health.setReady("stuck." + tp.topic() + "-" + tp.partition(), Boolean.toString(stuck));
    }

    @Override
    public void afterCommit(Consumer<String, V> consumer, Map<TopicPartition, Long> committed, int generation) {
        delegate.afterCommit(consumer, committed, generation);
    }
}
