package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Spec §5.4 watermark writes. beforePoll: at most every 250 ms, read Redis TIME as T and then the end
 * offsets E of the assigned partitions (a failed call takes no snapshot). afterCommit, every iteration
 * including empty polls and backoff: for each assigned partition publish T of the newest snapshot whose
 * E[p] is at or below the committed position; write nothing when none is satisfied. Poll thread only.
 */
final class DetectorWatermarkHooks implements BatchConsumerLoop.Hooks<byte[]> {
    static final Duration SNAPSHOT_INTERVAL = Duration.ofMillis(250);
    static final Duration BROKER_TIMEOUT = Duration.ofSeconds(1);
    static final int KEEP = 8;

    private final Watermark watermark;
    private final Health health;
    private final Metrics metrics;
    private final LongSupplier nanoTime;
    private final WatermarkSnapshots snapshots = new WatermarkSnapshots(KEEP);
    private final Map<TopicPartition, Long> committed = new HashMap<>();
    private boolean attempted;
    private long lastAttemptNanos;

    DetectorWatermarkHooks(Watermark watermark, Health health, Metrics metrics, LongSupplier nanoTime) {
        this.watermark = watermark;
        this.health = health;
        this.metrics = metrics;
        this.nanoTime = nanoTime;
    }

    @Override
    public void beforePoll(Consumer<String, byte[]> consumer) {
        Set<TopicPartition> assigned = consumer.assignment();
        committed.keySet().retainAll(assigned);
        seedCommitted(consumer, assigned);
        long now = nanoTime.getAsLong();
        if (assigned.isEmpty() || (attempted && now - lastAttemptNanos < SNAPSHOT_INTERVAL.toNanos())) return;
        attempted = true;
        lastAttemptNanos = now;
        try {
            Instant t = watermark.now();   // T before E: every record appended before T lies below E
            snapshots.add(t, consumer.endOffsets(assigned, BROKER_TIMEOUT));
        } catch (RuntimeException e) {
            metrics.increment("watermark.snapshot_failed");   // no snapshot; older ones stay valid
        }
    }

    @Override
    public void afterCommit(Consumer<String, byte[]> consumer, Map<TopicPartition, Long> committedNow, int generation) {
        Set<TopicPartition> assigned = consumer.assignment();
        committedNow.forEach((p, offset) -> {
            if (assigned.contains(p)) committed.merge(p, offset, Math::max);
        });
        for (TopicPartition p : assigned) {
            Long position = committed.get(p);
            if (position == null) continue;
            Optional<Instant> t = snapshots.satisfied(p, position);
            if (t.isEmpty()) continue;   // nothing satisfied: write nothing, the entry goes stale after 5 s
            try {
                watermark.publish(p.partition(), generation, t.get());
                long lagMs = Duration.between(t.get(), snapshots.newest().orElse(t.get())).toMillis();
                health.setReady("watermark.lag_ms.p" + p.partition(), Long.toString(lagMs));
            } catch (RuntimeException e) {
                metrics.increment("watermark.publish_failed");
            }
        }
    }

    /** Idle partitions may never be committed by this member; their broker-committed offset is still processed work. */
    private void seedCommitted(Consumer<String, byte[]> consumer, Set<TopicPartition> assigned) {
        Set<TopicPartition> missing = new HashSet<>(assigned);
        missing.removeAll(committed.keySet());
        if (missing.isEmpty()) return;
        try {
            Map<TopicPartition, OffsetAndMetadata> found = consumer.committed(missing, BROKER_TIMEOUT);
            for (TopicPartition p : missing) {
                OffsetAndMetadata o = found.get(p);
                committed.put(p, o == null ? 0L : o.offset());
            }
        } catch (RuntimeException e) {
            metrics.increment("watermark.committed_lookup_failed");
        }
    }
}
