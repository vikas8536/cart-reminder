package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
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
 * Spec §5.4 watermark writes, with review fix 1. beforePoll: at most every 250 ms, read Redis TIME as T and then the
 * end offsets E of the assigned partitions (a failed call takes no snapshot). afterCommit, every iteration including
 * empty polls and backoff: for each assigned partition publish T of the newest retained snapshot whose E[p] is at or
 * below the committed position, however old, so a lagging detector reports "behind by X" instead of going stale.
 * A detector cut off from the broker keeps republishing its last satisfied time, frozen: both gates hold as they would
 * on a stale entry, and the lag stays visible (a deliberate deviation from spec §5.4's "goes stale"). Once that time is
 * more than {@code history} behind the latest Redis TIME it can no longer gate any send, so it stops being published
 * and the entry goes stale after 5 s; otherwise a cut-off replica would keep its old, higher generation fresh and
 * fence out a consumer-group reset's new owners (wmSet accepts a lower generation only on a stale entry). A partition
 * behind every retained snapshot likewise writes nothing; /ready then shows {@code watermark.lag_ms.p<n>: stale}. The reported lag is the latest Redis TIME read minus the published time.
 * Poll thread only.
 */
final class DetectorWatermarkHooks implements BatchConsumerLoop.Hooks<byte[]> {
    static final Duration SNAPSHOT_INTERVAL = Duration.ofMillis(250);
    static final Duration BROKER_TIMEOUT = Duration.ofSeconds(1);
    static final int KEEP = 8;

    private final Watermark watermark;
    private final Health health;
    private final Metrics metrics;
    private final LongSupplier nanoTime;
    private final Duration history;
    private final WatermarkSnapshots snapshots;
    private final Map<TopicPartition, Long> committed = new HashMap<>();
    private boolean attempted;
    private long lastAttemptNanos;
    private Instant redisNow;

    DetectorWatermarkHooks(Watermark watermark, Health health, Metrics metrics, LongSupplier nanoTime, Duration history) {
        this.watermark = watermark;
        this.health = health;
        this.metrics = metrics;
        this.nanoTime = nanoTime;
        this.history = history;
        this.snapshots = new WatermarkSnapshots(KEEP, history);
    }

    /** How far back snapshots are kept: max(latenessBounds) + CLOCK_SKEW. A partition further behind is past every bound. */
    static Duration history(RecoveryConfig recovery, Duration clockSkew) {
        return recovery.latenessBounds().stream().max(Comparator.naturalOrder()).orElseThrow().plus(clockSkew);
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
            redisNow = t;
            snapshots.add(t, consumer.endOffsets(assigned, BROKER_TIMEOUT));
        } catch (RuntimeException e) {
            metrics.increment("watermark.snapshot_failed");   // no snapshot; the retained ones stay valid
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
            Optional<Instant> t = position == null ? Optional.empty() : snapshots.satisfied(p, position);
            if (t.isEmpty() || Duration.between(t.get(), redisNow).compareTo(history) > 0) {
                // behind every retained snapshot, or a frozen time past the history: write nothing, stale after 5 s
                health.setReady("watermark.lag_ms.p" + p.partition(), "stale");
                continue;
            }
            try {
                watermark.publish(p.partition(), generation, t.get());
                long lagMs = Math.max(0, Duration.between(t.get(), redisNow).toMillis());
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
