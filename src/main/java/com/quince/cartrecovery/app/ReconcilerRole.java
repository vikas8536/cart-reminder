package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.core.Reconciler;
import com.quince.cartrecovery.infra.dynamo.DynamoCartStateStore;
import com.quince.cartrecovery.infra.dynamo.DynamoSendLedger;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.KafkaOutcomeRecorder;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.infra.redis.RedisMeta;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Every second: check the Redis run_id and role (recording a change when it is observed, even mid-sweep); on a
 * change, once the current work finishes, replay cart-events from 60 s before the recorded change into timer upserts
 * (spec §6.2 failover replay); otherwise sweep all shards when the epoch is missing, at start, and every
 * RECONCILE_INTERVAL. One piece of work runs at a time on a platform worker thread.
 */
public final class ReconcilerRole implements Role {
    public static final String EPOCH_KEY = RedisMeta.EPOCH;
    static final Duration TICK = Duration.ofSeconds(1);
    static final Duration REPLAY_LOOKBACK = Duration.ofSeconds(60);

    @Override
    public String name() { return "reconciler"; }

    @Override
    public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        try (RoleContext ctx = new RoleContext(config)) {
            ctx.verifyStartup();
            TimerStore timers = new RedisTimerStore(ctx.redis(), config.shards(), config.dispatch().lease());
            Reconciler reconciler = new Reconciler(config.recovery(),
                new DynamoCartStateStore(ctx.dynamo(), DynamoTables.CARTS, config.recovery(), config.shards()), timers,
                new DynamoSendLedger(ctx.dynamo(), DynamoTables.SEND_LEDGER, config.dispatch().lease(), config.shards()),
                new KafkaOutcomeRecorder(ctx.producer()), Instant::now, metrics);
            Cycle cycle = new Cycle(config, new RedisMeta(ctx.redis()), timers, reconciler, new RecoveryMetaStore(ctx.dynamo()), health, metrics);
            AtomicBoolean running = new AtomicBoolean(true);
            RoleContext.runLoops(() -> running.set(false), List.of(() -> cycle.loop(running)));
        }
    }

    static final class Cycle {
        private final InfraConfig config;
        private final RedisMeta redis;
        private final TimerStore timers;
        private final Reconciler reconciler;
        private final RecoveryMetaStore meta;
        private final Health health;
        private final Metrics metrics;
        private final Duration smallestBound;
        private Future<?> current;
        private Instant lastSweep;

        Cycle(InfraConfig config, RedisMeta redis, TimerStore timers, Reconciler reconciler,
              RecoveryMetaStore meta, Health health, Metrics metrics) {
            this.config = config;
            this.redis = redis;
            this.timers = timers;
            this.reconciler = reconciler;
            this.meta = meta;
            this.health = health;
            this.metrics = metrics;
            this.smallestBound = config.recovery().latenessBounds().stream().min(Comparator.naturalOrder()).orElseThrow();
        }

        void loop(AtomicBoolean running) {
            // Platform worker: the replay drives a KafkaConsumer, which must not run on a virtual thread.
            ExecutorService work = Executors.newSingleThreadExecutor(Thread.ofPlatform().name("reconciler-work").factory());
            try {
                while (running.get()) {
                    health.beat("reconciler");
                    Identity identity = observeIdentity();   // every tick, even while work is in flight
                    if (identity != null && (current == null || current.isDone())) {
                        report();
                        current = next(work, identity);
                    }
                    if (!RoleContext.sleep(TICK)) return;
                }
            } finally {
                work.shutdownNow();
            }
        }

        private void report() {
            if (current == null) return;
            try {
                current.get();
            } catch (ExecutionException e) {
                metrics.increment("reconciler.errors");
                System.err.println("reconciler: " + e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            current = null;
        }

        /** The current Redis identity, and whether it differs from the one stored in recovery-meta. */
        private record Identity(String runId, String role, boolean changed) {}

        /**
         * Reads the Redis identity and, on a change, records it at the time it is observed (markRedisChange keeps the
         * earliest unrepaired change), so a long sweep cannot push the replay's lookback past the change. Null when
         * Redis or DynamoDB is unreachable (or init has not run): counted, retried next tick.
         */
        private Identity observeIdentity() {
            try {
                String runId = redis.runId();
                String role = redis.role();
                RecoveryMetaStore.Meta m = meta.read();
                if (m.redisRunId() == null) {
                    meta.setRedisIdentity(runId, role);
                    return new Identity(runId, role, false);
                }
                boolean changed = !runId.equals(m.redisRunId()) || !role.equals(m.redisRole());
                if (changed) meta.markRedisChange(Instant.now(), m.redisRunId(), m.redisRole());
                return new Identity(runId, role, changed);
            } catch (RuntimeException e) {
                metrics.increment("reconciler.errors");
                return null;
            }
        }

        private Future<?> next(ExecutorService work, Identity identity) {
            try {
                if (identity.changed()) {
                    Instant stored = meta.read().redisChangeAt();
                    Instant changeAt = stored != null ? stored : Instant.now();
                    return work.submit(() -> { replay(identity.runId(), identity.role(), changeAt); return null; });
                }
                boolean due = lastSweep == null || !Instant.now().isBefore(lastSweep.plus(config.reconcileInterval()));
                if (due || !redis.epochPresent()) {
                    lastSweep = Instant.now();
                    return work.submit(() -> { sweep(); return null; });
                }
            } catch (RuntimeException e) {
                metrics.increment("reconciler.errors");   // Redis or DynamoDB unreachable: try again next tick
            }
            return null;
        }

        private void sweep() throws Exception {
            long start = System.nanoTime();
            try (ExecutorService shards = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> futures = new ArrayList<>();
                for (int s = 0; s < config.shards(); s++) {
                    int shard = s;
                    futures.add(shards.submit(() -> reconciler.reconcileShard(shard)));
                }
                for (Future<?> f : futures) f.get();
            }
            redis.writeEpoch();
            Duration took = Duration.ofNanos(System.nanoTime() - start);
            boolean slow = took.compareTo(smallestBound) > 0;
            health.setReady("reconciler.sweep_ms", Long.toString(took.toMillis()));
            health.setReady("reconciler.sweep_slow", Boolean.toString(slow));
            metrics.increment("reconciler.sweeps");
            System.out.printf("reconciler: sweep of %d shards took %d ms%s%n", config.shards(), took.toMillis(),
                slow ? " (longer than the smallest lateness bound)" : "");
        }

        /**
         * Re-issues CHECK_ABANDON upserts for every edit or resume appended since changeAt − 60 s, up to the end
         * offsets read at the start. Aborts if Redis changes again; the next tick restarts from the earliest
         * change still stored in recovery-meta. Stores the new identity only after a complete replay.
         */
        private void replay(String targetRunId, String targetRole, Instant changeAt) {
            Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            long upserts = 0;
            try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
                List<TopicPartition> parts = consumer.partitionsFor(Topics.CART_EVENTS).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
                consumer.assign(parts);
                Map<TopicPartition, Long> ends = consumer.endOffsets(parts);
                long from = changeAt.minus(REPLAY_LOOKBACK).toEpochMilli();
                Map<TopicPartition, Long> query = new HashMap<>();
                for (TopicPartition p : parts) query.put(p, from);
                Map<TopicPartition, OffsetAndTimestamp> starts = consumer.offsetsForTimes(query);
                for (TopicPartition p : parts) {
                    OffsetAndTimestamp s = starts.get(p);
                    consumer.seek(p, s == null ? ends.get(p) : s.offset());
                }
                while (parts.stream().anyMatch(p -> consumer.position(p) < ends.get(p))) {
                    if (!targetRunId.equals(redis.runId()) || !targetRole.equals(redis.role())) {
                        throw new IllegalStateException("Redis changed again during failover replay; restarting from " + changeAt);
                    }
                    for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(500))) {
                        if (r.offset() >= ends.get(new TopicPartition(r.topic(), r.partition()))) continue;
                        CartEvent event;
                        try {
                            event = JsonCodec.decodeCartEvent(r.value());
                        } catch (RuntimeException e) {
                            continue;   // poison: the detector dead-letters it
                        }
                        if (event instanceof CartEvent.CartEdited || event instanceof CartEvent.CartResumed) {
                            timers.upsert(Timer.checkAbandon(event.cartId(), event.version(),
                                event.occurredAt().plus(config.recovery().window()), r.partition()));
                            upserts++;
                        }
                    }
                }
            }
            meta.setRedisIdentity(targetRunId, targetRole);
            metrics.increment("reconciler.replays");
            System.out.printf("reconciler: failover replay from %s re-issued %d timer upserts%n", changeAt.minus(REPLAY_LOOKBACK), upserts);
        }
    }
}
