package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Dispatcher;
import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.infra.dynamo.DynamoCartStateStore;
import com.quince.cartrecovery.infra.dynamo.DynamoSendLedger;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.KafkaDeadLetterQueue;
import com.quince.cartrecovery.infra.kafka.KafkaOutcomeRecorder;
import com.quince.cartrecovery.infra.kafka.KafkaRecordingSink;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.model.HandleResult;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Two lane consumers (fast, slow) sharing one token bucket, a breaker-wrapped recording sink, and a control
 * loop that polls the ledger retry index every RETRY_POLL, re-checks gate-held partitions, and re-reads the
 * guardrail switch every 5 s.
 */
public final class DispatcherRole implements Role {
    static final int MAX_POLL_RECORDS = 50;
    static final int RETRY_LIMIT = 100;
    static final Duration META_POLL = Duration.ofSeconds(5);

    @Override
    public String name() { return "dispatcher"; }

    @Override
    public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        try (RoleContext ctx = new RoleContext(config)) {
            ctx.verifyStartup();
            logAttempts(config);
            Clock clock = Instant::now;
            Watermark watermark = new RedisWatermark(ctx.redis(), config.partitions());
            GateHolds holds = new GateHolds(watermark, config.dispatch().clockSkew());
            TokenBucket budget = new TokenBucket(config.maxSendRate(), config.fastReserve(), System::nanoTime);
            CircuitBreaker breaker = new CircuitBreaker(
                new KafkaRecordingSink(ctx.producer(), clock, config.sendFailureRate(), new Random()), 100, 0.5, Duration.ofSeconds(30), clock);
            Dispatcher dispatcher = new Dispatcher(config.recovery(), config.dispatch(),
                new DynamoCartStateStore(ctx.dynamo(), DynamoTables.CARTS, config.recovery(), config.shards()),
                new DynamoSendLedger(ctx.dynamo(), DynamoTables.SEND_LEDGER, config.dispatch().lease(), config.shards()),
                watermark, budget, breaker,
                new KafkaOutcomeRecorder(ctx.producer()), new KafkaDeadLetterQueue(ctx.producer()), clock, metrics);
            RecoveryMetaStore meta = new RecoveryMetaStore(ctx.dynamo());
            AtomicBoolean metaPaused = new AtomicBoolean(readPaused(meta, false, metrics));
            AtomicBoolean running = new AtomicBoolean(true);
            BatchConsumerLoop<byte[]> fast = lane(ctx, config, "dispatcher-fast", Topics.INTENTS_FAST, dispatcher, holds, health, metrics);
            BatchConsumerLoop<byte[]> slow = lane(ctx, config, "dispatcher-slow", Topics.INTENTS_SLOW, dispatcher, holds, health, metrics);
            fast.pauseWhile(pauseFast(budget, breaker, metaPaused::get).or(holds::holds));
            slow.pauseWhile(pauseSlow(budget, breaker, metaPaused::get).or(holds::holds));
            Runnable control = () -> controlLoop(dispatcher, breaker, meta, metaPaused, holds, config, running, health, metrics);
            RoleContext.runLoops(() -> {
                running.set(false);
                closeTogether(CLOSE_DEADLINE, fast::close, slow::close);
            }, List.of(fast::run, slow::run, control));
        }
    }

    /** Both lanes drain at once within one deadline, inside Main's 30 s shutdown wait. */
    static final Duration CLOSE_DEADLINE = BatchConsumerLoop.DRAIN_BUDGET.plusSeconds(2);

    /** Starts every closer at once, then waits for all of them until one shared deadline. */
    static void closeTogether(Duration deadline, Runnable... closers) {
        List<Thread> threads = Arrays.stream(closers).map(c -> Thread.ofVirtual().start(c)).toList();
        long end = System.nanoTime() + deadline.toNanos();
        try {
            for (Thread t : threads) t.join(Duration.ofNanos(Math.max(1, end - System.nanoTime())));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Fast lane pauses when the bucket is empty; both pause while the breaker is open or the guardrail switch is set. */
    static Predicate<TopicPartition> pauseFast(TokenBucket budget, CircuitBreaker breaker, BooleanSupplier metaPaused) {
        return tp -> breaker.isOpen() || metaPaused.getAsBoolean() || !budget.anyAvailable();
    }

    /** Slow lane pauses while the bucket is at or below the fast reserve. */
    static Predicate<TopicPartition> pauseSlow(TokenBucket budget, CircuitBreaker breaker, BooleanSupplier metaPaused) {
        return tp -> breaker.isOpen() || metaPaused.getAsBoolean() || !budget.slowAllowed();
    }

    static BatchConsumerLoop<byte[]> lane(RoleContext ctx, InfraConfig config, String group, String topic,
                                          Dispatcher dispatcher, GateHolds holds, Health health, Metrics metrics) {
        return new BatchConsumerLoop<>(ctx.consumerProps(group),
            new BatchConsumerLoop.Settings(group, List.of(topic), MAX_POLL_RECORDS, Duration.ofMillis(500),
                config.maxInFlight(), Topics.REMINDER_DLQ),
            new ByteArrayDeserializer(), record -> handleIntent(dispatcher, holds, record),
            RoleContext.withStuckDetection(new BatchConsumerLoop.Hooks<byte[]>() {}, health), ctx.producer(), health, metrics);
    }

    /**
     * An undeserializable intent is poison (to reminder-dlq); HOLD pauses and seeks back the record's partition,
     * and a HOLD from the watermark gate keeps it paused until the source partition catches up.
     */
    static BatchConsumerLoop.Verdict handleIntent(Dispatcher dispatcher, GateHolds holds, ConsumerRecord<String, byte[]> record) {
        ReminderIntent intent;
        try {
            intent = JsonCodec.decodeIntent(record.value());
        } catch (RuntimeException e) {
            throw new PoisonException("undecodable intent at " + record.topic() + "-" + record.partition()
                + "@" + record.offset(), e);
        }
        if (dispatcher.handle(intent) == HandleResult.DONE) return BatchConsumerLoop.Verdict.DONE;
        holds.onHold(new TopicPartition(record.topic(), record.partition()), intent.srcPartition());
        return BatchConsumerLoop.Verdict.HOLD;
    }

    static void controlLoop(Dispatcher dispatcher, CircuitBreaker breaker, RecoveryMetaStore meta, AtomicBoolean metaPaused,
                            GateHolds holds, InfraConfig config, AtomicBoolean running, Health health, Metrics metrics) {
        long nextMetaRead = System.nanoTime() + META_POLL.toNanos();
        boolean wasOpen = false;
        while (running.get()) {
            health.beat("dispatcher-retry");
            if (System.nanoTime() >= nextMetaRead) {
                metaPaused.set(readPaused(meta, metaPaused.get(), metrics));
                nextMetaRead = System.nanoTime() + META_POLL.toNanos();
            }
            holds.recheck();
            boolean open = breaker.isOpen();
            if (open && !wasOpen) metrics.increment("dispatch.breaker_open");
            wasOpen = open;
            health.setReady("breaker", open ? "open" : "closed");
            health.setReady("paused", Boolean.toString(metaPaused.get()));
            if (!open && !metaPaused.get()) {
                int start = ThreadLocalRandom.current().nextInt(config.shards());
                for (int i = 0; i < config.shards() && running.get(); i++) {
                    try {
                        dispatcher.retryDue((start + i) % config.shards(), RETRY_LIMIT);
                    } catch (RuntimeException e) {
                        metrics.increment("dispatch.retry_error");
                    }
                }
            }
            if (!RoleContext.sleep(config.retryPoll())) return;
        }
    }

    /**
     * Lane partitions held by the watermark gate (spec §5.4), each with the source partition it waits on. The pause
     * predicate reads only this map (BatchConsumerLoop evaluates it inside poll); the control loop re-checks the
     * watermark every RETRY_POLL and releases partitions whose source caught up. Without this, a held partition is
     * redelivered every poll timeout and spends a send token each time, since the Dispatcher takes the token first.
     */
    static final class GateHolds {
        private final Map<TopicPartition, Integer> held = new ConcurrentHashMap<>();
        private final Watermark watermark;
        private final Duration clockSkew;

        GateHolds(Watermark watermark, Duration clockSkew) {
            this.watermark = watermark;
            this.clockSkew = clockSkew;
        }

        /**
         * After a HOLD: hold the partition only if the gate is the reason (a HOLD for want of a token is not). A failed
         * watermark read holds nothing; the loop's ordinary short pause then applies.
         */
        void onHold(TopicPartition tp, int srcPartition) {
            try {
                if (lagging(srcPartition)) held.put(tp, srcPartition);
            } catch (RuntimeException e) {
                // Redis unreachable: the Dispatcher's own gate read fails the same way on redelivery
            }
        }

        boolean holds(TopicPartition tp) {
            return held.containsKey(tp);
        }

        /** Releases partitions whose source partition caught up; on a watermark read failure they stay held. */
        void recheck() {
            for (int src : new HashSet<>(held.values())) {
                try {
                    if (!lagging(src)) held.values().removeIf(v -> v == src);
                } catch (RuntimeException e) {
                    // Redis unreachable: keep holding, the next iteration tries again
                }
            }
        }

        /** Same test as the Dispatcher's gate. */
        private boolean lagging(int srcPartition) {
            return watermark.current(srcPartition).isBefore(watermark.now().minus(clockSkew));
        }
    }

    /** The guardrail switch; on a read failure (including a missing item) keep the last known value. */
    static boolean readPaused(RecoveryMetaStore meta, boolean last, Metrics metrics) {
        try {
            return meta.read().paused();
        } catch (RuntimeException e) {
            metrics.increment("dispatcher.meta_read_failed");
            return last;
        }
    }

    /** Attempts that fit in a lateness bound when each retry waits at most RETRY_BASE × 2^(n−1) (spec §6.3). */
    static int effectiveAttempts(Duration bound, Duration retryBase, int maxAttempts) {
        int attempts = 1;
        Duration waited = Duration.ZERO;
        Duration next = retryBase;
        while (attempts < maxAttempts && waited.plus(next).compareTo(bound) <= 0) {
            waited = waited.plus(next);
            next = next.multipliedBy(2);
            attempts++;
        }
        return attempts;
    }

    private static void logAttempts(InfraConfig config) {
        List<Duration> bounds = config.recovery().latenessBounds();
        for (int i = 0; i < bounds.size(); i++) {
            System.out.printf("dispatcher: offset %d allows about %d send attempts within its %s lateness bound%n", i,
                effectiveAttempts(bounds.get(i), config.recovery().retryBase(), config.recovery().maxSendAttempts()), bounds.get(i));
        }
    }
}
