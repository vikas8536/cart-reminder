package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.app.Health;
import com.quince.cartrecovery.app.InfraConfig;
import com.quince.cartrecovery.app.Role;
import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.inmemory.HashArmAssigner;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * One-off role: scripts a workload (D0), publishes it to {@code cart-events} at a real-time-paced
 * rate, waits out the drain period, reads back {@code sink-sends} and {@code reminder-outcomes}
 * filtered to its own run prefix, and writes the load test report (spec 8.5). Takes no CLI arguments:
 * RATE, DURATION and RUN_PREFIX come from the environment (controller ruling R9).
 */
public final class LoadgenRole implements Role {
    /** The salt every infra role uses (RoleContext.ARM_SALT), so Expected skips exactly the detector's holdout carts. */
    static final String ARM_SALT = "cart-recovery-v1";

    @Override public String name() { return "loadgen"; }

    @Override public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        double rate = Double.parseDouble(System.getenv().getOrDefault("RATE", "5000"));
        Duration duration = Duration.parse(System.getenv().getOrDefault("DURATION", "PT5M"));
        String runPrefix = System.getenv().getOrDefault("RUN_PREFIX", "load-" + System.currentTimeMillis());

        Instant testStart = Instant.now();
        Workload.Result workload = Workload.generate(testStart.toEpochMilli(), runPrefix, rate, duration, testStart, config.recovery());

        ArmAssigner assigner = new HashArmAssigner(ARM_SALT, config.recovery().holdoutPercent());
        Set<String> expectedKeys = Expected.keys(workload.scripts(), config.recovery(), assigner);
        // The publisher replays the nominal script shifted so its first event lands at testStart.
        Duration shift = workload.events().isEmpty() ? Duration.ZERO
            : Duration.between(workload.events().get(0).occurredAt(), testStart);
        Map<String, Instant> purchaseAtByCart = new HashMap<>();
        Map<String, Instant> lastActivityByCartVersion = new HashMap<>();   // "cartId:version" -> real lastActivityAt
        for (CartScript script : workload.scripts()) {
            if (script.purchaseAt() != null) purchaseAtByCart.put(script.cartId(), script.purchaseAt().plus(shift));
            for (Cycle cycle : script.cycles()) {
                lastActivityByCartVersion.put(script.cartId() + ":" + cycle.version(), cycle.lastActivityAt().plus(shift));
            }
        }

        List<String> groups = List.of("detector", "dispatcher-fast", "dispatcher-slow");
        AtomicLong producedCount = new AtomicLong();

        RedisClient redisClient = RedisClient.create(config.redisUrl());
        try {
            try (StatefulRedisConnection<String, String> redisConnection = redisClient.connect()) {
                RedisWatermark watermark = new RedisWatermark(redisConnection, config.partitions());
                RedisTimerStore timerStore = new RedisTimerStore(redisConnection, config.shards(), config.dispatch().lease());

                LagSampler sampler = new LagSampler(config.kafkaBootstrap(), groups, health, producedCount,
                    watermark, timerStore, config.partitions());
                OutcomeCollector collector = new OutcomeCollector(config.kafkaBootstrap(), runPrefix, lastActivityByCartVersion,
                    config.recovery().offsets(), config.dispatch().fastOffsets());

                AtomicReference<Instant> publishFinished = new AtomicReference<>();
                try {
                    publishAndDrain(sampler, collector, () -> {
                        try (Producer<String, byte[]> producer = buildProducer(config.kafkaBootstrap())) {
                            publishPaced(producer, workload.events(), testStart, health, producedCount);
                        }
                        publishFinished.set(Instant.now());

                        Duration drainWait = drainWait(config.recovery(), config.reconcileInterval());
                        health.setReady("loadgen.phase", "draining");
                        sleepInBeats(drainWait, Duration.ofSeconds(1), () -> health.beat("loadgen"));
                        return null;
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();   // Role contract: run() returns promptly on interrupt.
                    return;
                }

                writeReport(config, health, runPrefix, rate, testStart, publishFinished.get(), workload, expectedKeys,
                    purchaseAtByCart, sampler, collector);
            }
        } finally {
            redisClient.shutdown();
        }
    }

    private static void writeReport(InfraConfig config, Health health, String runPrefix, double rate, Instant testStart,
                              Instant publishFinished, Workload.Result workload, Set<String> expectedKeys,
                              Map<String, Instant> purchaseAtByCart, LagSampler sampler, OutcomeCollector collector) {
        List<SinkSend> sends = collector.sinkSends();
        List<OutcomeRow> outcomes = collector.outcomes();

        Map<String, String> resolved = Accounting.resolveOutcomes(outcomes);
        long sent = Accounting.countByKind(resolved, "SENT");
        long skippedLate = Accounting.countByKind(resolved, "SKIPPED_LATE");
        long cancelled = Accounting.countByKind(resolved, "CANCELLED");
        long dead = Accounting.countByKind(resolved, "DEAD");
        long duplicates = Accounting.duplicateSends(sends);
        long postPurchase = Accounting.postPurchaseSends(sends, purchaseAtByCart, config.dispatch().clockSkew());
        long unexplainedMissing = Accounting.unexplainedMissing(expectedKeys.size(), sent, skippedLate, cancelled, dead);
        double ratio = Accounting.unexplainedMissingRatio(unexplainedMissing, expectedKeys.size());

        Map<String, Percentiles.Result> latencyByLane = Map.of(
            "fast", Percentiles.compute(collector.fastLatencies(), testStart, Duration.ofSeconds(30)),
            "slow", Percentiles.compute(collector.slowLatencies(), testStart, Duration.ofSeconds(30)));

        Instant finished = Instant.now();
        double achievedRate = achievedRate(workload.events().size(), testStart, publishFinished);

        LoadTestSummary summary = new LoadTestSummary(
            runPrefix, testStart, finished, rate, achievedRate,
            sampler.maxLagByGroup(), sampler.endingLagByGroup(), sampler.bottleneckStage(),
            sampler.maxWatermarkLagMillis(), sampler.maxPastDueBacklog(),
            latencyByLane,
            expectedKeys.size(), sent, skippedLate, cancelled, dead,
            duplicates, postPurchase, unexplainedMissing, ratio,
            Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().maxMemory());

        java.nio.file.Path written = Report.write(summary, java.nio.file.Path.of("build/reports/load"));
        System.out.println(Report.render(summary));
        System.out.println("Report written to " + written);
        health.beat("loadgen");
    }

    /** window + last offset + last lateness bound + one reconcile interval: how long to wait for the tail of the sequence to finish. */
    static Duration drainWait(RecoveryConfig recovery, Duration reconcileInterval) {
        Duration lastOffset = recovery.offsets().get(recovery.offsets().size() - 1);
        Duration lastLateness = recovery.latenessBounds().get(recovery.latenessBounds().size() - 1);
        return recovery.window().plus(lastOffset).plus(lastLateness).plus(reconcileInterval);
    }

    /** Events produced divided by the wall time actually spent producing (not including the drain wait). */
    static double achievedRate(int eventCount, Instant publishStart, Instant publishFinished) {
        double seconds = Duration.between(publishStart, publishFinished).toSeconds();
        return seconds > 0 ? eventCount / seconds : 0.0;
    }

    /**
     * Starts {@code sampler} and {@code collector}, runs {@code body}, and always stops both
     * afterward, even when {@code body} is interrupted or throws, so a leftover sample scheduler,
     * AdminClient or poll thread never outlives the role.
     */
    static void publishAndDrain(LagSampler sampler, OutcomeCollector collector, Callable<Void> body) throws Exception {
        sampler.start();
        collector.start();
        try {
            body.call();
        } finally {
            sampler.stop();
            collector.stop();
        }
    }

    /** Sleeps {@code total}, calling {@code onBeat} after every {@code step}, so a long wait still shows liveness. */
    static void sleepInBeats(Duration total, Duration step, Runnable onBeat) throws InterruptedException {
        long remainingMillis = total.toMillis();
        long stepMillis = Math.max(1, step.toMillis());
        while (remainingMillis > 0) {
            long thisStep = Math.min(stepMillis, remainingMillis);
            Thread.sleep(thisStep);
            remainingMillis -= thisStep;
            onBeat.run();
        }
    }

    private static Producer<String, byte[]> buildProducer(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaProducer<>(props);
    }

    /** Anchors the script's nominal timeline to the real test start and sleeps to each event's real send time. */
    private static void publishPaced(Producer<String, byte[]> producer, List<ScriptedEvent> events,
                                       Instant testStart, Health health, AtomicLong producedCount) throws InterruptedException {
        if (events.isEmpty()) return;
        Instant nominalStart = events.get(0).occurredAt();
        int beat = 0;
        for (ScriptedEvent event : events) {
            Instant target = testStart.plus(Duration.between(nominalStart, event.occurredAt()));
            long waitMillis = Duration.between(Instant.now(), target).toMillis();
            if (waitMillis > 0) Thread.sleep(waitMillis);

            CartEvent cartEvent = toCartEvent(event, Instant.now());
            producer.send(new ProducerRecord<>(Topics.CART_EVENTS, event.cartId(), JsonCodec.encode(cartEvent)));
            producedCount.incrementAndGet();
            if (++beat % 200 == 0) health.beat("loadgen");
        }
        producer.flush();
    }

    private static CartEvent toCartEvent(ScriptedEvent event, Instant realOccurredAt) {
        return switch (event.type()) {
            case EDIT -> new CartEvent.CartEdited(event.cartId(), event.shopperKey(), event.version(), realOccurredAt,
                IntStream.range(0, event.itemCount())
                    .mapToObj(i -> new CartItem("SKU-" + i, "Item " + i, 1, 999))
                    .toList(),
                null);
            case RESUME -> new CartEvent.CartResumed(event.cartId(), event.shopperKey(), event.version(), realOccurredAt);
            case PURCHASE -> new CartEvent.CartPurchased(event.cartId(), event.shopperKey(), event.version(), realOccurredAt);
        };
    }
}
