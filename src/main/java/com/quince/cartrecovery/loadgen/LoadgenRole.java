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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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
import java.util.concurrent.atomic.AtomicBoolean;
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

    @Override public String name() { return "loadgen"; }

    @Override public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        double rate = Double.parseDouble(System.getenv().getOrDefault("RATE", "5000"));
        Duration duration = Duration.parse(System.getenv().getOrDefault("DURATION", "PT5M"));
        String runPrefix = System.getenv().getOrDefault("RUN_PREFIX", "load-" + System.currentTimeMillis());

        Instant testStart = Instant.now();
        Workload.Result workload = Workload.generate(testStart.toEpochMilli(), runPrefix, rate, duration, testStart, config.recovery());

        // The detector's salt, so Expected skips exactly its holdout carts.
        ArmAssigner assigner = new HashArmAssigner(HashArmAssigner.SALT, config.recovery().holdoutPercent());
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
                AtomicBoolean finalReadCaughtUp = new AtomicBoolean(true);
                try {
                    publishAndDrain(sampler, collector, () -> {
                        try (Producer<String, byte[]> producer = buildProducer(config.kafkaBootstrap())) {
                            publishPaced(producer, workload.events(), testStart, health, producedCount);
                        }
                        publishFinished.set(Instant.now());

                        // Fix round 1, finding 3: bounded wait for the pipeline to actually catch up before
                        // starting the spec's fixed drain wait, so an overloaded run's tail isn't clipped by
                        // a drain sized for the nominal (unloaded) timings.
                        health.setReady("loadgen.phase", "draining-lag");
                        waitForLagToDrain(sampler, timerStore, LAG_DRAIN_TIMEOUT, health);

                        Duration drainWait = drainWait(config.recovery(), config.reconcileInterval());
                        health.setReady("loadgen.phase", "draining");
                        sleepInBeats(drainWait, Duration.ofSeconds(1), () -> health.beat("loadgen"));

                        // Fix round 1, finding 3: catch the collector up to sink-sends' and reminder-outcomes'
                        // end offsets while its poll thread is still running — publishAndDrain's finally stops
                        // it as soon as this body returns, so this must happen before that, not after.
                        health.setReady("loadgen.phase", "final-read");
                        finalReadCaughtUp.set(waitForCollectorCaughtUp(collector, config.kafkaBootstrap(),
                            config.partitions(), FINAL_READ_TIMEOUT, health));
                        return null;
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();   // Role contract: run() returns promptly on interrupt.
                    return;
                }

                writeReport(config, health, runPrefix, rate, duration, testStart, publishFinished.get(), workload,
                    expectedKeys, assigner, purchaseAtByCart, sampler, collector, finalReadCaughtUp.get());
            }
        } finally {
            redisClient.shutdown();
        }
    }

    private static void writeReport(InfraConfig config, Health health, String runPrefix, double rate, Duration nominalDuration,
                              Instant testStart, Instant publishFinished, Workload.Result workload, Set<String> expectedKeys,
                              ArmAssigner assigner, Map<String, Instant> purchaseAtByCart, LagSampler sampler,
                              OutcomeCollector collector, boolean finalReadCaughtUp) {
        List<SinkSend> sends = collector.sinkSends();
        List<OutcomeRow> outcomes = collector.outcomes();

        long duplicates = Accounting.duplicateSends(sends);
        long postPurchase = Accounting.postPurchaseSends(sends, purchaseAtByCart, config.dispatch().clockSkew());
        // Fix round 2: sent/skippedLate/cancelled/dead are restricted to expectedKeys inside here, so an
        // outcome on a non-expected key can't silently cancel out a real miss (see CorrectnessSummary).
        CorrectnessSummary.Result correctness = CorrectnessSummary.compute(expectedKeys, outcomes, workload.scripts(),
            config.recovery(), assigner);

        Map<String, Percentiles.Result> latencyByLane = Map.of(
            "fast", Percentiles.compute(collector.fastLatencies(), testStart, Duration.ofSeconds(30)),
            "slow", Percentiles.compute(collector.slowLatencies(), testStart, Duration.ofSeconds(30)));

        Instant finished = Instant.now();
        double achievedRate = achievedRate(workload.events().size(), testStart, publishFinished);
        long publishSpanSeconds = Duration.between(testStart, publishFinished).toSeconds();

        LoadTestSummary summary = new LoadTestSummary(
            runPrefix, testStart, finished, rate, achievedRate,
            nominalDuration.toSeconds(), publishSpanSeconds,
            sampler.maxLagByGroup(), sampler.endingLagByGroup(), sampler.bottleneckStage(),
            sampler.maxWatermarkLagMillis(), sampler.watermarkEverStale(), sampler.maxPastDueBacklog(),
            latencyByLane,
            expectedKeys.size(), correctness.sent(), correctness.skippedLate(), correctness.cancelled(), correctness.dead(),
            duplicates, postPurchase, correctness.supersededBeforeSend(), correctness.missedWhileLagging(),
            correctness.outcomesOnNonExpectedKeys(), correctness.unexplainedMissing(), correctness.unexplainedMissingRatio(),
            !finalReadCaughtUp,
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

    /** Fix round 1, finding 3: bounds so an overloaded run's tail can never hang the role forever. */
    static final Duration LAG_DRAIN_TIMEOUT = Duration.ofMinutes(10);
    static final Duration FINAL_READ_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_STEP = Duration.ofSeconds(2);

    /**
     * Fix round 1, finding 3: before the spec's fixed drain wait (sized for the nominal, unloaded
     * timings), wait — bounded, beating health every step — until every sampled consumer group has
     * zero lag and Redis has no timer past due, so an overloaded run's real backlog gets a real chance
     * to resolve instead of being clipped by a drain wait that assumes the pipeline was never behind.
     * Gives up (and lets the caller proceed to the spec drain wait regardless) after {@code timeout}.
     */
    static void waitForLagToDrain(LagSampler sampler, RedisTimerStore timerStore, Duration timeout, Health health)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            boolean lagClear;
            try {
                lagClear = sampler.currentLagByGroup().values().stream().allMatch(lag -> lag == 0L);
            } catch (Exception e) {
                lagClear = false;   // broker hiccup: treat as not yet drained, try again next step
            }
            boolean timersClear = timerStore.pastDue() == 0;
            if (lagClear && timersClear) return;
            if (System.nanoTime() >= deadline) {
                System.err.println("loadgen: consumer lag / timer backlog did not drain to zero within " + timeout
                    + "; proceeding to the spec drain wait regardless");
                return;
            }
            health.beat("loadgen");
            Thread.sleep(POLL_STEP.toMillis());
        }
    }

    /**
     * Fix round 1, finding 3: after the drain waits, make sure {@code collector} has actually consumed
     * sink-sends and reminder-outcomes up to the end offsets they had at this moment, so a last-instant
     * race between the collector's poll loop and this read never clips the count. Bounded; if the
     * collector cannot catch up within {@code timeout} (a stuck broker, say) this logs a warning, returns
     * {@code false} (fix round 2: so the caller can flag it in the report instead of silently reporting a
     * possibly-incomplete count as final), and lets the caller compute results from whatever it has.
     */
    static boolean waitForCollectorCaughtUp(OutcomeCollector collector, String bootstrap, int partitions,
                                            Duration timeout, Health health) throws InterruptedException {
        Map<TopicPartition, Long> targets;
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (AdminClient admin = AdminClient.create(props)) {
            Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
            for (String topic : List.of(Topics.SINK_SENDS, Topics.OUTCOMES)) {
                for (int p = 0; p < partitions; p++) specs.put(new TopicPartition(topic, p), OffsetSpec.latest());
            }
            ListOffsetsResult ends = admin.listOffsets(specs);
            targets = new HashMap<>();
            for (TopicPartition tp : specs.keySet()) targets.put(tp, ends.partitionResult(tp).get().offset());
        } catch (Exception e) {
            System.err.println("loadgen: could not fetch end offsets for the final read: " + e.getMessage());
            return false;
        }

        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            Map<TopicPartition, Long> positions = collector.positions();
            boolean caughtUp = targets.entrySet().stream()
                .allMatch(e -> e.getValue() == 0 || positions.getOrDefault(e.getKey(), 0L) >= e.getValue());
            if (caughtUp) return true;
            if (System.nanoTime() >= deadline) {
                System.err.println("loadgen: final read did not catch up to end offsets within " + timeout);
                return false;
            }
            health.beat("loadgen");
            Thread.sleep(500);
        }
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
