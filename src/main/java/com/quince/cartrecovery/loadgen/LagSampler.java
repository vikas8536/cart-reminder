package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.app.Health;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Samples, every 5 s (spec §8.5): the achieved produce rate (from a shared counter the publisher
 * increments), consumer group lag via AdminClient, the per-partition Redis watermark lag (Redis
 * TIME minus the furthest-behind partition's watermark), and the Redis timer backlog past due.
 * Tracks the max (and, for consumer lag, the ending) value of each, and prints one console line
 * per sample.
 */
final class LagSampler {
    private final AdminClient admin;
    private final List<String> groups;
    private final Health health;
    private final AtomicLong producedCount;
    private final RedisWatermark watermark;
    private final RedisTimerStore timerStore;
    private final int partitions;

    private final Map<String, Long> maxLag = new HashMap<>();
    private final Map<String, Long> endingLag = new HashMap<>();
    private final AtomicLong maxWatermarkLagMillis = new AtomicLong();
    private final AtomicLong maxPastDueBacklog = new AtomicLong();
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);

    private long lastProducedCount;
    private long lastSampleNanos = System.nanoTime();

    LagSampler(String bootstrap, List<String> groups, Health health, AtomicLong producedCount,
               RedisWatermark watermark, RedisTimerStore timerStore, int partitions) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        this.admin = AdminClient.create(props);
        this.groups = groups;
        this.health = health;
        this.producedCount = producedCount;
        this.watermark = watermark;
        this.timerStore = timerStore;
        this.partitions = partitions;
        for (String g : groups) { maxLag.put(g, 0L); endingLag.put(g, 0L); }
    }

    void start() {
        scheduler.scheduleAtFixedRate(this::sampleOnce, 0, 5, TimeUnit.SECONDS);
    }

    void stop() {
        scheduler.shutdownNow();
        admin.close();
    }

    boolean isStopped() { return scheduler.isShutdown(); }

    /** Package-visible so a test can trigger one sample directly instead of waiting on the 5 s schedule. */
    void sampleOnce() {
        try {
            double producedRate = sampleProducedRate();

            Map<String, Long> lagNow = new HashMap<>();
            for (String group : groups) {
                long lag = lagFor(group);
                endingLag.put(group, lag);
                maxLag.merge(group, lag, Math::max);
                lagNow.put(group, lag);
            }

            long watermarkLagMs = sampleWatermarkLagMillis();
            maxWatermarkLagMillis.accumulateAndGet(watermarkLagMs, Math::max);

            long backlog = timerStore.pastDue();
            maxPastDueBacklog.accumulateAndGet(backlog, Math::max);

            System.out.printf(
                "[loadgen sample] producedRate=%.1f/s consumerLag=%s watermarkLagMs=%d timerBacklogPastDue=%d%n",
                producedRate, lagNow, watermarkLagMs, backlog);
            health.beat("loadgen.sampler");
        } catch (Exception e) {
            System.err.println("sample failed: " + e.getMessage());
        }
    }

    /** Events produced since the last sample, divided by the elapsed wall time. */
    private double sampleProducedRate() {
        long now = System.nanoTime();
        long count = producedCount.get();
        double seconds = (now - lastSampleNanos) / 1_000_000_000.0;
        double rate = seconds > 0 ? (count - lastProducedCount) / seconds : 0.0;
        lastProducedCount = count;
        lastSampleNanos = now;
        return rate;
    }

    /** Redis TIME minus the oldest per-partition watermark: how far behind the furthest partition is. */
    private long sampleWatermarkLagMillis() {
        Instant now = watermark.now();
        long max = 0;
        for (int p = 0; p < partitions; p++) {
            max = Math.max(max, Duration.between(watermark.current(p), now).toMillis());
        }
        return max;
    }

    private long lagFor(String group) throws InterruptedException, ExecutionException {
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
            admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
        if (committed.isEmpty()) return 0L;

        Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
        for (TopicPartition tp : committed.keySet()) latestSpecs.put(tp, OffsetSpec.latest());
        ListOffsetsResult ends = admin.listOffsets(latestSpecs);

        long total = 0L;
        for (TopicPartition tp : committed.keySet()) {
            long end = ends.partitionResult(tp).get().offset();
            long pos = committed.get(tp).offset();
            total += Math.max(0, end - pos);
        }
        return total;
    }

    Map<String, Long> maxLagByGroup() { return Map.copyOf(maxLag); }
    Map<String, Long> endingLagByGroup() { return Map.copyOf(endingLag); }
    long maxWatermarkLagMillis() { return maxWatermarkLagMillis.get(); }
    long maxPastDueBacklog() { return maxPastDueBacklog.get(); }

    /** The group with the largest observed max lag, or "none" if every group stayed caught up. */
    String bottleneckStage() {
        return maxLag.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .filter(e -> e.getValue() > 100)
            .map(Map.Entry::getKey)
            .orElse("none — every group stayed within 100 records of caught up");
    }
}
