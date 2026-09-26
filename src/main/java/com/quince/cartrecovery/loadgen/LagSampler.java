package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.app.Health;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Samples consumer group lag every 5 s via AdminClient; tracks the max and the last (ending) lag per group. */
final class LagSampler {
    private final AdminClient admin;
    private final List<String> groups;
    private final Health health;
    private final Map<String, Long> maxLag = new HashMap<>();
    private final Map<String, Long> endingLag = new HashMap<>();
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);

    LagSampler(String bootstrap, List<String> groups, Health health) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        this.admin = AdminClient.create(props);
        this.groups = groups;
        this.health = health;
        for (String g : groups) { maxLag.put(g, 0L); endingLag.put(g, 0L); }
    }

    void start() {
        scheduler.scheduleAtFixedRate(this::sampleOnce, 0, 5, TimeUnit.SECONDS);
    }

    void stop() {
        scheduler.shutdownNow();
        admin.close();
    }

    private void sampleOnce() {
        try {
            for (String group : groups) {
                long lag = lagFor(group);
                endingLag.put(group, lag);
                maxLag.merge(group, lag, Math::max);
            }
            health.beat("loadgen.lag-sampler");
        } catch (Exception e) {
            System.err.println("lag sample failed: " + e.getMessage());
        }
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

    /** The group with the largest observed max lag, or "none" if every group stayed caught up. */
    String bottleneckStage() {
        return maxLag.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .filter(e -> e.getValue() > 100)
            .map(Map.Entry::getKey)
            .orElse("none — every group stayed within 100 records of caught up");
    }
}
