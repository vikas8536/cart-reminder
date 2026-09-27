package com.quince.cartrecovery.infra.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/** Creates the 7 topics with equal P (spec §5.1) and refuses to proceed when an existing topic has a different P. */
public final class TopicAdmin {
    private final Admin admin;

    public TopicAdmin(Admin admin) {
        this.admin = admin;
    }

    public void createAll(int partitions, int replicationFactor, int minInsyncReplicas) {
        List<NewTopic> topics = Topics.ALL.stream()
                .map(t -> new NewTopic(t, partitions, (short) replicationFactor).configs(Map.of(
                        "retention.ms", Long.toString(Topics.retention(t).toMillis()),
                        "min.insync.replicas", Integer.toString(minInsyncReplicas))))
                .toList();
        admin.createTopics(topics).values().forEach((name, future) -> {
            try {
                future.get();
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw new IllegalStateException("cannot create topic " + name + ": " + e.getCause().getMessage(), e.getCause());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted creating topic " + name, e);
            }
        });
        verify(partitions);
    }

    public void verify(int partitions) {
        Map<String, Integer> counts = partitionCounts();
        for (String topic : Topics.ALL) {
            Integer actual = counts.get(topic);
            if (actual == null) throw new IllegalStateException("topic " + topic + " is missing");
            if (actual != partitions) {
                throw new IllegalStateException("topic " + topic + " has " + actual + " partitions, expected " + partitions);
            }
        }
    }

    /** Partition count per existing topic of {@link Topics#ALL}; a missing topic is absent from the map. */
    public Map<String, Integer> partitionCounts() {
        Map<String, Integer> counts = new HashMap<>();
        admin.describeTopics(Topics.ALL).topicNameValues().forEach((name, future) -> {
            try {
                counts.put(name, future.get().partitions().size());
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    throw new IllegalStateException("topic check failed: " + e.getCause().getMessage(), e.getCause());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted checking topics", e);
            }
        });
        return counts;
    }
}
