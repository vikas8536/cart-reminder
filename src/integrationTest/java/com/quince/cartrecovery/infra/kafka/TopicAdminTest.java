package com.quince.cartrecovery.infra.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class TopicAdminTest {
    @Test
    void createsAllSevenTopicsWithEqualPartitionsIdempotently() throws Exception {
        try (Admin admin = TestKafka.admin()) {
            new TopicAdmin(admin).createAll(TestKafka.PARTITIONS, 1, 1); // TestKafka already created them once

            Map<String, TopicDescription> topics = admin.describeTopics(Topics.ALL).allTopicNames().get();
            assertEquals(7, topics.size());
            topics.values().forEach(t -> assertEquals(TestKafka.PARTITIONS, t.partitions().size(), t.name()));

            ConfigResource dlq = new ConfigResource(ConfigResource.Type.TOPIC, Topics.REMINDER_DLQ);
            ConfigResource events = new ConfigResource(ConfigResource.Type.TOPIC, Topics.CART_EVENTS);
            Map<ConfigResource, Config> configs = admin.describeConfigs(List.of(dlq, events)).all().get();
            assertEquals(Long.toString(Duration.ofDays(30).toMillis()), configs.get(dlq).get("retention.ms").value());
            assertEquals(Long.toString(Duration.ofDays(7).toMillis()), configs.get(events).get("retention.ms").value());
            assertEquals("1", configs.get(events).get("min.insync.replicas").value());
        }
    }

    @Test
    void partitionCountsListsEveryExistingTopic() {
        try (Admin admin = TestKafka.admin()) {
            Map<String, Integer> counts = new TopicAdmin(admin).partitionCounts();
            assertEquals(Topics.ALL.size(), counts.size());
            for (String topic : Topics.ALL) assertEquals(TestKafka.PARTITIONS, counts.get(topic), topic);
        }
    }

    @Test
    void refusesAPartitionMismatchNamingTheTopic() {
        try (Admin admin = TestKafka.admin()) {
            TopicAdmin topics = new TopicAdmin(admin);
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> topics.verify(TestKafka.PARTITIONS + 1));
            assertTrue(e.getMessage().contains(Topics.CART_EVENTS), e.getMessage());
            assertThrows(IllegalStateException.class, () -> topics.createAll(TestKafka.PARTITIONS + 1, 1, 1));
        }
    }
}
