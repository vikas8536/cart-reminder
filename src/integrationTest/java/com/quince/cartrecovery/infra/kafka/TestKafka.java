package com.quince.cartrecovery.infra.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** One single-node KRaft broker shared by every Kafka integration test in the JVM, with the 7 topics created once. */
public final class TestKafka {
    public static final int PARTITIONS = 3;

    private static KafkaContainer container;
    private static Producer<String, byte[]> producer;

    private TestKafka() {}

    public static synchronized String bootstrap() {
        if (container == null) {
            container = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));
            container.start();
            try (Admin admin = KafkaClients.admin(container.getBootstrapServers())) {
                new TopicAdmin(admin).createAll(PARTITIONS, 1, 1);
            }
        }
        return container.getBootstrapServers();
    }

    public static synchronized Producer<String, byte[]> producer() {
        if (producer == null) producer = KafkaClients.producer(bootstrap());
        return producer;
    }

    public static Admin admin() {
        return KafkaClients.admin(bootstrap());
    }

    /** Reads the topic from the beginning until {@code expected} records with the key prefix arrive or the timeout passes. */
    public static List<ConsumerRecord<String, byte[]>> read(String topic, String keyPrefix, int expected, Duration timeout) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
            long deadline = System.nanoTime() + timeout.toNanos();
            while (out.size() < expected && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(200))) {
                    if (r.key() != null && r.key().startsWith(keyPrefix)) out.add(r);
                }
            }
            return out;
        }
    }
}
