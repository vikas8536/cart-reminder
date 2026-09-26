package com.quince.cartrecovery.app;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Reads a whole topic from the beginning without a group and keeps every record seen, in partition order. */
public final class TopicTail implements AutoCloseable {
    private final KafkaConsumer<String, byte[]> consumer;
    private final List<ConsumerRecord<String, byte[]>> seen = new ArrayList<>();

    public TopicTail(String bootstrap, String topic) {
        consumer = new KafkaConsumer<>(Map.<String, Object>of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class));
        List<TopicPartition> parts = consumer.partitionsFor(topic).stream()
            .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
        consumer.assign(parts);
        consumer.seekToBeginning(parts);
    }

    /** Polls what is new, then returns every record so far whose key starts with keyPrefix. */
    public synchronized List<ConsumerRecord<String, byte[]>> records(String keyPrefix) {
        for (int i = 0; i < 5; i++) {
            ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(100));
            if (batch.isEmpty()) break;
            batch.forEach(seen::add);
        }
        return seen.stream().filter(r -> r.key() != null && r.key().startsWith(keyPrefix)).toList();
    }

    @Override
    public synchronized void close() {
        consumer.close();
    }
}
