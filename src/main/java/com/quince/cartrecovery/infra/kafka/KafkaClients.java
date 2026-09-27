package com.quince.cartrecovery.infra.kafka;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Producer and admin client settings from spec §5.1. */
public final class KafkaClients {
    private KafkaClients() {}

    public static Map<String, Object> producerProps(String bootstrap) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    }

    /** Thread-safe; share one per process. */
    public static Producer<String, byte[]> producer(String bootstrap) {
        return new KafkaProducer<>(producerProps(bootstrap));
    }

    public static Admin admin(String bootstrap) {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
    }

    /** Blocks until the broker acknowledges; any failure is thrown so the caller retries or leaves work uncommitted. */
    public static void sendAndWait(Producer<String, byte[]> producer, ProducerRecord<String, byte[]> record) {
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted producing to " + record.topic(), e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("produce to " + record.topic() + " failed: " + e.getCause().getMessage(), e.getCause());
        }
    }
}
