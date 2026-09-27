package com.quince.cartrecovery.infra.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

class KafkaClientsTest {
    @Test
    void producerIsIdempotentAndWaitsForAllReplicas() {
        Map<String, Object> props = KafkaClients.producerProps("broker:9092");
        assertEquals("broker:9092", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("all", props.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(true, props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }
}
