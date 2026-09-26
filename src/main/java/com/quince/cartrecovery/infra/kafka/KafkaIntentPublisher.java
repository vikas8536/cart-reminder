package com.quince.cartrecovery.infra.kafka;

import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.ports.IntentPublisher;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Publishes to the fast lane for offsetIndex < fastOffsets, otherwise the slow lane; blocks until acknowledged. */
public final class KafkaIntentPublisher implements IntentPublisher {
    private final Producer<String, byte[]> producer;
    private final int fastOffsets;

    public KafkaIntentPublisher(Producer<String, byte[]> producer, int fastOffsets) {
        this.producer = producer;
        this.fastOffsets = fastOffsets;
    }

    @Override
    public void publish(ReminderIntent intent) {
        String topic = Topics.intents(Lane.of(intent.offsetIndex(), fastOffsets));
        KafkaClients.sendAndWait(producer, new ProducerRecord<>(topic, intent.cartId(), JsonCodec.encode(intent)));
    }
}
