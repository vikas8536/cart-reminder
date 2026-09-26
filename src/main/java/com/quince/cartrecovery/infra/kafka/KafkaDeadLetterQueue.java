package com.quince.cartrecovery.infra.kafka;

import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.ports.DeadLetterQueue;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/** reminder-dlq; blocks so every DEAD row has a replayable record before finish(DEAD). */
public final class KafkaDeadLetterQueue implements DeadLetterQueue {
    private final Producer<String, byte[]> producer;

    public KafkaDeadLetterQueue(Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    @Override
    public void add(DeadLetter letter) {
        KafkaClients.sendAndWait(producer,
                new ProducerRecord<>(Topics.REMINDER_DLQ, letter.intent().cartId(), JsonCodec.encode(letter)));
    }
}
