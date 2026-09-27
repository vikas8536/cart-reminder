package com.quince.cartrecovery.infra.kafka;

import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/** reminder-outcomes; blocks so an outcome is durable before the ledger finish that follows it. */
public final class KafkaOutcomeRecorder implements OutcomeRecorder {
    private final Producer<String, byte[]> producer;

    public KafkaOutcomeRecorder(Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    @Override
    public void record(Outcome outcome) {
        KafkaClients.sendAndWait(producer, new ProducerRecord<>(Topics.OUTCOMES, outcome.cartId(), JsonCodec.encode(outcome)));
    }
}
