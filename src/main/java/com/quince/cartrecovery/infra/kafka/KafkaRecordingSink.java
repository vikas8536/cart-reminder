package com.quince.cartrecovery.infra.kafka;

import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.NotificationSink;
import java.util.Random;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Stands in for the notification gateway: never sends, records one {@code sink-sends} record per delivered send
 * (key, cartId, at, hasFirstName, itemCount; no personal data). {@code failureRate} injects transient failures
 * for load tests; an injected failure delivers and records nothing.
 */
public final class KafkaRecordingSink implements NotificationSink {
    private final Producer<String, byte[]> producer;
    private final Clock clock;
    private final double failureRate;
    private final Random random;

    public KafkaRecordingSink(Producer<String, byte[]> producer, Clock clock, double failureRate, Random random) {
        this.producer = producer;
        this.clock = clock;
        this.failureRate = failureRate;
        this.random = random;
    }

    @Override
    public SendResult send(ReminderMessage m) {
        if (failureRate > 0 && random.nextDouble() < failureRate) return SendResult.TRANSIENT_FAILURE;
        boolean hasFirstName = m.firstName() != null && !m.firstName().isBlank();
        JsonCodec.SinkSend record = new JsonCodec.SinkSend(m.key(), m.cartId(), clock.now(), hasFirstName, m.items().size());
        try {
            KafkaClients.sendAndWait(producer, new ProducerRecord<>(Topics.SINK_SENDS, m.cartId(), JsonCodec.encode(record)));
        } catch (IllegalStateException unknownDelivery) {
            // Like a gateway timeout: the dispatcher retries under the same idempotency key.
            return SendResult.TRANSIENT_FAILURE;
        }
        return SendResult.SENT;
    }
}
