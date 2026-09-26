package com.quince.cartrecovery.infra.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class KafkaAdaptersTest {
    private static final Instant T = Instant.parse("2026-01-01T09:00:00Z");
    private static final CartItem ITEM = new CartItem("SKU-1", "Linen Shirt", 1, 4990);
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final Producer<String, byte[]> producer = TestKafka.producer();
    private final String prefix = "b3-" + UUID.randomUUID() + "-";

    private static List<byte[]> values(List<ConsumerRecord<String, byte[]>> records) {
        return records.stream().map(ConsumerRecord::value).toList();
    }

    @Test
    void intentsGoToTheirLaneKeyedByCartId() {
        String cartId = prefix + "cart";
        List<ReminderIntent> intents = IntStream.range(0, 3)
                .mapToObj(i -> new ReminderIntent(new LedgerKey(cartId, 1, i).toString(), cartId, 1, i, 2, T, T.plusSeconds(300)))
                .toList();
        KafkaIntentPublisher publisher = new KafkaIntentPublisher(producer, 2);
        intents.forEach(publisher::publish);

        List<ConsumerRecord<String, byte[]>> fast = TestKafka.read(Topics.INTENTS_FAST, prefix, 2, WAIT);
        List<ConsumerRecord<String, byte[]>> slow = TestKafka.read(Topics.INTENTS_SLOW, prefix, 1, WAIT);
        assertEquals(intents.subList(0, 2), values(fast).stream().map(JsonCodec::decodeIntent).toList());
        assertEquals(intents.subList(2, 3), values(slow).stream().map(JsonCodec::decodeIntent).toList());
        assertEquals(cartId, fast.get(0).key());
    }

    @Test
    void outcomesAndDeadLettersRoundTrip() {
        String cartId = prefix + "cart";
        Outcome sent = new Outcome(cartId + ":3:0", cartId, 3, Arm.TREATMENT, OutcomeKind.SENT, T, 2);
        Outcome abandoned = new Outcome(null, cartId, 3, Arm.HOLDOUT, OutcomeKind.ABANDONED, T, 0);
        KafkaOutcomeRecorder recorder = new KafkaOutcomeRecorder(producer);
        recorder.record(sent);
        recorder.record(abandoned);
        assertEquals(List.of(sent, abandoned),
                values(TestKafka.read(Topics.OUTCOMES, prefix, 2, WAIT)).stream().map(JsonCodec::decodeOutcome).toList());

        ReminderIntent intent = new ReminderIntent(cartId + ":3:1", cartId, 3, 1, 0, T, T.plusSeconds(300));
        DeadLetter letter = new DeadLetter(intent, "permanent", T.plusSeconds(5));
        new KafkaDeadLetterQueue(producer).add(letter);
        assertEquals(List.of(letter),
                values(TestKafka.read(Topics.REMINDER_DLQ, prefix, 1, WAIT)).stream().map(JsonCodec::decodeDeadLetter).toList());
    }

    @Test
    void recordingSinkWritesOneRecordPerDeliveredSendWithoutPersonalData() throws Exception {
        Random scripted = new Random() {
            private final double[] draws = {0.1, 0.9};
            private int next;

            @Override
            public double nextDouble() { return draws[next++]; }
        };
        KafkaRecordingSink sink = new KafkaRecordingSink(producer, () -> T, 0.5, scripted);
        ReminderMessage message = new ReminderMessage(prefix + "cart:1:0", prefix + "cart", "shopper@example.com", "Ada", List.of(ITEM, ITEM));

        assertEquals(SendResult.TRANSIENT_FAILURE, sink.send(message), "draw 0.1 < 0.5 injects a failure");
        assertEquals(SendResult.SENT, sink.send(message));

        List<ConsumerRecord<String, byte[]>> records = TestKafka.read(Topics.SINK_SENDS, prefix, 2, Duration.ofSeconds(3));
        assertEquals(1, records.size(), "the injected failure delivered nothing and recorded nothing");
        Set<String> fields = new HashSet<>();
        new ObjectMapper().readTree(records.get(0).value()).fieldNames().forEachRemaining(fields::add);
        assertEquals(Set.of("schemaVersion", "key", "cartId", "at", "hasFirstName", "itemCount"), fields, "no name, shopper key or items");
        assertEquals(new JsonCodec.SinkSend(message.key(), message.cartId(), T, true, 2), JsonCodec.decodeSinkSend(records.get(0).value()));
    }

    @Test
    void sinkRecordsNoNameAndNoItems() {
        KafkaRecordingSink sink = new KafkaRecordingSink(producer, () -> T, 0.0, new Random(1));
        ReminderMessage message = new ReminderMessage(prefix + "cart:1:0", prefix + "cart", "s", null, List.of());
        assertEquals(SendResult.SENT, sink.send(message));
        List<ConsumerRecord<String, byte[]>> records = TestKafka.read(Topics.SINK_SENDS, prefix, 1, WAIT);
        assertEquals(new JsonCodec.SinkSend(message.key(), message.cartId(), T, false, 0), JsonCodec.decodeSinkSend(records.get(0).value()));
    }
}
