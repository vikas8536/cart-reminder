package com.quince.cartrecovery.infra.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.ReminderIntent;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonCodecTest {
    private static final Instant T = Instant.parse("2026-01-01T09:00:00.123Z"); // 1767258000123
    private static final CartItem ITEM = new CartItem("SKU-1", "Linen Shirt", 1, 4990);
    private static final ReminderIntent INTENT =
            new ReminderIntent("a:b|c:3:1", "a:b|c", 3, 1, 5, T, T.plusSeconds(300));
    private static final ObjectMapper JSON = new ObjectMapper();

    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    @Test
    void everyCartEventTypeRoundTrips() throws Exception {
        List<CartEvent> events = List.of(
                new CartEvent.CartEdited("a:b|c", "s1", 3, T, List.of(ITEM), "Ada"),
                new CartEvent.CartResumed("c", "s1", 4, T),
                new CartEvent.CartCleared("c", "s1", 5, T),
                new CartEvent.CartPurchased("c", "s1", 6, T));
        List<String> types = List.of("EDITED", "RESUMED", "CLEARED", "PURCHASED");
        for (int i = 0; i < events.size(); i++) {
            byte[] json = JsonCodec.encode(events.get(i));
            assertEquals(types.get(i), JSON.readTree(json).get("type").asText());
            assertEquals(1767258000123L, JSON.readTree(json).get("occurredAt").asLong());
            assertEquals(events.get(i), JsonCodec.decodeCartEvent(json));
        }
    }

    @Test
    void absentFirstNameAndEmptyItemsStayAbsentAndEmpty() throws Exception {
        CartEvent.CartEdited e = new CartEvent.CartEdited("c", "s", 1, T, List.of(), null);
        JsonNode node = JSON.readTree(JsonCodec.encode(e));
        assertFalse(node.has("firstName"));
        assertEquals(0, node.get("items").size());
        assertEquals(e, JsonCodec.decodeCartEvent(JsonCodec.encode(e)));

        CartEvent minimal = JsonCodec.decodeCartEvent(bytes(
                "{\"schemaVersion\":1,\"type\":\"EDITED\",\"cartId\":\"c\",\"shopperKey\":\"s\",\"version\":1,\"occurredAt\":1767258000123}"));
        assertEquals(e, minimal, "a producer that omits items and firstName decodes to empty and absent");
    }

    @Test
    void everyPayloadCarriesSchemaVersionOne() throws Exception {
        List<byte[]> payloads = List.of(
                JsonCodec.encode(new CartEvent.CartResumed("c", "s", 1, T)),
                JsonCodec.encode(INTENT),
                JsonCodec.encode(new Outcome("k", "c", 1, Arm.TREATMENT, OutcomeKind.SENT, T, 1)),
                JsonCodec.encode(new DeadLetter(INTENT, "permanent", T)),
                JsonCodec.encode(new JsonCodec.SinkSend("k", "c", T, false, 0)));
        for (byte[] p : payloads) assertEquals(1, JSON.readTree(p).get("schemaVersion").asInt());
    }

    @Test
    void intentOutcomeDeadLetterAndSinkSendRoundTrip() {
        assertEquals(INTENT, JsonCodec.decodeIntent(JsonCodec.encode(INTENT)));

        Outcome sent = new Outcome("k:1:0", "c", 1, Arm.TREATMENT, OutcomeKind.SENT, T, 2);
        assertEquals(sent, JsonCodec.decodeOutcome(JsonCodec.encode(sent)));
        Outcome abandoned = new Outcome(null, "c", 1, Arm.HOLDOUT, OutcomeKind.ABANDONED, T, 0);
        assertEquals(abandoned, JsonCodec.decodeOutcome(JsonCodec.encode(abandoned)));

        DeadLetter letter = new DeadLetter(INTENT, "permanent", T.plusSeconds(1));
        assertEquals(letter, JsonCodec.decodeDeadLetter(JsonCodec.encode(letter)));

        JsonCodec.SinkSend send = new JsonCodec.SinkSend("k:1:0", "c", T, true, 2);
        assertEquals(send, JsonCodec.decodeSinkSend(JsonCodec.encode(send)));
    }

    @Test
    void outcomeWithoutKeyOmitsTheField() throws Exception {
        byte[] json = JsonCodec.encode(new Outcome(null, "c", 1, Arm.HOLDOUT, OutcomeKind.ABANDONED, T, 0));
        assertFalse(JSON.readTree(json).has("key"));
    }

    @Test
    void unknownPropertiesAreIgnored() {
        ReminderIntent decoded = JsonCodec.decodeIntent(bytes(
                "{\"schemaVersion\":2,\"key\":\"c:1:0\",\"cartId\":\"c\",\"version\":1,\"offsetIndex\":0,"
                        + "\"srcPartition\":2,\"scheduledFor\":1000,\"sendBy\":2000,\"futureField\":{\"x\":1}}"));
        assertEquals(new ReminderIntent("c:1:0", "c", 1, 0, 2, Instant.ofEpochMilli(1000), Instant.ofEpochMilli(2000)), decoded);
    }

    @Test
    void malformedJsonUnknownTypeAndMissingCartIdAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> JsonCodec.decodeCartEvent(bytes("not json")));
        assertThrows(IllegalArgumentException.class, () -> JsonCodec.decodeCartEvent(bytes(
                "{\"schemaVersion\":1,\"type\":\"TELEPORTED\",\"cartId\":\"c\",\"version\":1,\"occurredAt\":1}")));
        assertThrows(IllegalArgumentException.class, () -> JsonCodec.decodeCartEvent(bytes(
                "{\"schemaVersion\":1,\"type\":\"EDITED\",\"version\":1,\"occurredAt\":1}")));
        assertThrows(IllegalArgumentException.class, () -> JsonCodec.decodeIntent(bytes("{\"key\":")));
    }
}
