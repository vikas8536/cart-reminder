package com.quince.cartrecovery.infra.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.ReminderIntent;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * JSON wire format for every Kafka payload (spec §5.1). Model records stay free of Jackson: private wire records
 * carry epoch-millisecond times and {@code schemaVersion}; unknown fields are ignored so writers can add fields first.
 */
public final class JsonCodec {
    public static final int SCHEMA_VERSION = 1;

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private JsonCodec() {}

    /** One record per delivered send at the recording sink. No personal data. */
    public record SinkSend(String key, String cartId, Instant at, boolean hasFirstName, int itemCount) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record EventJson(int schemaVersion, String type, String cartId, String shopperKey, String firstName,
                     long version, long occurredAt, List<CartItem> items) {}

    record IntentJson(int schemaVersion, String key, String cartId, long version, int offsetIndex, int srcPartition,
                      long scheduledFor, long sendBy) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record OutcomeJson(int schemaVersion, String key, String cartId, long version, Arm arm, OutcomeKind kind,
                       long at, int attempts) {}

    record DeadLetterJson(int schemaVersion, String key, String cartId, long version, int offsetIndex, int srcPartition,
                          long scheduledFor, long sendBy, String reason, long failedAt) {}

    record SinkSendJson(int schemaVersion, String key, String cartId, long at, boolean hasFirstName, int itemCount) {}

    public static byte[] encode(CartEvent e) {
        String type = switch (e) {
            case CartEvent.CartEdited x -> "EDITED";
            case CartEvent.CartResumed x -> "RESUMED";
            case CartEvent.CartCleared x -> "CLEARED";
            case CartEvent.CartPurchased x -> "PURCHASED";
        };
        List<CartItem> items = e instanceof CartEvent.CartEdited edit ? edit.items() : List.of();
        String firstName = e instanceof CartEvent.CartEdited edit ? edit.firstName() : null;
        return write(new EventJson(SCHEMA_VERSION, type, e.cartId(), e.shopperKey(), firstName, e.version(),
                e.occurredAt().toEpochMilli(), items));
    }

    public static CartEvent decodeCartEvent(byte[] bytes) {
        EventJson j = read(bytes, EventJson.class);
        require(j.cartId() != null && j.type() != null, "cart event needs cartId and type");
        Instant at = Instant.ofEpochMilli(j.occurredAt());
        return switch (j.type()) {
            case "EDITED" -> new CartEvent.CartEdited(j.cartId(), j.shopperKey(), j.version(), at,
                    j.items() == null ? List.of() : j.items(), j.firstName());
            case "RESUMED" -> new CartEvent.CartResumed(j.cartId(), j.shopperKey(), j.version(), at);
            case "CLEARED" -> new CartEvent.CartCleared(j.cartId(), j.shopperKey(), j.version(), at);
            case "PURCHASED" -> new CartEvent.CartPurchased(j.cartId(), j.shopperKey(), j.version(), at);
            default -> throw new IllegalArgumentException("unknown cart event type " + j.type());
        };
    }

    public static byte[] encode(ReminderIntent i) {
        return write(new IntentJson(SCHEMA_VERSION, i.key(), i.cartId(), i.version(), i.offsetIndex(), i.srcPartition(),
                i.scheduledFor().toEpochMilli(), i.sendBy().toEpochMilli()));
    }

    public static ReminderIntent decodeIntent(byte[] bytes) {
        IntentJson j = read(bytes, IntentJson.class);
        require(j.key() != null && j.cartId() != null, "intent needs key and cartId");
        return new ReminderIntent(j.key(), j.cartId(), j.version(), j.offsetIndex(), j.srcPartition(),
                Instant.ofEpochMilli(j.scheduledFor()), Instant.ofEpochMilli(j.sendBy()));
    }

    public static byte[] encode(Outcome o) {
        return write(new OutcomeJson(SCHEMA_VERSION, o.key(), o.cartId(), o.version(), o.arm(), o.kind(),
                o.at().toEpochMilli(), o.attempts()));
    }

    public static Outcome decodeOutcome(byte[] bytes) {
        OutcomeJson j = read(bytes, OutcomeJson.class);
        require(j.cartId() != null && j.kind() != null, "outcome needs cartId and kind");
        return new Outcome(j.key(), j.cartId(), j.version(), j.arm(), j.kind(), Instant.ofEpochMilli(j.at()), j.attempts());
    }

    public static byte[] encode(DeadLetter d) {
        ReminderIntent i = d.intent();
        return write(new DeadLetterJson(SCHEMA_VERSION, i.key(), i.cartId(), i.version(), i.offsetIndex(), i.srcPartition(),
                i.scheduledFor().toEpochMilli(), i.sendBy().toEpochMilli(), d.reason(), d.at().toEpochMilli()));
    }

    public static DeadLetter decodeDeadLetter(byte[] bytes) {
        DeadLetterJson j = read(bytes, DeadLetterJson.class);
        require(j.key() != null && j.cartId() != null, "dead letter needs key and cartId");
        ReminderIntent intent = new ReminderIntent(j.key(), j.cartId(), j.version(), j.offsetIndex(), j.srcPartition(),
                Instant.ofEpochMilli(j.scheduledFor()), Instant.ofEpochMilli(j.sendBy()));
        return new DeadLetter(intent, j.reason(), Instant.ofEpochMilli(j.failedAt()));
    }

    public static byte[] encode(SinkSend s) {
        return write(new SinkSendJson(SCHEMA_VERSION, s.key(), s.cartId(), s.at().toEpochMilli(), s.hasFirstName(), s.itemCount()));
    }

    public static SinkSend decodeSinkSend(byte[] bytes) {
        SinkSendJson j = read(bytes, SinkSendJson.class);
        require(j.key() != null && j.cartId() != null, "sink send needs key and cartId");
        return new SinkSend(j.key(), j.cartId(), Instant.ofEpochMilli(j.at()), j.hasFirstName(), j.itemCount());
    }

    private static byte[] write(Object value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot encode " + value.getClass().getSimpleName(), e);
        }
    }

    private static <T> T read(byte[] bytes, Class<T> type) {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (IOException e) {
            throw new IllegalArgumentException("malformed " + type.getSimpleName() + ": " + e.getMessage(), e);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }
}
