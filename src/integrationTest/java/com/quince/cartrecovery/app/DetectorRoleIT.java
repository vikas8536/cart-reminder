package com.quince.cartrecovery.app;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.infra.dynamo.DynamoCartStateStore;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.TimerStore;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class DetectorRoleIT {
    static final Duration WAIT = Duration.ofSeconds(25);
    static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));

    @BeforeAll
    static void infra() { RoleInfra.start(); }

    @Test
    void writesTimerAndCartWithTheRecordedPartitionAndKeepsIdlePartitionsCurrent() {
        InfraConfig c = RoleInfra.config(Map.of("WINDOW", "PT60S", "OFFSETS", "PT60S,PT120S,PT180S"));
        String cart = RoleInfra.prefix("det") + "cart";
        DynamoCartStateStore carts = new DynamoCartStateStore(RoleInfra.ctx().dynamo(), DynamoTables.CARTS, c.recovery(), c.shards());
        TimerStore timers = new RedisTimerStore(RoleInfra.ctx().redis(), c.shards(), c.dispatch().lease());
        Watermark wm = new RedisWatermark(RoleInfra.ctx().redis(), c.partitions());
        try (RoleThread detector = new RoleThread(new DetectorRole(), c)) {
            RecordMetadata sent = RoleInfra.produce(new CartEvent.CartEdited(cart, "shopper", 1, Instant.now(), ITEMS, "Ada"));
            Await.until(() -> carts.get(cart).isPresent(), WAIT);
            CartRecord record = carts.get(cart).orElseThrow();
            assertEquals(CartStatus.ACTIVE, record.status());
            assertEquals(sent.partition(), record.srcPartition());
            assertTrue(timers.existing(Shards.of(cart, c.shards()), List.of(cart)).contains(cart));
            Await.until(() -> IntStream.range(0, c.partitions()).allMatch(p ->
                Duration.between(wm.current(p), wm.now()).compareTo(Duration.ofSeconds(3)) < 0), WAIT);
            assertNull(detector.failure());
        }
    }

    @Test
    void undecodableEventGoesToTheDeadLetterTopic() {
        String key = RoleInfra.prefix("det") + "poison";
        try (TopicTail dlq = new TopicTail(RoleInfra.bootstrap(), Topics.CART_EVENTS_DLQ);
             RoleThread detector = new RoleThread(new DetectorRole(), RoleInfra.config(Map.of()))) {
            RoleInfra.send(Topics.CART_EVENTS, key, "not json".getBytes(UTF_8));
            Await.until(() -> !dlq.records(key).isEmpty(), WAIT);
            assertNull(detector.failure());
        }
    }
}
