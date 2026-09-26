package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertNull;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class SchedulerRoleIT {
    static final Duration WAIT = Duration.ofSeconds(25);
    static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));

    @BeforeAll
    static void infra() { RoleInfra.start(); }

    @Test
    void abandonsTheCartAndPublishesTheFirstIntentOnTheFastLane() {
        InfraConfig c = RoleInfra.config(Map.of());
        String cart = RoleInfra.prefix("sched") + "cart";
        String firstKey = new LedgerKey(cart, 1, 0).toString();
        try (TopicTail outcomes = new TopicTail(RoleInfra.bootstrap(), Topics.OUTCOMES);
             TopicTail fast = new TopicTail(RoleInfra.bootstrap(), Topics.INTENTS_FAST);
             RoleThread detector = new RoleThread(new DetectorRole(), c);
             RoleThread scheduler = new RoleThread(new SchedulerRole(), c)) {
            RoleInfra.produce(new CartEvent.CartEdited(cart, "shopper", 1, Instant.now(), ITEMS, "Ada"));
            Await.until(() -> outcomes.records(cart).stream().map(r -> JsonCodec.decodeOutcome(r.value()))
                .anyMatch(o -> o.kind() == OutcomeKind.ABANDONED), WAIT);
            Await.until(() -> fast.records(cart).stream().map(r -> JsonCodec.decodeIntent(r.value()).key())
                .anyMatch(firstKey::equals), WAIT);
            assertNull(scheduler.failure());
            assertNull(detector.failure());
        }
    }
}
