package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class ReconcilerRoleIT {
    static final Duration WAIT = Duration.ofSeconds(25);
    static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));

    @BeforeAll
    static void infra() { RoleInfra.start(); }

    @Test
    void missingEpochTriggersAnImmediateSweepThatRebuildsTheTimer() {
        InfraConfig c = RoleInfra.config(Map.of("WINDOW", "PT60S", "OFFSETS", "PT60S,PT120S,PT180S"));
        TimerStore timers = new RedisTimerStore(RoleInfra.ctx().redis(), c.shards(), c.dispatch().lease());
        String cart = RoleInfra.prefix("recon") + "cart";
        int shard = Shards.of(cart, c.shards());
        try (RoleThread detector = new RoleThread(new DetectorRole(), c)) {
            RoleInfra.produce(new CartEvent.CartEdited(cart, "shopper", 1, Instant.now(), ITEMS, "Ada"));
            Await.until(() -> timers.existing(shard, List.of(cart)).contains(cart), WAIT);
        }
        try (RoleThread reconciler = new RoleThread(new ReconcilerRole(), c)) {
            Await.until(() -> reconciler.metrics().get("reconciler.sweeps") >= 1, WAIT);
            timers.remove(cart, 1);
            RoleInfra.ctx().redis().sync().del(ReconcilerRole.EPOCH_KEY);
            assertFalse(timers.existing(shard, List.of(cart)).contains(cart));
            Await.until(() -> timers.existing(shard, List.of(cart)).contains(cart), WAIT);
            assertTrue(reconciler.metrics().get("reconciler.sweeps") >= 2);
            assertNotNull(RoleInfra.ctx().redis().sync().get(ReconcilerRole.EPOCH_KEY));
            assertNull(reconciler.failure());
        }
    }
}
