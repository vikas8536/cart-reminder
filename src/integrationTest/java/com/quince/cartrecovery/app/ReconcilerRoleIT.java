package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.core.Reconciler;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.redis.RedisMeta;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.InMemorySendLedger;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.TimerStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
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

    /**
     * Final review finding 4: the Redis identity is checked every tick, not only when idle, so a change seen while a
     * sweep is in flight is recorded at the time it was observed, and the replay runs once the sweep finishes.
     */
    @Test
    void aRedisChangeDuringAnInFlightSweepIsRecordedWhenObservedAndReplayedAfterTheSweep() throws Exception {
        InfraConfig c = RoleInfra.config(Map.of());
        RedisMeta redis = new RedisMeta(RoleInfra.ctx().redis());
        RecoveryMetaStore meta = new RecoveryMetaStore(RoleInfra.ctx().dynamo());
        meta.setRedisIdentity(redis.runId(), redis.role());
        TimerStore timers = new RedisTimerStore(RoleInfra.ctx().redis(), c.shards(), c.dispatch().lease());
        CountDownLatch sweeping = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CartStateStore real = new InMemoryCartStateStore(c.recovery(), c.shards());
        CartStateStore blocking = (CartStateStore) Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[] {CartStateStore.class}, (proxy, method, args) -> {
                if (method.getName().equals("openCartIds")) {
                    sweeping.countDown();
                    release.await();
                    return Stream.empty();
                }
                try {
                    return method.invoke(real, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
        Metrics metrics = new Metrics();
        Reconciler reconciler = new Reconciler(c.recovery(), blocking, timers,
            new InMemorySendLedger(c.dispatch().lease(), c.shards()), Instant::now, metrics);
        ReconcilerRole.Cycle cycle = new ReconcilerRole.Cycle(c, redis, timers, reconciler, meta, new Health(), metrics);
        AtomicBoolean running = new AtomicBoolean(true);
        Thread loop = Thread.ofPlatform().start(() -> cycle.loop(running));
        try {
            assertTrue(sweeping.await(10, TimeUnit.SECONDS), "the start-up sweep is in flight");
            Instant observedFrom = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            meta.setRedisIdentity("previous-run", "master");   // as if Redis restarted mid-sweep

            Await.until(() -> meta.read().redisChangeAt() != null, WAIT);
            Instant changeAt = meta.read().redisChangeAt();
            assertFalse(changeAt.isBefore(observedFrom), "recorded no earlier than the change");
            assertTrue(changeAt.isBefore(observedFrom.plusSeconds(3)), "recorded within a tick or two, not after the sweep");
            assertEquals(0, metrics.get("reconciler.replays"), "the replay waits for the in-flight sweep");

            release.countDown();
            Await.until(() -> metrics.get("reconciler.replays") >= 1, WAIT);
            assertEquals(redis.runId(), meta.read().redisRunId());
            assertNull(meta.read().redisChangeAt());
        } finally {
            release.countDown();
            running.set(false);
            loop.join(5_000);
        }
    }
}
