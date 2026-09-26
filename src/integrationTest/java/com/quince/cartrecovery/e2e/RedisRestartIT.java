package com.quince.cartrecovery.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.app.DetectorRole;
import com.quince.cartrecovery.app.InfraConfig;
import com.quince.cartrecovery.app.ReconcilerRole;
import com.quince.cartrecovery.app.RoleContext;
import com.quince.cartrecovery.app.RoleInfra;
import com.quince.cartrecovery.app.RoleThread;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.redis.RedisMeta;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.TimerStore;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class RedisRestartIT {
    static final Duration WAIT = Duration.ofSeconds(25);
    static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));
    static int port;
    static GenericContainer<?> redis;

    @BeforeAll
    static void infra() throws IOException {
        RoleInfra.start();
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        redis = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withCommand("redis-server", "--appendonly", "yes")
            .withExposedPorts(6379)
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(6379))));
        redis.start();
    }

    /**
     * The reconciler stored this private Redis's identity in the shared recovery-meta; put back the shared Redis's,
     * or the next reconciler in this JVM would see a change and run a spurious failover replay.
     */
    @AfterAll
    static void stopRedis() {
        try {
            RedisMeta shared = new RedisMeta(RoleInfra.ctx().redis());
            new RecoveryMetaStore(RoleInfra.ctx().dynamo()).setRedisIdentity(shared.runId(), shared.role());
        } finally {
            redis.stop();
        }
    }

    @Test
    void failoverReplayRestoresADroppedDetectorUpsert() {
        InfraConfig c = RoleInfra.config(Map.of(
            "REDIS_URL", "redis://" + redis.getHost() + ":" + port,
            "WINDOW", "PT60S", "OFFSETS", "PT60S,PT120S,PT180S", "RECONCILE_INTERVAL", "PT10M"));
        RecoveryMetaStore meta = new RecoveryMetaStore(RoleInfra.ctx().dynamo());
        try (RoleContext own = new RoleContext(c);
             RoleThread reconciler = new RoleThread(new ReconcilerRole(), c)) {
            RedisCommands<String, String> cmd = own.redis().sync();
            RedisMeta redisMeta = new RedisMeta(own.redis());
            String runIdBefore = redisMeta.runId();
            Await.until(() -> reconciler.metrics().get("reconciler.sweeps") >= 1
                && runIdBefore.equals(meta.read().redisRunId()), WAIT);

            TimerStore timers = new RedisTimerStore(own.redis(), c.shards(), c.dispatch().lease());
            String cart = RoleInfra.prefix("restart") + "cart";
            int shard = Shards.of(cart, c.shards());
            try (RoleThread detector = new RoleThread(new DetectorRole(), c)) {
                RoleInfra.produce(new CartEvent.CartEdited(cart, "shopper", 1, Instant.now(), ITEMS, "Ada"));
                Await.until(() -> timers.existing(shard, List.of(cart)).contains(cart), WAIT);
            }
            timers.remove(cart, 1);   // the detector upsert that the restart "dropped"
            assertFalse(timers.existing(shard, List.of(cart)).contains(cart));
            long sweeps = reconciler.metrics().get("reconciler.sweeps");
            long replays = reconciler.metrics().get("reconciler.replays");

            DockerClientFactory.instance().client().restartContainerCmd(redis.getContainerId()).exec();

            Await.until(() -> reconciler.metrics().get("reconciler.replays") > replays, WAIT);
            assertTrue(timers.existing(shard, List.of(cart)).contains(cart), "failover replay restores the timer");
            assertEquals(sweeps, reconciler.metrics().get("reconciler.sweeps"), "no key sweep ran: the epoch survived via AOF");
            assertNotNull(cmd.get(ReconcilerRole.EPOCH_KEY));
            assertFalse(runIdBefore.equals(redisMeta.runId()), "run_id changed on restart");
            assertNull(reconciler.failure());
        }
    }
}
