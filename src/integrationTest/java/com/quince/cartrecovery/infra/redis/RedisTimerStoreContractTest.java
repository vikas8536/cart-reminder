package com.quince.cartrecovery.infra.redis;

import com.quince.cartrecovery.contract.TimerStoreContract;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Duration;
import java.time.Instant;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Runs thread A's timer store contract against Redis 7. Time is Redis TIME, so advancing time sleeps. */
@Testcontainers(disabledWithoutDocker = true)
class RedisTimerStoreContractTest extends TimerStoreContract {
    @Override
    protected TimerStore newStore(Duration lease) {
        TestRedis.flushAll();
        return new RedisTimerStore(TestRedis.connection(), 8, lease);
    }

    @Override
    protected Instant now() {
        return TestRedis.now();
    }

    @Override
    protected void advance(Duration d) {
        TestRedis.sleep(d);
    }
}
