package com.quince.cartrecovery.infra.redis;

import com.quince.cartrecovery.contract.WatermarkContract;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Runs thread A's watermark contract against Redis 7 with the production 5 s staleness. */
@Testcontainers(disabledWithoutDocker = true)
class RedisWatermarkContractTest extends WatermarkContract {
    /** The contract writes partitions 0 to 2, so current(-1) is the minimum over exactly those. */
    @Override
    protected Watermark newWatermark() {
        TestRedis.flushAll();
        return new RedisWatermark(TestRedis.connection(), 3);
    }

    @Override
    protected void advance(Duration d) {
        TestRedis.sleep(d);
    }
}
