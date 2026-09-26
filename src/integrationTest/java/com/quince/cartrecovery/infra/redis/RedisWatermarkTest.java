package com.quince.cartrecovery.infra.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class RedisWatermarkTest {
    @BeforeEach
    void setUp() {
        TestRedis.flushAll();
    }

    @Test
    void unknownSourceGatesOnTheMinimumOverAllPartitions() {
        RedisWatermark wm = new RedisWatermark(TestRedis.connection(), 3);
        assertEquals(Instant.EPOCH, wm.current(0));
        Instant t = wm.now();
        wm.publish(0, 1, t);
        wm.publish(1, 1, t.plusMillis(5));
        assertEquals(t, wm.current(0));
        assertEquals(t.plusMillis(5), wm.current(1));
        assertEquals(Instant.EPOCH, wm.current(-1), "partition 2 never published");
        wm.publish(2, 1, t.plusMillis(9));
        assertEquals(t, wm.current(-1));
    }

    @Test
    void olderGenerationIsRejectedAndSameGenerationKeepsTheMax() {
        RedisWatermark wm = new RedisWatermark(TestRedis.connection(), 1);
        Instant t = wm.now();
        wm.publish(0, 5, t);
        wm.publish(0, 5, t.minusSeconds(1));
        assertEquals(t, wm.current(0));
        wm.publish(0, 4, t.plusSeconds(1));
        assertEquals(t, wm.current(0));
        wm.publish(0, 6, t.minusSeconds(2));
        assertEquals(t.minusSeconds(2), wm.current(0));
    }

    @Test
    void goesStaleAfterSilence() {
        RedisWatermark wm = new RedisWatermark(TestRedis.connection(), 1, Duration.ofMillis(200));
        Instant t = wm.now();
        wm.publish(0, 1, t);
        assertEquals(t, wm.current(0));
        TestRedis.sleep(Duration.ofMillis(300));
        assertEquals(Instant.EPOCH, wm.current(0));
    }

    @Test
    void nowIsRedisTime() {
        RedisWatermark wm = new RedisWatermark(TestRedis.connection(), 1);
        assertTrue(Duration.between(wm.now(), TestRedis.now()).abs().toMillis() < 1_000);
    }

    /**
     * Controller ruling: a lower-generation publish is rejected outright by wmSet.lua (returns 0 before the
     * HSET), so it must not act as a heartbeat that keeps a stale partition looking fresh.
     */
    @Test
    void aRejectedLowerGenerationPublishDoesNotRefreshStaleness() {
        RedisWatermark wm = new RedisWatermark(TestRedis.connection(), 1, Duration.ofMillis(300));
        Instant t = wm.now();
        wm.publish(0, 5, t);
        assertEquals(t, wm.current(0));

        TestRedis.sleep(Duration.ofMillis(200));
        wm.publish(0, 4, t.plusSeconds(100)); // lower generation: rejected, must not touch updatedAt
        TestRedis.sleep(Duration.ofMillis(150)); // 350 ms since the accepted publish: past the 300 ms staleAfter

        assertEquals(Instant.EPOCH, wm.current(0), "the rejected publish must not have refreshed staleness");
    }

    /**
     * Controller ruling: a same-generation publish with an older eventTime keeps the stored (max) value, but
     * wmSet.lua still writes updatedAt = TIME on that call, so it acts as a heartbeat.
     */
    @Test
    void aSameGenerationOlderEventTimePublishKeepsTheValueButRefreshesTheHeartbeat() {
        RedisWatermark wm = new RedisWatermark(TestRedis.connection(), 1, Duration.ofMillis(300));
        Instant t = wm.now();
        wm.publish(0, 5, t);
        assertEquals(t, wm.current(0));

        TestRedis.sleep(Duration.ofMillis(200));
        wm.publish(0, 5, t.minusSeconds(10)); // same generation, older eventTime: value unchanged, heartbeat refreshed
        assertEquals(t, wm.current(0), "value must stay at the stored maximum");

        TestRedis.sleep(Duration.ofMillis(200)); // 400 ms since the first publish, but only 200 ms since the heartbeat
        assertEquals(t, wm.current(0), "the heartbeat must have refreshed staleness even though the value did not change");
    }

    /**
     * Controller ruling: current(-1) is the minimum over all configured partitions, and a partition that has
     * gone stale (not just one that was never published) counts as Instant.EPOCH there too.
     */
    @Test
    void minusOneCountsAStalePartitionAsEpochEvenWhileAnotherPartitionIsFresh() {
        RedisWatermark wm = new RedisWatermark(TestRedis.connection(), 2, Duration.ofMillis(300));
        Instant t = wm.now();
        wm.publish(0, 1, t);
        wm.publish(1, 1, t.plusMillis(50));
        assertEquals(t, wm.current(-1));

        TestRedis.sleep(Duration.ofMillis(400)); // both partitions go stale
        Instant fresh = t.plusSeconds(5);
        wm.publish(1, 1, fresh); // only partition 1 is heartbeated back to fresh

        assertEquals(Instant.EPOCH, wm.current(0), "partition 0 is stale");
        assertEquals(fresh, wm.current(1), "partition 1 was just republished");
        assertEquals(Instant.EPOCH, wm.current(-1), "the stale partition holds the gate");
    }
}
