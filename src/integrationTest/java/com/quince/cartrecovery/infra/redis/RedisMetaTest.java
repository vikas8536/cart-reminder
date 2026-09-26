package com.quince.cartrecovery.infra.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class RedisMetaTest {
    @Test
    void epochSentinelIsMissingAfterFlushAndPresentAfterWrite() {
        TestRedis.flushAll();
        RedisMeta meta = new RedisMeta(TestRedis.connection());
        assertFalse(meta.epochPresent());
        meta.writeEpoch();
        assertTrue(meta.epochPresent());
    }

    @Test
    void reportsRunIdAndRole() {
        RedisMeta meta = new RedisMeta(TestRedis.connection());
        assertEquals(40, meta.runId().length());
        assertEquals("master", meta.role());
    }
}
