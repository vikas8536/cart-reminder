package com.quince.cartrecovery.infra.dynamo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class RecoveryMetaStoreTest {
    private RecoveryMetaStore meta;

    @BeforeEach
    void setUp() {
        String table = TestDynamo.table("recovery-meta");
        DynamoTables.createMeta(TestDynamo.client(), table);
        meta = new RecoveryMetaStore(TestDynamo.client(), table);
    }

    @Test
    void initWritesShardsAndPartitionsOnceAndNeverOverwrites() {
        IllegalStateException missing = assertThrows(IllegalStateException.class, meta::read);
        assertTrue(missing.getMessage().contains("--role=init"), missing.getMessage());

        meta.init(8, 8);
        assertEquals(new RecoveryMetaStore.Meta(8, 8, false, null, null, null), meta.read());
        meta.init(64, 16);
        assertEquals(new RecoveryMetaStore.Meta(8, 8, false, null, null, null), meta.read(),
                "a second init with other values keeps what is stored; the caller compares and refuses to start");
    }

    @Test
    void pauseSwitchToggles() {
        meta.init(8, 8);
        meta.setPaused(true);
        assertTrue(meta.read().paused());
        meta.setPaused(false);
        assertFalse(meta.read().paused());
    }

    @Test
    void redisChangeKeepsTheEarliestUntilTheIdentityIsStored() {
        meta.init(8, 8);
        Instant first = Instant.parse("2026-01-01T09:00:00Z");
        meta.markRedisChange(first);
        meta.markRedisChange(first.plusSeconds(30));
        assertEquals(first, meta.read().redisChangeAt());

        meta.setRedisIdentity("run-2", "master");
        RecoveryMetaStore.Meta m = meta.read();
        assertEquals("run-2", m.redisRunId());
        assertEquals("master", m.redisRole());
        assertNull(m.redisChangeAt());
    }
}
