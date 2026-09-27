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
        meta.setRedisIdentity("run-1", "master");
        Instant first = Instant.parse("2026-01-01T09:00:00Z");
        assertTrue(meta.markRedisChange(first, "run-1", "master"));
        assertTrue(meta.markRedisChange(first.plusSeconds(30), "run-1", "master"));
        assertEquals(first, meta.read().redisChangeAt());

        meta.setRedisIdentity("run-2", "master");
        RecoveryMetaStore.Meta m = meta.read();
        assertEquals("run-2", m.redisRunId());
        assertEquals("master", m.redisRole());
        assertNull(m.redisChangeAt());
    }

    @Test
    void aChangeMarkedAgainstAnIdentityAlreadyReplacedIsANoOp() {
        // The reconciler tick reads run-1, the replay worker stores run-2 (clearing the change), then the tick's
        // mark lands: it must not leave a stale redisChangeAt behind for the next failover to replay from.
        meta.init(8, 8);
        meta.setRedisIdentity("run-2", "master");
        assertFalse(meta.markRedisChange(Instant.parse("2026-01-01T09:00:00Z"), "run-1", "master"));
        assertNull(meta.read().redisChangeAt());
    }
}
