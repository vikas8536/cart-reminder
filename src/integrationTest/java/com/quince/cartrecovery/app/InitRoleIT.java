package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.TopicAdmin;
import com.quince.cartrecovery.infra.kafka.Topics;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class InitRoleIT {
    @BeforeAll
    static void infra() { RoleInfra.start(); }

    @Test
    void initIsIdempotent() throws Exception {
        InfraConfig c = RoleInfra.config(Map.of());
        new InitRole().run(c, new Health(), new Metrics());
        new InitRole().run(c, new Health(), new Metrics());
        Map<String, Integer> counts = new TopicAdmin(RoleInfra.ctx().admin()).partitionCounts();
        for (String topic : Topics.ALL) assertEquals(8, counts.get(topic), topic);
        RecoveryMetaStore.Meta meta = new RecoveryMetaStore(RoleInfra.ctx().dynamo()).read();
        assertEquals(8, meta.shards());
        assertEquals(8, meta.partitions());
        assertNotNull(meta.redisRunId());
        assertNotNull(RoleInfra.ctx().redis().sync().get(ReconcilerRole.EPOCH_KEY));
    }

    @Test
    void initRefusesToChangePartitions() {
        InfraConfig c = RoleInfra.config(Map.of("PARTITIONS", "4"));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new InitRole().run(c, new Health(), new Metrics()));
        assertTrue(e.getMessage().contains("PARTITIONS=4"), e.getMessage());
    }

    @Test
    void rolesRefuseToStartOnAShardMismatch() {
        InfraConfig c = RoleInfra.config(Map.of("SHARDS", "4"));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new DetectorRole().run(c, new Health(), new Metrics()));
        assertTrue(e.getMessage().contains("SHARDS=4"), e.getMessage());
    }
}
