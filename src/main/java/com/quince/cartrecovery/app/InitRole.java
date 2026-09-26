package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.TopicAdmin;
import com.quince.cartrecovery.infra.redis.RedisMeta;
import java.util.Map;

/** One-off: tables, meta item (S, P, Redis identity), topics with equal P, and the Redis epoch. Idempotent. */
public final class InitRole implements Role {
    @Override
    public String name() { return "init"; }

    @Override
    public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        try (RoleContext ctx = new RoleContext(config)) {
            DynamoTables.createAll(ctx.dynamo());   // carts, send-ledger, recovery-meta; idempotent
            RecoveryMetaStore meta = new RecoveryMetaStore(ctx.dynamo());
            meta.init(config.shards(), config.partitions());
            RecoveryMetaStore.Meta stored = meta.read();
            // Check S and P before touching topics: init never recreates topics on a running system.
            RoleContext.startupProblem(config.shards(), config.partitions(), stored.shards(), stored.partitions(), Map.of())
                .filter(problem -> !problem.startsWith("topic "))
                .ifPresent(problem -> { throw new IllegalStateException(problem); });
            new TopicAdmin(ctx.admin()).createAll(config.partitions(), config.replicationFactor(), config.minInsyncReplicas());
            ctx.verifyStartup();
            RedisMeta redis = new RedisMeta(ctx.redis());
            if (!redis.epochPresent()) redis.writeEpoch();
            if (stored.redisRunId() == null) meta.setRedisIdentity(redis.runId(), redis.role());
            System.out.printf("init: shards=%d partitions=%d ready%n", config.shards(), config.partitions());
        }
    }
}
