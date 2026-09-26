package com.quince.cartrecovery.infra.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * Redis identity checks for the reconciler (spec §5.2, §6.2): the {@code epoch} sentinel (missing means Redis lost
 * data) and the server {@code run_id} and replication role (a change means a restart or failover).
 */
public final class RedisMeta {
    public static final String EPOCH = "epoch";

    private final RedisCommands<String, String> redis;

    public RedisMeta(StatefulRedisConnection<String, String> connection) {
        this.redis = connection.sync();
    }

    public boolean epochPresent() {
        return redis.exists(EPOCH) == 1L;
    }

    public void writeEpoch() {
        redis.set(EPOCH, "1");
    }

    public String runId() {
        return field(redis.info("server"), "run_id");
    }

    public String role() {
        return field(redis.info("replication"), "role");
    }

    static String field(String info, String name) {
        String prefix = name + ":";
        for (String line : info.split("\r?\n")) {
            if (line.startsWith(prefix)) return line.substring(prefix.length()).trim();
        }
        throw new IllegalStateException("INFO has no field " + name);
    }
}
