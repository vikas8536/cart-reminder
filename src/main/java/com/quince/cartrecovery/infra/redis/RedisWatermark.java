package com.quince.cartrecovery.infra.redis;

import com.quince.cartrecovery.ports.Watermark;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Per-partition watermarks in the {@code watermarks} hash (spec §5.2, §5.4): generation-fenced writes, max within a
 * generation, and a read that treats a missing entry or one silent for {@code staleAfter} as {@link Instant#EPOCH}.
 */
public final class RedisWatermark implements Watermark {
    static final String KEY = "watermarks";

    private final RedisCommands<String, String> redis;
    private final RedisScripts scripts;
    private final int partitions;
    private final long staleAfterMs;

    public RedisWatermark(StatefulRedisConnection<String, String> connection, int partitions) {
        this(connection, partitions, Duration.ofSeconds(5));
    }

    public RedisWatermark(StatefulRedisConnection<String, String> connection, int partitions, Duration staleAfter) {
        this.redis = connection.sync();
        this.scripts = new RedisScripts(redis, "wmSet", "wmGet");
        this.partitions = partitions;
        this.staleAfterMs = staleAfter.toMillis();
    }

    @Override
    public void publish(int partition, long generation, Instant eventTime) {
        scripts.run("wmSet", ScriptOutputType.INTEGER, new String[] {KEY},
                Integer.toString(partition), Long.toString(generation), Long.toString(eventTime.toEpochMilli()));
    }

    /** srcPartition -1 (a cart record written before the field existed, or "gate on everything") is the minimum over partitions 0..partitions-1. */
    @Override
    public Instant current(int srcPartition) {
        List<String> args = new ArrayList<>();
        args.add(Long.toString(staleAfterMs));
        if (srcPartition >= 0) {
            args.add(Integer.toString(srcPartition));
        } else {
            for (int p = 0; p < partitions; p++) args.add(Integer.toString(p));
        }
        List<Object> r = scripts.run("wmGet", ScriptOutputType.MULTI, new String[] {KEY}, args.toArray(String[]::new));
        return Instant.ofEpochMilli(Long.parseLong((String) r.get(0)));
    }

    @Override
    public Instant now() {
        return RedisTimerStore.redisTime(redis);
    }
}
