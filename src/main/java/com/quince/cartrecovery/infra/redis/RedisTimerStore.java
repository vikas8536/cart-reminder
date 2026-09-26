package com.quince.cartrecovery.infra.redis;

import static java.util.stream.Collectors.groupingBy;

import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerKind;
import com.quince.cartrecovery.ports.TimerStore;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Timers in Redis (spec §5.2): per shard a sorted set {@code timers:{s}} (member cartId, score due or lease expiry)
 * and a hash {@code timerdata:{s}} (cartId -> kind|version|offsetIndex|srcPartition|dueAtMillis). The cart id is
 * never inside the packed value, so it may contain ':' or '|'. All time comes from Redis TIME inside the scripts.
 */
public final class RedisTimerStore implements TimerStore {
    private static final int EXISTING_CHUNK = 500;

    private final RedisCommands<String, String> sync;
    private final RedisAsyncCommands<String, String> async;
    private final RedisScripts scripts;
    private final int shards;
    private final long leaseMs;
    private final AtomicInteger nextShard = new AtomicInteger();

    public RedisTimerStore(StatefulRedisConnection<String, String> connection, int shards, Duration lease) {
        this.sync = connection.sync();
        this.async = connection.async();
        this.scripts = new RedisScripts(sync, "upsert", "claim", "release", "ack", "remove");
        this.shards = shards;
        this.leaseMs = lease.toMillis();
    }

    static String timersKey(int shard) { return "timers:{" + shard + "}"; }

    static String dataKey(int shard) { return "timerdata:{" + shard + "}"; }

    public static String pack(Timer t) {
        return t.kind().name() + "|" + t.version() + "|" + t.offsetIndex() + "|" + t.srcPartition() + "|" + t.dueAt().toEpochMilli();
    }

    public static Timer unpack(String cartId, String packed) {
        String[] f = packed.split("\\|", -1);
        return new Timer(cartId, TimerKind.valueOf(f[0]), Long.parseLong(f[1]), Integer.parseInt(f[2]),
                Instant.ofEpochMilli(Long.parseLong(f[4])), Integer.parseInt(f[3]));
    }

    private String[] keys(String cartId) {
        int s = Shards.of(cartId, shards);
        return new String[] {timersKey(s), dataKey(s)};
    }

    @Override
    public boolean upsert(Timer t) {
        if (t.version() < 0) throw new IllegalArgumentException("timer version must not be negative: " + t.version());
        Long written = scripts.run("upsert", ScriptOutputType.INTEGER, keys(t.cartId()), t.cartId(), pack(t),
                Long.toString(t.version()), Integer.toString(t.offsetIndex()), Long.toString(t.dueAt().toEpochMilli()));
        return written == 1L;
    }

    @Override
    public void remove(String cartId, long version) {
        scripts.run("remove", ScriptOutputType.INTEGER, keys(cartId), cartId, Long.toString(version));
    }

    /**
     * Walks shards from a rotating start so no shard starves under load, but a claim spans every shard: the
     * merged result must still come back in due order (controller ruling), so the per-shard batches (each
     * already ascending, since claim.lua does ZRANGEBYSCORE lowest-first) are merged by dueAt before returning.
     */
    @Override
    public List<Timer> claimDue(int limit) {
        List<Timer> out = new ArrayList<>();
        int start = Math.floorMod(nextShard.getAndIncrement(), shards);
        for (int i = 0; i < shards && out.size() < limit; i++) {
            int s = (start + i) % shards;
            List<Object> r = scripts.run("claim", ScriptOutputType.MULTI, new String[] {timersKey(s), dataKey(s)},
                    Integer.toString(limit - out.size()), Long.toString(leaseMs));
            for (int j = 0; j < r.size(); j += 2) out.add(unpack((String) r.get(j), (String) r.get(j + 1)));
        }
        out.sort(Comparator.comparing(Timer::dueAt));
        return out;
    }

    @Override
    public void release(Timer timer, Duration delay) {
        scripts.run("release", ScriptOutputType.INTEGER, keys(timer.cartId()), timer.cartId(), pack(timer),
                Long.toString(delay.toMillis()));
    }

    @Override
    public void ack(Timer timer) {
        scripts.run("ack", ScriptOutputType.INTEGER, keys(timer.cartId()), timer.cartId(), pack(timer));
    }

    /** Pipelined HMGETs, one per shard chunk. Each id is looked up in its own shard, so {@code shard} is only a hint. */
    @Override
    public Set<String> existing(int shard, Collection<String> cartIds) {
        Map<Integer, List<String>> byShard = cartIds.stream().distinct().collect(groupingBy(id -> Shards.of(id, shards)));
        List<RedisFuture<List<KeyValue<String, String>>>> futures = new ArrayList<>();
        byShard.forEach((s, ids) -> {
            for (int i = 0; i < ids.size(); i += EXISTING_CHUNK) {
                futures.add(async.hmget(dataKey(s), ids.subList(i, Math.min(ids.size(), i + EXISTING_CHUNK)).toArray(String[]::new)));
            }
        });
        Set<String> found = new HashSet<>();
        for (RedisFuture<List<KeyValue<String, String>>> f : futures) {
            for (KeyValue<String, String> kv : f.toCompletableFuture().join()) {
                if (kv.hasValue()) found.add(kv.getKey());
            }
        }
        return found;
    }

    /** Sum over shards of timers due at or before Redis TIME right now (load test sampling, spec §8.5). */
    public long pastDue() {
        long nowMs = redisTime(sync).toEpochMilli();
        long total = 0;
        for (int s = 0; s < shards; s++) {
            Long count = sync.zcount(timersKey(s), Range.create(Double.NEGATIVE_INFINITY, (double) nowMs));
            total += count == null ? 0 : count;
        }
        return total;
    }

    static Instant redisTime(RedisCommands<String, String> redis) {
        List<String> t = redis.time();
        return Instant.ofEpochMilli(Long.parseLong(t.get(0)) * 1000 + Long.parseLong(t.get(1)) / 1000);
    }
}
