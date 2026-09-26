package com.quince.cartrecovery.infra.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** One Redis 7 container and one connection shared by every integration test in the JVM, started on first use. */
public final class TestRedis {
    private static GenericContainer<?> container;
    private static StatefulRedisConnection<String, String> connection;

    private TestRedis() {}

    public static synchronized StatefulRedisConnection<String, String> connection() {
        if (connection == null) {
            container = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);
            container.start();
            connection = RedisClient.create(url()).connect();
        }
        return connection;
    }

    public static synchronized String url() {
        if (container == null) connection();
        return "redis://" + container.getHost() + ":" + container.getMappedPort(6379);
    }

    public static RedisCommands<String, String> sync() { return connection().sync(); }

    public static void flushAll() { sync().flushall(); }

    /** Redis server time, the time source of every script. */
    public static Instant now() {
        List<String> t = sync().time();
        return Instant.ofEpochMilli(Long.parseLong(t.get(0)) * 1000 + Long.parseLong(t.get(1)) / 1000);
    }

    /** Redis time cannot be faked, so advancing time in a contract test means waiting. */
    public static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
