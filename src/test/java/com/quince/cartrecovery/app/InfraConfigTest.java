package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.DispatchConfig;
import com.quince.cartrecovery.model.RecoveryConfig;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class InfraConfigTest {

    @Test
    void defaultsMatchTheSpec() {
        InfraConfig c = InfraConfig.fromEnv(Map.of());
        assertEquals(RecoveryConfig.defaults(), c.recovery());
        assertEquals(DispatchConfig.defaults(), c.dispatch());
        assertEquals("localhost:9092", c.kafkaBootstrap());
        assertEquals("redis://localhost:6379", c.redisUrl());
        assertNull(c.dynamoEndpoint());
        assertEquals(8, c.shards());
        assertEquals(8, c.partitions());
        assertEquals(1, c.replicationFactor());
        assertEquals(1, c.minInsyncReplicas());
        assertEquals(1000.0, c.maxSendRate());
        assertEquals(0.3, c.fastReserve());
        assertEquals(0.0, c.sendFailureRate());
        assertEquals(Duration.ofMinutes(5), c.reconcileInterval());
        assertEquals(Duration.ofSeconds(1), c.retryPoll());
        assertEquals(256, c.maxInFlight());
        assertEquals(8081, c.healthPort());
    }

    @Test
    void readsEveryVariable() {
        Map<String, String> env = new HashMap<>();
        env.put("WINDOW", "PT30S");
        env.put("OFFSETS", "PT30S, PT60S ,PT120S");
        env.put("LATENESS_BOUNDS", "PT20S,PT20S,PT0S");
        env.put("FREQUENCY_CAP", "0");
        env.put("FREQUENCY_WINDOW", "P1D");
        env.put("HOLDOUT_PERCENT", "0");
        env.put("MAX_SEND_ATTEMPTS", "2");
        env.put("RETRY_BASE", "PT2S");
        env.put("FAST_OFFSETS", "1");
        env.put("KAFKA_BOOTSTRAP", "kafka:9092");
        env.put("REDIS_URL", "redis://redis:6379");
        env.put("DYNAMO_ENDPOINT", "http://dynamodb:8000");
        env.put("SHARDS", "64");
        env.put("PARTITIONS", "16");
        env.put("REPLICATION_FACTOR", "3");
        env.put("MIN_INSYNC_REPLICAS", "2");
        env.put("LEASE", "PT30S");
        env.put("GATEWAY_TIMEOUT", "PT10S");
        env.put("CLOCK_SKEW", "PT2S");
        env.put("MAX_SEND_RATE", "50.5");
        env.put("FAST_RESERVE", "0");
        env.put("SEND_FAILURE_RATE", "0.3");
        env.put("RECONCILE_INTERVAL", "PT30S");
        env.put("RETRY_POLL", "PT0.5S");
        env.put("MAX_IN_FLIGHT", "32");
        env.put("HEALTH_PORT", "9000");

        InfraConfig c = InfraConfig.fromEnv(env);

        assertEquals(new RecoveryConfig(Duration.ofSeconds(30),
                List.of(Duration.ofSeconds(30), Duration.ofSeconds(60), Duration.ofSeconds(120)),
                List.of(Duration.ofSeconds(20), Duration.ofSeconds(20), Duration.ZERO),
                0, Duration.ofDays(1), 0, 2, Duration.ofSeconds(2)), c.recovery());
        assertEquals(new DispatchConfig(Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofSeconds(2), 1),
                c.dispatch());
        assertEquals("kafka:9092", c.kafkaBootstrap());
        assertEquals("redis://redis:6379", c.redisUrl());
        assertEquals("http://dynamodb:8000", c.dynamoEndpoint());
        assertEquals(64, c.shards());
        assertEquals(16, c.partitions());
        assertEquals(3, c.replicationFactor());
        assertEquals(2, c.minInsyncReplicas());
        assertEquals(50.5, c.maxSendRate());
        assertEquals(0.0, c.fastReserve());
        assertEquals(0.3, c.sendFailureRate());
        assertEquals(Duration.ofSeconds(30), c.reconcileInterval());
        assertEquals(Duration.ofMillis(500), c.retryPoll());
        assertEquals(32, c.maxInFlight());
        assertEquals(9000, c.healthPort());
    }

    @Test
    void blankValuesFallBackToDefaults() {
        InfraConfig c = InfraConfig.fromEnv(Map.of("DYNAMO_ENDPOINT", "", "SHARDS", "  "));
        assertNull(c.dynamoEndpoint());
        assertEquals(8, c.shards());
    }

    // Review Focus 5: a malformed value exits with one line naming the variable, not a stack trace.
    @ParameterizedTest(name = "{0}={1}")
    @CsvSource(delimiter = '|', quoteCharacter = '"', value = {
        "WINDOW|30m|WINDOW: expected an ISO-8601 duration such as PT30M, got '30m'",
        "SHARDS|abc|SHARDS: expected an integer, got 'abc'",
        "SHARDS|0|SHARDS: must be at least 1, got 0",
        "PARTITIONS|-1|PARTITIONS: must be at least 1, got -1",
        "WINDOW|PT0S|WINDOW: must be a positive duration, got PT0S",
        "OFFSETS|PT1H,PT30M,PT2H|OFFSETS: offsets must be strictly increasing",
        "OFFSETS|PT10M,PT1H,PT24H|OFFSETS: first offset PT10M must be >= window PT30M",
        "OFFSETS|PT30M,,PT24H|OFFSETS: expected an ISO-8601 duration such as PT30M, got ''",
        "LATENESS_BOUNDS|PT5M|LATENESS_BOUNDS: needs one bound per OFFSETS entry (3), got 1",
        "LATENESS_BOUNDS|PT5M,-PT1M,PT5M|LATENESS_BOUNDS: must not be negative, got PT-1M",
        "LEASE|PT60S|LEASE: must be at least 3 x GATEWAY_TIMEOUT (PT30S), got PT1M",
        "GATEWAY_TIMEOUT|PT31S|LEASE: must be at least 3 x GATEWAY_TIMEOUT (PT31S), got PT1M30S",
        "FAST_RESERVE|1.5|FAST_RESERVE: must be in [0, 1), got 1.5",
        "MAX_SEND_RATE|0|MAX_SEND_RATE: must be positive, got 0.0",
        "SEND_FAILURE_RATE|lots|SEND_FAILURE_RATE: expected a number, got 'lots'",
        "HOLDOUT_PERCENT|101|HOLDOUT_PERCENT: must be at most 100, got 101",
        "MIN_INSYNC_REPLICAS|2|MIN_INSYNC_REPLICAS: must not exceed REPLICATION_FACTOR (1), got 2",
        "RETRY_POLL|1s|RETRY_POLL: expected an ISO-8601 duration such as PT30M, got '1s'",
        "MAX_IN_FLIGHT|0|MAX_IN_FLIGHT: must be at least 1, got 0",
        "HEALTH_PORT|70000|HEALTH_PORT: must be at most 65535, got 70000",
    })
    void rejectsMalformedValuesWithOneLineNamingTheVariable(String name, String value, String message) {
        ConfigException ex = assertThrows(ConfigException.class, () -> InfraConfig.fromEnv(Map.of(name, value)));
        assertEquals(message, ex.getMessage());
        assertFalse(ex.getMessage().contains("\n"));
    }

    @Test
    void hashIsStableAndTracksTheEffectiveConfig() {
        String h = InfraConfig.fromEnv(Map.of()).hash();
        assertTrue(h.matches("[0-9a-f]{12}"), h);
        assertEquals(h, InfraConfig.fromEnv(Map.of("UNRELATED", "x")).hash());
        assertEquals(h, InfraConfig.fromEnv(Map.of("SHARDS", "8")).hash());
        assertNotEquals(h, InfraConfig.fromEnv(Map.of("SHARDS", "16")).hash());
    }
}
