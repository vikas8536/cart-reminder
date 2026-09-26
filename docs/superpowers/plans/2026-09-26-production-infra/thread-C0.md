# Thread C0: runtime building blocks (C0a, C0b, C0c)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the runtime pieces every infra role stands on: environment configuration with one-line validation errors, health endpoints, the shared send budget, the circuit breaker, the batch consumer loop, and the Docker packaging, so thread C1 only wires roles.

**Master plan:** `docs/superpowers/plans/2026-09-26-production-infra.md` (frozen contracts §1, especially §1.5; tasks §2; protocol §3).
**Spec (binding):** `docs/superpowers/specs/2026-09-25-production-infra-design.md`, §5.1 (consumer settings), §6.3 (concurrency and failure handling), §7.1 to §7.4 (configuration, packaging, compose, health).

**Binding note:** Every public type and member named in master §1.5 is created here with exactly that name and signature. A task may add private (or package-private, for tests) helpers, constructors, and constants, but may not add, rename, or change a public member. If a contract looks wrong, stop and report `BLOCKED` (see "Contract issues" below for the ones already found).

## Tasks and file ownership

| Task | Scope | Owns (creates) |
|---|---|---|
| **C0a** | `InfraConfig` + `ConfigException`, `Health` + `HealthServer`, `Role`, `TokenBucket`, `CircuitBreaker`, unit tests | `src/main/java/com/quince/cartrecovery/app/{ConfigException,InfraConfig,Health,HealthServer,Role,TokenBucket,CircuitBreaker}.java`; `src/test/java/com/quince/cartrecovery/app/{InfraConfigTest,HealthServerTest,TokenBucketTest,CircuitBreakerTest}.java` |
| **C0b** | `BatchConsumerLoop` + `PoisonException`, Testcontainers Kafka tests | `src/main/java/com/quince/cartrecovery/app/{BatchConsumerLoop,PoisonException}.java`; `src/integrationTest/java/com/quince/cartrecovery/app/BatchConsumerLoopTest.java` |
| **C0c** | `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `demo.env`, demo.env validity test | `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `demo.env`, `src/test/java/com/quince/cartrecovery/app/DemoEnvTest.java` |

No task in this thread modifies `build.gradle.kts` (owned by T0), `Main.java` (A4, then C1c), or any file outside the table.

**Dependencies (controller ruling R1):** C0a depends on T0 and A1 (it uses `DispatchConfig`, `SendBudget`, `Lane`, `NotificationSink`, `ReminderMessage`, thread-safe `Metrics`); C0b and C0c depend on C0a (C0b takes `Health`; C0c's `DemoEnvTest` uses `InfraConfig`). C0b and C0c touch disjoint files and run in parallel.

## Dependencies and images

T0's pinned set applies (controller ruling R2): `kafka-clients` 4.3.1 (`CloseOptions` is available from 4.1), Testcontainers BOM 1.21.2 (`org.testcontainers:kafka`, `org.testcontainers:junit-jupiter`: `org.testcontainers.kafka.KafkaContainer`, `org.testcontainers.junit.jupiter.{Testcontainers,Container}`), JUnit BOM 5.10.2 (the `junit-jupiter` aggregate includes `junit-jupiter-params`). Images, one tag each for tests and compose (master Global Constraints): `apache/kafka:4.3.1` (has `/opt/kafka/bin/kafka-broker-api-versions.sh`), `redis:7.4-alpine`, `amazon/dynamodb-local:3.3.1` (runs as user `dynamodblocal`, entrypoint `java`, no data dir in the image); build stages `gradle:8.10.2-jdk21` (runs as root) and `eclipse-temurin:21-jre` (has `/usr/bin/wget`).

Integration tests rely on T0's `integrationTest` source set (sees `main` and `test` output, JUnit Platform, `-Djdk.tracePinnedThreads=full`) and T0's `com.quince.cartrecovery.Await.until(BooleanSupplier, Duration)`.

## Contract issues

All resolved by controller rulings (master plan):

1. Dependencies: controller ruling R1 (C0a depends on A1; C0b and C0c on C0a).
2. `Hooks` receive a `Consumer<String, V>` view of the byte-valued consumer (the loop deserializes per record so undeserializable values become poison with their original bytes). No signature change; hooks use only offset, assignment and metadata methods, never `poll`. `afterCommit` also runs once per iteration that committed nothing, with an empty map, so the detector's hooks refresh idle partitions every poll loop (spec §5.4; master §1.5).
3. `-inMemory` for load runs: controller ruling R14 (`DYNAMO_STORAGE=-inMemory`, a deliberate deviation from the spec §7.3 wording).
4. `MAX_SEND_RATE` default 1000 and `HEALTH_PORT` default 8081: controller ruling R14.
5. `CircuitBreaker` open behaviour as defined in C0a (`send` fails fast with `TRANSIENT_FAILURE`; `isOpen()` false once `openFor` elapsed so consumers resume and supply the single probe): controller ruling R14.
6. Waiting in tests: controller ruling R16 (`BatchConsumerLoopTest` uses T0's `Await.until`).

---

### Task C0a: InfraConfig, Health, HealthServer, Role, TokenBucket, CircuitBreaker

**Model:** sonnet (master §2).

**Files:**
- Create: `src/main/java/com/quince/cartrecovery/app/ConfigException.java`
- Create: `src/main/java/com/quince/cartrecovery/app/InfraConfig.java`
- Create: `src/main/java/com/quince/cartrecovery/app/Health.java`
- Create: `src/main/java/com/quince/cartrecovery/app/HealthServer.java`
- Create: `src/main/java/com/quince/cartrecovery/app/Role.java`
- Create: `src/main/java/com/quince/cartrecovery/app/TokenBucket.java`
- Create: `src/main/java/com/quince/cartrecovery/app/CircuitBreaker.java`
- Test: `src/test/java/com/quince/cartrecovery/app/InfraConfigTest.java`
- Test: `src/test/java/com/quince/cartrecovery/app/HealthServerTest.java`
- Test: `src/test/java/com/quince/cartrecovery/app/TokenBucketTest.java`
- Test: `src/test/java/com/quince/cartrecovery/app/CircuitBreakerTest.java`

**Interfaces:**
- Consumes (A1, master §1.1 and §1.2): `record RecoveryConfig(...)` with `static defaults()` (existing, unchanged); `record DispatchConfig(Duration lease, Duration gatewayTimeout, Duration clockSkew, int fastOffsets)` with `static defaults()`; `enum Lane { FAST, SLOW }`; `interface SendBudget { boolean tryAcquire(Lane lane); }`; `interface NotificationSink { SendResult send(ReminderMessage message); }`; `record ReminderMessage(String key, String cartId, String shopperKey, String firstName, List<CartItem> items)`; `enum SendResult { SENT, TRANSIENT_FAILURE, PERMANENT_FAILURE }`; `interface Clock { Instant now(); }`; `Metrics` (`increment`, `get`, `snapshot()` returning `Map<String, Long>`); `FakeClock(Instant)` with `advance(Duration)` (tests).
- Produces (master §1.5, exact):
  - `record InfraConfig(RecoveryConfig recovery, DispatchConfig dispatch, String kafkaBootstrap, String redisUrl, String dynamoEndpoint, int shards, int partitions, int replicationFactor, int minInsyncReplicas, double maxSendRate, double fastReserve, double sendFailureRate, Duration reconcileInterval, Duration retryPoll, int maxInFlight, int healthPort)` with `static InfraConfig fromEnv(Map<String, String> env)` and `String hash()` (12 lowercase hex chars).
  - `final class ConfigException extends RuntimeException { ConfigException(String message); }` — message is one line, starting `<VARIABLE>: `.
  - `final class Health { Health(); void beat(String loop); void setReady(String key, String value); boolean healthy(Duration maxSilence); }` (+ package-private `Health(LongSupplier nanoTime)`, `List<String> stale(Duration)`, `Map<String, String> readiness()`).
  - `final class HealthServer implements AutoCloseable { HealthServer(int port, Health health, Metrics metrics, Duration maxSilence); void close(); }` — `/health` 200 `ok` or 503 `stale: a,b`; `/ready` lines `key: value`; `/metrics` lines `name value`.
  - `interface Role { String name(); void run(InfraConfig config, Health health, Metrics metrics) throws Exception; }`
  - `final class TokenBucket implements SendBudget { TokenBucket(double ratePerSecond, double fastReserve, LongSupplier nanoTime); boolean tryAcquire(Lane lane); boolean slowAllowed(); boolean anyAvailable(); }`
  - `final class CircuitBreaker implements NotificationSink { CircuitBreaker(NotificationSink delegate, int window, double threshold, Duration openFor, Clock clock); SendResult send(ReminderMessage m); boolean isOpen(); }`

#### Part 1: InfraConfig and ConfigException

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/quince/cartrecovery/app/InfraConfigTest.java`:

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.InfraConfigTest' --console=plain`
Expected: FAIL at `:compileTestJava` with `error: cannot find symbol ... class InfraConfig` and `class ConfigException`.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/com/quince/cartrecovery/app/ConfigException.java`:

```java
package com.quince.cartrecovery.app;

/** An invalid environment value. The message is one line and starts with the variable name. */
public final class ConfigException extends RuntimeException {
    public ConfigException(String message) {
        super(message);
    }
}
```

Create `src/main/java/com/quince/cartrecovery/app/InfraConfig.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.model.DispatchConfig;
import com.quince.cartrecovery.model.RecoveryConfig;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** Effective runtime configuration, read once from the environment (spec §7.1). */
public record InfraConfig(RecoveryConfig recovery, DispatchConfig dispatch,
                          String kafkaBootstrap, String redisUrl, String dynamoEndpoint,
                          int shards, int partitions, int replicationFactor, int minInsyncReplicas,
                          double maxSendRate, double fastReserve, double sendFailureRate,
                          Duration reconcileInterval, Duration retryPoll, int maxInFlight, int healthPort) {

    /** Parses and validates every §7.1 variable; unset or blank values take the defaults. */
    public static InfraConfig fromEnv(Map<String, String> env) {
        Env e = new Env(env);

        Duration window = e.duration("WINDOW", "PT30M");
        List<Duration> offsets = e.durations("OFFSETS", "PT30M,PT1H,PT24H", false);
        List<Duration> bounds = e.durations("LATENESS_BOUNDS", "PT5M,PT5M,PT30M", true);
        if (bounds.size() != offsets.size())
            throw new ConfigException("LATENESS_BOUNDS: needs one bound per OFFSETS entry ("
                + offsets.size() + "), got " + bounds.size());
        int frequencyCap = e.integer("FREQUENCY_CAP", 3, 0);
        Duration frequencyWindow = e.duration("FREQUENCY_WINDOW", "P7D");
        int holdout = e.integer("HOLDOUT_PERCENT", 10, 0);
        if (holdout > 100) throw new ConfigException("HOLDOUT_PERCENT: must be at most 100, got " + holdout);
        int maxSendAttempts = e.integer("MAX_SEND_ATTEMPTS", 5, 1);
        Duration retryBase = e.duration("RETRY_BASE", "PT1M");
        RecoveryConfig recovery;
        try {
            recovery = new RecoveryConfig(window, offsets, bounds, frequencyCap, frequencyWindow,
                holdout, maxSendAttempts, retryBase);
        } catch (IllegalArgumentException ex) {
            // Every other RecoveryConfig rule is checked above; what remains is offset order against WINDOW.
            throw new ConfigException("OFFSETS: " + ex.getMessage());
        }

        int fastOffsets = e.integer("FAST_OFFSETS", 2, 0);
        Duration lease = e.duration("LEASE", "PT90S");
        Duration gatewayTimeout = e.duration("GATEWAY_TIMEOUT", "PT30S");
        Duration clockSkew = e.duration("CLOCK_SKEW", "PT5S");
        if (lease.compareTo(gatewayTimeout.multipliedBy(3)) < 0)
            throw new ConfigException("LEASE: must be at least 3 x GATEWAY_TIMEOUT (" + gatewayTimeout + "), got " + lease);
        DispatchConfig dispatch = new DispatchConfig(lease, gatewayTimeout, clockSkew, fastOffsets);

        int replicationFactor = e.integer("REPLICATION_FACTOR", 1, 1);
        int minInsync = e.integer("MIN_INSYNC_REPLICAS", 1, 1);
        if (minInsync > replicationFactor)
            throw new ConfigException("MIN_INSYNC_REPLICAS: must not exceed REPLICATION_FACTOR ("
                + replicationFactor + "), got " + minInsync);
        double maxSendRate = e.decimal("MAX_SEND_RATE", 1000);
        if (maxSendRate <= 0) throw new ConfigException("MAX_SEND_RATE: must be positive, got " + maxSendRate);
        double fastReserve = e.decimal("FAST_RESERVE", 0.3);
        if (fastReserve < 0 || fastReserve >= 1)
            throw new ConfigException("FAST_RESERVE: must be in [0, 1), got " + fastReserve);
        double sendFailureRate = e.decimal("SEND_FAILURE_RATE", 0);
        if (sendFailureRate < 0 || sendFailureRate > 1)
            throw new ConfigException("SEND_FAILURE_RATE: must be in [0, 1], got " + sendFailureRate);
        int healthPort = e.integer("HEALTH_PORT", 8081, 1);
        if (healthPort > 65535) throw new ConfigException("HEALTH_PORT: must be at most 65535, got " + healthPort);

        return new InfraConfig(recovery, dispatch,
            e.text("KAFKA_BOOTSTRAP", "localhost:9092"),
            e.text("REDIS_URL", "redis://localhost:6379"),
            e.text("DYNAMO_ENDPOINT", null),
            e.integer("SHARDS", 8, 1),
            e.integer("PARTITIONS", 8, 1),
            replicationFactor, minInsync,
            maxSendRate, fastReserve, sendFailureRate,
            e.duration("RECONCILE_INTERVAL", "PT5M"),
            e.duration("RETRY_POLL", "PT1S"),
            e.integer("MAX_IN_FLIGHT", 256, 1),
            healthPort);
    }

    /** First 12 hex chars of SHA-256 over the effective config; every role logs it at startup. */
    public String hash() {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 6);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private record Env(Map<String, String> vars) {
        String text(String name, String fallback) {
            String v = vars.get(name);
            return v == null || v.isBlank() ? fallback : v.trim();
        }

        Duration duration(String name, String fallback) {
            Duration d = parse(name, text(name, fallback));
            if (!d.isPositive()) throw new ConfigException(name + ": must be a positive duration, got " + d);
            return d;
        }

        List<Duration> durations(String name, String fallback, boolean zeroAllowed) {
            List<Duration> out = new ArrayList<>();
            for (String part : text(name, fallback).split(",", -1)) {
                Duration d = parse(name, part.trim());
                if (d.isNegative() || (!zeroAllowed && d.isZero()))
                    throw new ConfigException(name + ": " + (zeroAllowed ? "must not be negative" : "must be positive")
                        + ", got " + d);
                out.add(d);
            }
            return out;
        }

        int integer(String name, int fallback, int min) {
            String v = text(name, null);
            int n;
            if (v == null) {
                n = fallback;
            } else {
                try {
                    n = Integer.parseInt(v);
                } catch (NumberFormatException ex) {
                    throw new ConfigException(name + ": expected an integer, got '" + v + "'");
                }
            }
            if (n < min) throw new ConfigException(name + ": must be at least " + min + ", got " + n);
            return n;
        }

        double decimal(String name, double fallback) {
            String v = text(name, null);
            if (v == null) return fallback;
            try {
                double d = Double.parseDouble(v);
                if (Double.isFinite(d)) return d;
            } catch (NumberFormatException ignored) {
                // reported below
            }
            throw new ConfigException(name + ": expected a number, got '" + v + "'");
        }

        private static Duration parse(String name, String v) {
            try {
                return Duration.parse(v);
            } catch (DateTimeParseException ex) {
                throw new ConfigException(name + ": expected an ISO-8601 duration such as PT30M, got '" + v + "'");
            }
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.InfraConfigTest' --console=plain`
Expected: `BUILD SUCCESSFUL`; 4 plain tests and 20 parameterized cases PASSED. If a `RecoveryConfig` message case fails, A1 changed the wording in `RecoveryConfig`; update only the expected string in the CSV row to the new message (the `OFFSETS: ` prefix must stay).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/app/ConfigException.java \
        src/main/java/com/quince/cartrecovery/app/InfraConfig.java \
        src/test/java/com/quince/cartrecovery/app/InfraConfigTest.java
git commit -F - <<'EOF'
Add InfraConfig: env parsing, one-line validation errors, config hash

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
```

#### Part 2: Health, HealthServer, Role

- [ ] **Step 6: Write the failing test**

Create `src/test/java/com/quince/cartrecovery/app/HealthServerTest.java`:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.core.Metrics;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class HealthServerTest {
    private final AtomicLong nanos = new AtomicLong();
    private final Health health = new Health(nanos::get);
    private final Metrics metrics = new Metrics();
    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void healthyOnlyWhileEveryRegisteredLoopBeatWithinMaxSilence() {
        assertTrue(health.healthy(Duration.ofSeconds(1)), "no loop registered yet");
        health.beat("poll");
        health.beat("retry");
        nanos.addAndGet(Duration.ofSeconds(2).toNanos());
        health.beat("poll");
        assertFalse(health.healthy(Duration.ofSeconds(1)));
        assertEquals(List.of("retry"), health.stale(Duration.ofSeconds(1)));
        assertTrue(health.healthy(Duration.ofSeconds(3)));
    }

    @Test
    void servesHealthReadyAndMetrics() throws Exception {
        int port = freePort();
        health.beat("poll");
        health.setReady("breaker", "closed");
        health.setReady("breaker", "open");
        metrics.increment("dispatch.sent");
        metrics.increment("dispatch.sent");
        try (HealthServer ignored = new HealthServer(port, health, metrics, Duration.ofSeconds(1))) {
            HttpResponse<String> ok = get(port, "/health");
            assertEquals(200, ok.statusCode());
            assertEquals("ok\n", ok.body());

            nanos.addAndGet(Duration.ofSeconds(2).toNanos());
            HttpResponse<String> stale = get(port, "/health");
            assertEquals(503, stale.statusCode());
            assertEquals("stale: poll\n", stale.body());

            HttpResponse<String> ready = get(port, "/ready");
            assertEquals(200, ready.statusCode());
            assertEquals("breaker: open\n", ready.body());

            HttpResponse<String> m = get(port, "/metrics");
            assertEquals(200, m.statusCode());
            assertTrue(m.body().contains("dispatch.sent 2\n"), m.body());
        }
    }

    private HttpResponse<String> get(int port, String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
```

- [ ] **Step 7: Run test to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.HealthServerTest' --console=plain`
Expected: FAIL at `:compileTestJava` with `cannot find symbol ... class Health` and `class HealthServer`.

- [ ] **Step 8: Write the implementation**

Create `src/main/java/com/quince/cartrecovery/app/Health.java`:

```java
package com.quince.cartrecovery.app;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Liveness and readiness state of one role (spec §7.4). A loop registers on its first beat and must
 * beat on every iteration, including while backing off or paused, so a dependency outage never fails
 * the health check.
 */
public final class Health {
    private final LongSupplier nanoTime;
    private final Map<String, Long> beats = new ConcurrentHashMap<>();
    private final Map<String, String> ready = new ConcurrentHashMap<>();

    public Health() {
        this(System::nanoTime);
    }

    Health(LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
    }

    /** The loop made progress (an iteration, including backoff or pause). */
    public void beat(String loop) {
        beats.put(loop, nanoTime.getAsLong());
    }

    /** Shown on /ready as {@code key: value}. */
    public void setReady(String key, String value) {
        ready.put(key, value);
    }

    /** True when every registered loop beat within {@code maxSilence}; true when none registered. */
    public boolean healthy(Duration maxSilence) {
        return stale(maxSilence).isEmpty();
    }

    List<String> stale(Duration maxSilence) {
        long now = nanoTime.getAsLong();
        long limit = maxSilence.toNanos();
        return beats.entrySet().stream()
            .filter(e -> now - e.getValue() > limit)
            .map(Map.Entry::getKey)
            .sorted()
            .toList();
    }

    Map<String, String> readiness() {
        return new TreeMap<>(ready);
    }
}
```

Create `src/main/java/com/quince/cartrecovery/app/HealthServer.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** /health, /ready and /metrics on the JDK HTTP server (spec §7.4). Plain text. */
public final class HealthServer implements AutoCloseable {
    private final HttpServer server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public HealthServer(int port, Health health, Metrics metrics, Duration maxSilence) {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot bind health port " + port, e);
        }
        server.createContext("/health", ex -> {
            List<String> stale = health.stale(maxSilence);
            if (stale.isEmpty()) respond(ex, 200, "ok\n");
            else respond(ex, 503, "stale: " + String.join(",", stale) + "\n");
        });
        server.createContext("/ready", ex -> respond(ex, 200, lines(health.readiness(), ": ")));
        server.createContext("/metrics", ex -> respond(ex, 200, lines(metrics.snapshot(), " ")));
        server.setExecutor(executor);
        server.start();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.close();
    }

    private static String lines(Map<String, ?> values, String separator) {
        StringBuilder sb = new StringBuilder();
        values.forEach((k, v) -> sb.append(k).append(separator).append(v).append('\n'));
        return sb.toString();
    }

    private static void respond(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }
}
```

Create `src/main/java/com/quince/cartrecovery/app/Role.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;

/**
 * One deployable role, selected with {@code --role=<name>}. {@link #run} returns when its thread is interrupted:
 * Main's shutdown hook (SIGTERM) interrupts the role thread and waits up to 30 s for it to return.
 */
public interface Role {
    String name();

    void run(InfraConfig config, Health health, Metrics metrics) throws Exception;
}
```

- [ ] **Step 9: Run test to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.HealthServerTest' --console=plain`
Expected: `BUILD SUCCESSFUL`; 2 tests PASSED.

- [ ] **Step 10: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/app/Health.java \
        src/main/java/com/quince/cartrecovery/app/HealthServer.java \
        src/main/java/com/quince/cartrecovery/app/Role.java \
        src/test/java/com/quince/cartrecovery/app/HealthServerTest.java
git commit -F - <<'EOF'
Add Health, HealthServer (/health, /ready, /metrics) and Role

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
```

#### Part 3: TokenBucket

- [ ] **Step 11: Write the failing test**

Create `src/test/java/com/quince/cartrecovery/app/TokenBucketTest.java`:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Lane;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TokenBucketTest {
    private final AtomicLong nanos = new AtomicLong();
    private final TokenBucket bucket = new TokenBucket(10, 0.3, nanos::get);   // capacity 10, reserve 3

    @Test
    void slowLaneStopsAtTheFastReserveAndFastTakesTheRest() {
        int slow = 0;
        while (bucket.tryAcquire(Lane.SLOW)) slow++;
        assertEquals(7, slow);
        assertFalse(bucket.slowAllowed());
        assertTrue(bucket.anyAvailable());

        int fast = 0;
        while (bucket.tryAcquire(Lane.FAST)) fast++;
        assertEquals(3, fast);
        assertFalse(bucket.anyAvailable());
        assertFalse(bucket.slowAllowed());
    }

    @Test
    void refillsAtTheRateUpToOneSecondOfCapacity() {
        while (bucket.tryAcquire(Lane.FAST)) { }
        nanos.addAndGet(100_000_000L);                 // 100 ms at 10/s = 1 token
        assertTrue(bucket.tryAcquire(Lane.FAST));
        assertFalse(bucket.tryAcquire(Lane.FAST));

        nanos.addAndGet(60_000_000_000L);              // a minute idle still caps at 10
        int n = 0;
        while (bucket.tryAcquire(Lane.FAST)) n++;
        assertEquals(10, n);
    }

    @Test
    void slowLaneResumesOnlyAboveTheReserve() {
        while (bucket.tryAcquire(Lane.FAST)) { }
        nanos.addAndGet(300_000_000L);                 // 3 tokens: exactly the reserve
        assertTrue(bucket.anyAvailable());
        assertFalse(bucket.slowAllowed());
        assertFalse(bucket.tryAcquire(Lane.SLOW));

        nanos.addAndGet(100_000_000L);                 // 4 tokens
        assertTrue(bucket.slowAllowed());
        assertTrue(bucket.tryAcquire(Lane.SLOW));
    }

    @Test
    void rejectsInvalidSettings() {
        assertThrows(IllegalArgumentException.class, () -> new TokenBucket(0, 0.3, nanos::get));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucket(10, 1.0, nanos::get));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucket(10, -0.1, nanos::get));
    }
}
```

- [ ] **Step 12: Run test to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.TokenBucketTest' --console=plain`
Expected: FAIL at `:compileTestJava` with `cannot find symbol ... class TokenBucket`.

- [ ] **Step 13: Write the implementation**

Create `src/main/java/com/quince/cartrecovery/app/TokenBucket.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.ports.SendBudget;
import java.util.function.LongSupplier;

/**
 * The per-replica send budget shared by both lanes (spec §6.3). Capacity is one second of rate.
 * FAST takes any whole token; SLOW takes one only while more than {@code fastReserve × capacity}
 * remain, so the next-day burst cannot starve the early reminders. Never blocks.
 */
public final class TokenBucket implements SendBudget {
    private final double ratePerNano;
    private final double capacity;
    private final double reserve;
    private final LongSupplier nanoTime;
    private double tokens;
    private long last;

    public TokenBucket(double ratePerSecond, double fastReserve, LongSupplier nanoTime) {
        if (!(ratePerSecond > 0)) throw new IllegalArgumentException("ratePerSecond must be positive");
        if (fastReserve < 0 || fastReserve >= 1) throw new IllegalArgumentException("fastReserve must be in [0, 1)");
        // ponytail: capacity floors at 1 token so a rate below 1/s still sends; burst = 1 s of rate.
        this.capacity = Math.max(1.0, ratePerSecond);
        this.ratePerNano = ratePerSecond / 1e9;
        this.reserve = fastReserve * capacity;
        this.nanoTime = nanoTime;
        this.tokens = capacity;
        this.last = nanoTime.getAsLong();
    }

    @Override
    public synchronized boolean tryAcquire(Lane lane) {
        refill();
        boolean ok = lane == Lane.FAST ? tokens >= 1 : slowOk();
        if (ok) tokens -= 1;
        return ok;
    }

    /** False while the bucket is at or below the fast reserve: the slow-lane consumer pauses. */
    public synchronized boolean slowAllowed() {
        refill();
        return slowOk();
    }

    /** False while the bucket is empty: the fast-lane consumer pauses. */
    public synchronized boolean anyAvailable() {
        refill();
        return tokens >= 1;
    }

    private boolean slowOk() {
        return tokens >= 1 && tokens > reserve;
    }

    private void refill() {
        long now = nanoTime.getAsLong();
        tokens = Math.min(capacity, tokens + (now - last) * ratePerNano);
        last = now;
    }
}
```

- [ ] **Step 14: Run test to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.TokenBucketTest' --console=plain`
Expected: `BUILD SUCCESSFUL`; 4 tests PASSED.

- [ ] **Step 15: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/app/TokenBucket.java \
        src/test/java/com/quince/cartrecovery/app/TokenBucketTest.java
git commit -F - <<'EOF'
Add TokenBucket send budget with fast-lane reserve

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
```

#### Part 4: CircuitBreaker

- [ ] **Step 16: Write the failing test**

Create `src/test/java/com/quince/cartrecovery/app/CircuitBreakerTest.java`:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.ports.NotificationSink;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CircuitBreakerTest {
    private static final ReminderMessage MSG = new ReminderMessage("c1:1:0", "c1", "shopper-1", null, List.of());
    private static final SendResult S = SendResult.SENT;
    private static final SendResult T = SendResult.TRANSIENT_FAILURE;

    private final FakeClock clock = new FakeClock(Instant.parse("2026-01-01T00:00:00Z"));
    private final Deque<SendResult> script = new ArrayDeque<>();
    private final AtomicInteger calls = new AtomicInteger();
    private final NotificationSink sink = m -> {
        calls.incrementAndGet();
        return script.isEmpty() ? SendResult.SENT : script.poll();
    };
    private final CircuitBreaker breaker = new CircuitBreaker(sink, 10, 0.5, Duration.ofSeconds(30), clock);

    private void sendAll(SendResult... results) {
        script.addAll(List.of(results));
        for (int i = 0; i < results.length; i++) breaker.send(MSG);
    }

    private void open() {
        sendAll(T, T, T, T, T, T, T, T, T, T);
        assertTrue(breaker.isOpen());
    }

    @Test
    void ratioAtTheThresholdStaysClosed() {
        sendAll(S, S, S, S, S, T, T, T, T, T);
        assertFalse(breaker.isOpen());
    }

    @Test
    void opensOnlyOnceTheWindowIsFullAndTheRatioIsOverTheThreshold() {
        sendAll(S, S, S, S, T, T, T, T, T);
        assertFalse(breaker.isOpen(), "window of 10 not full yet");
        sendAll(T);
        assertTrue(breaker.isOpen());
    }

    @Test
    void theWindowSlidesOverTheLastAttempts() {
        sendAll(S, S, S, S, S, S, S, S, S, S, T, T, T, T, T);
        assertFalse(breaker.isOpen(), "5 of the last 10");
        sendAll(T);
        assertTrue(breaker.isOpen(), "6 of the last 10");
    }

    @Test
    void whileOpenSendsFailFastWithoutReachingTheDelegate() {
        open();
        int before = calls.get();
        assertEquals(T, breaker.send(MSG));
        assertEquals(before, calls.get());
        clock.advance(Duration.ofSeconds(29));
        assertTrue(breaker.isOpen());
        assertEquals(T, breaker.send(MSG));
        assertEquals(before, calls.get());
    }

    @Test
    void aSuccessfulProbeClosesAndClearsTheWindow() {
        open();
        clock.advance(Duration.ofSeconds(30));
        assertFalse(breaker.isOpen(), "due for a probe: consumers resume and supply it");
        int before = calls.get();
        assertEquals(S, breaker.send(MSG));
        assertEquals(before + 1, calls.get());
        assertFalse(breaker.isOpen());
        sendAll(T, T, T, T, T, T, T, T, T);
        assertFalse(breaker.isOpen(), "old failures were cleared");
    }

    @Test
    void aFailedProbeReopensForAnotherPeriod() {
        open();
        clock.advance(Duration.ofSeconds(30));
        script.add(T);
        assertEquals(T, breaker.send(MSG));
        assertTrue(breaker.isOpen());
        clock.advance(Duration.ofSeconds(29));
        assertTrue(breaker.isOpen());
        clock.advance(Duration.ofSeconds(1));
        assertFalse(breaker.isOpen());
    }

    @Test
    void onlyOneProbeRunsWhileHalfOpen() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger n = new AtomicInteger();
        NotificationSink slow = m -> {
            if (n.incrementAndGet() <= 10) return T;
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return S;
        };
        CircuitBreaker b = new CircuitBreaker(slow, 10, 0.5, Duration.ofSeconds(30), clock);
        for (int i = 0; i < 10; i++) b.send(MSG);
        assertTrue(b.isOpen());
        clock.advance(Duration.ofSeconds(30));

        Thread probe = Thread.ofPlatform().start(() -> b.send(MSG));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertTrue(b.isOpen(), "open while the probe is in flight");
        assertEquals(T, b.send(MSG));
        assertEquals(11, n.get(), "the second caller never reached the delegate");

        release.countDown();
        probe.join();
        assertFalse(b.isOpen());
    }
}
```

- [ ] **Step 17: Run test to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.CircuitBreakerTest' --console=plain`
Expected: FAIL at `:compileTestJava` with `cannot find symbol ... class CircuitBreaker`.

- [ ] **Step 18: Write the implementation**

Create `src/main/java/com/quince/cartrecovery/app/CircuitBreaker.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.NotificationSink;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

/**
 * Wraps the sink (spec §6.2). Over the last {@code window} attempts, a transient-failure ratio above
 * {@code threshold} opens the breaker for {@code openFor}; while open, sends fail fast with
 * TRANSIENT_FAILURE without reaching the delegate. After {@code openFor}, exactly one probe send
 * decides: success closes and clears the window, a transient failure reopens.
 * {@link #isOpen()} is what consumers and the retry loop pause on.
 */
public final class CircuitBreaker implements NotificationSink {
    private final NotificationSink delegate;
    private final int window;
    private final double threshold;
    private final Duration openFor;
    private final Clock clock;
    private final boolean[] outcomes;   // ring of closed-state attempts; true = transient failure
    private int count;
    private int next;
    private int transients;
    private Instant openUntil;          // null while closed
    private boolean probing;

    public CircuitBreaker(NotificationSink delegate, int window, double threshold, Duration openFor, Clock clock) {
        if (window < 1) throw new IllegalArgumentException("window must be at least 1");
        if (threshold < 0 || threshold >= 1) throw new IllegalArgumentException("threshold must be in [0, 1)");
        if (!openFor.isPositive()) throw new IllegalArgumentException("openFor must be positive");
        this.delegate = delegate;
        this.window = window;
        this.threshold = threshold;
        this.openFor = openFor;
        this.clock = clock;
        this.outcomes = new boolean[window];
    }

    @Override
    public SendResult send(ReminderMessage message) {
        boolean probe = false;
        synchronized (this) {
            if (openUntil != null) {
                if (probing || clock.now().isBefore(openUntil)) return SendResult.TRANSIENT_FAILURE;
                probing = true;
                probe = true;
            }
        }
        SendResult result = null;
        try {
            result = delegate.send(message);   // outside the lock: a slow gateway never blocks isOpen()
            return result;
        } finally {
            settle(probe, result);
        }
    }

    public synchronized boolean isOpen() {
        return openUntil != null && (probing || clock.now().isBefore(openUntil));
    }

    private synchronized void settle(boolean probe, SendResult result) {
        boolean failed = result == null || result == SendResult.TRANSIENT_FAILURE;   // a throw counts as transient
        if (probe) {
            probing = false;
            if (failed) {
                openUntil = clock.now().plus(openFor);
            } else {
                openUntil = null;
                clear();
            }
            return;
        }
        if (openUntil != null) return;     // a send that started before the breaker opened
        if (count == window && outcomes[next]) transients--;
        outcomes[next] = failed;
        if (failed) transients++;
        next = (next + 1) % window;
        count = Math.min(count + 1, window);
        if (count == window && transients > threshold * window) {
            openUntil = clock.now().plus(openFor);
            clear();
        }
    }

    private void clear() {
        Arrays.fill(outcomes, false);
        count = 0;
        next = 0;
        transients = 0;
    }
}
```

- [ ] **Step 19: Run the C0a tests and the whole unit suite**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.*' --console=plain`
Expected: `BUILD SUCCESSFUL`; `CircuitBreakerTest` 7 PASSED plus the earlier C0a tests.

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`, no failures.

- [ ] **Step 20: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/app/CircuitBreaker.java \
        src/test/java/com/quince/cartrecovery/app/CircuitBreakerTest.java
git commit -F - <<'EOF'
Add CircuitBreaker sink decorator with single half-open probe

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task C0b: BatchConsumerLoop

**Model:** opus (master §2).

**Files:**
- Create: `src/main/java/com/quince/cartrecovery/app/PoisonException.java`
- Create: `src/main/java/com/quince/cartrecovery/app/BatchConsumerLoop.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/app/BatchConsumerLoopTest.java`

**Interfaces:**
- Consumes: `Health` (`beat(String)`) from C0a; `Metrics` (`increment`, `get`; thread-safe after A1); `kafka-clients` `KafkaConsumer`, `Producer<String, byte[]>`, `Deserializer<V>`.
- Produces (master §1.5, exact):
  - `final class PoisonException extends RuntimeException { PoisonException(String message, Throwable cause); }`
  - `final class BatchConsumerLoop<V> implements AutoCloseable` with nested `enum Verdict { DONE, HOLD }`, `interface Handler<V> { Verdict handle(ConsumerRecord<String, V> record) throws Exception; }`, `interface Hooks<V> { default void beforePoll(Consumer<String, V> consumer) {} default void afterCommit(Consumer<String, V> consumer, Map<TopicPartition, Long> committed, int generation) {} }`, `record Settings(String groupId, List<String> topics, int maxPollRecords, Duration pollTimeout, int maxInFlight, String dlqTopic)`; constructor `BatchConsumerLoop(Map<String, Object> consumerProps, Settings settings, Deserializer<V> valueDeserializer, Handler<V> handler, Hooks<V> hooks, Producer<String, byte[]> dlqProducer, Health health, Metrics metrics)`; `void pauseWhile(Predicate<TopicPartition> shouldPause)`; `void run()`; `void close()`.
- Behaviour C1 relies on:
  - Consumer props: the loop forces `group.id = settings.groupId`, `enable.auto.commit = false`, `group.protocol = classic`, `max.poll.records = settings.maxPollRecords`, and defaults `auto.offset.reset = earliest` when the caller does not set it. Caller supplies `bootstrap.servers` and anything else.
  - Health loop name: `"consumer:" + groupId + ":" + String.join(",", topics)`, beaten once per iteration (also while a batch is in flight, paused, or backing off).
  - `afterCommit` receives the *next offset to consume* per partition, the same number `commitSync` stored, and `consumer.groupMetadata().generationId()`. It also fires for the commit made inside `onPartitionsRevoked`, and once per poll-loop iteration that committed nothing, with an empty map, so hooks can act on idle partitions every iteration (spec §5.4: the detector writes watermarks every poll loop).
  - HOLD: the partition is seeked back to the held record, committed below it, and paused; it resumes at the first iteration at least `pollTimeout` later where `shouldPause.test(tp)` is false.
  - Handler failure: 3 attempts in process (backoff 100 ms, 200 ms); then the partition is committed below the failed record, seeked back to it, and paused for `min(30 s, 1 s × 2^(n−1))` for the n-th consecutive failure on that partition.
  - `PoisonException` (or a value the deserializer rejects): record produced to `dlqTopic` with the original key and value bytes and headers `error` (exception message) and `source-offset` (`<topic>-<partition>@<offset>`), then counted as done. With `dlqTopic == null` the record is dropped and counted `consumer.poison_dropped`.
  - Metrics: `consumer.hold`, `consumer.retry`, `consumer.retry_exhausted`, `consumer.poison`, `consumer.poison_dropped`, `consumer.commit_failed`, `consumer.pause_check_failed`.
  - `close()`: returns within 30 s: stop polling, wait up to 25 s for in-flight groups, commit completed prefixes, close the consumer with a 5 s timeout.

Design (spec §6.3): the poll thread owns the consumer. After a poll returns records, the batch is grouped by record key; each group runs on its own virtual thread behind a `Semaphore(maxInFlight)`, records within a group in order. While a batch is in flight the poll thread keeps iterating with every assigned partition paused: `poll(Duration.ZERO)` keeps group membership alive, runs `beforePoll` (the detector keeps writing watermarks), beats health, and lets a rebalance happen mid-batch. `onPartitionsRevoked` then commits only each revoked partition's completed prefix and marks it so the batch end does not commit it again (Review Focus 4).

- [ ] **Step 1: Write the failing test**

Create `src/integrationTest/java/com/quince/cartrecovery/app/BatchConsumerLoopTest.java`:

```java
package com.quince.cartrecovery.app;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.app.BatchConsumerLoop.Verdict;
import com.quince.cartrecovery.core.Metrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class BatchConsumerLoopTest {
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));
    static final Duration WAIT = Duration.ofSeconds(30);

    private static Admin admin;
    private static KafkaProducer<String, String> producer;
    private static KafkaProducer<String, byte[]> dlqProducer;

    private final List<BatchConsumerLoop<String>> loops = new ArrayList<>();
    private final Metrics metrics = new Metrics();
    private final Health health = new Health();
    private final String group = "g-" + UUID.randomUUID();

    @BeforeAll
    static void clients() {
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
        Map<String, Object> props = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
            ProducerConfig.ACKS_CONFIG, "all");
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        dlqProducer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
    }

    @AfterAll
    static void closeClients() {
        producer.close();
        dlqProducer.close();
        admin.close();
    }

    @AfterEach
    void closeLoops() {
        loops.forEach(BatchConsumerLoop::close);
    }

    @Test
    void recordsWithinAKeyRunInOrderAndEveryPartitionIsCommitted() throws Exception {
        String topic = topic(4);
        for (int i = 0; i < 20; i++)
            for (int k = 0; k < 10; k++) send(topic, "k" + k, "k" + k + ":" + i);
        Map<String, List<Integer>> seen = new ConcurrentHashMap<>();
        start(loop(topic, 50, 16, null, new StringDeserializer(), r -> {
            Thread.sleep(ThreadLocalRandom.current().nextInt(3));
            seen.computeIfAbsent(r.key(), k -> Collections.synchronizedList(new ArrayList<>()))
                .add(Integer.parseInt(r.value().substring(r.value().indexOf(':') + 1)));
            return Verdict.DONE;
        }));

        Await.until(() -> totalCommitted(topic, 4) == 200, WAIT);
        List<Integer> expected = IntStream.range(0, 20).boxed().toList();
        for (int k = 0; k < 10; k++) assertEquals(expected, seen.get("k" + k), "order for k" + k);
    }

    @Test
    void distinctKeysRunConcurrentlyUpToMaxInFlight() throws Exception {
        String topic = topic(1);
        for (int k = 0; k < 8; k++) send(topic, "k" + k, "v");
        AtomicInteger running = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        AtomicInteger handled = new AtomicInteger();
        start(loop(topic, 100, 4, null, new StringDeserializer(), r -> {
            peak.accumulateAndGet(running.incrementAndGet(), Math::max);
            Thread.sleep(300);
            running.decrementAndGet();
            handled.incrementAndGet();
            return Verdict.DONE;
        }));

        Await.until(() -> committed(new TopicPartition(topic, 0)) == 8, WAIT);
        assertEquals(8, handled.get());
        assertTrue(peak.get() >= 2, "keys should run concurrently, peak " + peak.get());
        assertTrue(peak.get() <= 4, "maxInFlight bounds concurrency, peak " + peak.get());
    }

    @Test
    void holdPausesSeeksBackAndCommitsBelowTheHeldRecord() throws Exception {
        String topic = topic(1);
        TopicPartition tp = new TopicPartition(topic, 0);
        for (int i = 0; i < 5; i++) send(topic, "k" + i, "v" + i);
        AtomicBoolean hold = new AtomicBoolean(true);
        AtomicInteger holds = new AtomicInteger();
        Map<String, AtomicInteger> done = new ConcurrentHashMap<>();
        BatchConsumerLoop<String> loop = loop(topic, 100, 8, null, new StringDeserializer(), r -> {
            if (r.value().equals("v2") && hold.get()) {
                holds.incrementAndGet();
                return Verdict.HOLD;
            }
            done.computeIfAbsent(r.value(), v -> new AtomicInteger()).incrementAndGet();
            return Verdict.DONE;
        });
        loop.pauseWhile(p -> hold.get() && holds.get() > 0);
        start(loop);

        Await.until(() -> committed(tp) == 2 && done.containsKey("v3") && done.containsKey("v4"), WAIT);
        Thread.sleep(1000);
        assertEquals(1, holds.get(), "a paused partition is not re-polled");
        assertEquals(2, committed(tp), "never committed past the held record");
        assertEquals(1, metrics.get("consumer.hold"));

        hold.set(false);
        Await.until(() -> committed(tp) == 5, WAIT);
        assertEquals(1, done.get("v2").get());
        assertEquals(2, done.get("v3").get(), "records after the held one are redelivered");
    }

    @Test
    void poisonAndUndeserializableRecordsGoToTheDlqWithOriginalBytes() throws Exception {
        String topic = topic(1);
        String dlq = topic(1);
        send(topic, "a", "ok1");
        send(topic, "b", "poison");
        send(topic, "c", "undeserializable");
        send(topic, "d", "ok2");
        Deserializer<String> deserializer = (t, data) -> {
            String s = new String(data, UTF_8);
            if (s.equals("undeserializable")) throw new SerializationException("bad bytes");
            return s;
        };
        Set<String> handled = ConcurrentHashMap.newKeySet();
        start(loop(topic, 100, 8, dlq, deserializer, r -> {
            if (r.value().equals("poison")) throw new PoisonException("unknown event type", null);
            handled.add(r.value());
            return Verdict.DONE;
        }));

        Await.until(() -> committed(new TopicPartition(topic, 0)) == 4, WAIT);
        assertEquals(Set.of("ok1", "ok2"), handled);
        assertEquals(2, metrics.get("consumer.poison"));

        Map<String, ConsumerRecord<String, byte[]>> dead = readAll(dlq, 2).stream()
            .collect(Collectors.toMap(ConsumerRecord::key, r -> r));
        assertArrayEquals("poison".getBytes(UTF_8), dead.get("b").value());
        assertEquals("unknown event type", header(dead.get("b"), "error"));
        assertEquals(topic + "-0@1", header(dead.get("b"), "source-offset"));
        assertArrayEquals("undeserializable".getBytes(UTF_8), dead.get("c").value());
        assertTrue(header(dead.get("c"), "error").contains("bad bytes"));
        assertEquals(topic + "-0@2", header(dead.get("c"), "source-offset"));
    }

    @Test
    void transientFailuresRetryInProcessThenRedeliverAfterBackoff() throws Exception {
        String topic = topic(1);
        send(topic, "a", "flaky2");   // fails twice, succeeds on the third in-process attempt
        send(topic, "b", "flaky3");   // fails all three in-process attempts, succeeds on redelivery
        Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        start(loop(topic, 100, 8, null, new StringDeserializer(), r -> {
            int n = attempts.computeIfAbsent(r.value(), v -> new AtomicInteger()).incrementAndGet();
            if (r.value().equals("flaky2") && n <= 2) throw new IllegalStateException("transient " + n);
            if (r.value().equals("flaky3") && n <= 3) throw new IllegalStateException("transient " + n);
            return Verdict.DONE;
        }));

        Await.until(() -> committed(new TopicPartition(topic, 0)) == 2, WAIT);
        assertEquals(3, attempts.get("flaky2").get(), "no redelivery after an in-process success");
        assertEquals(4, attempts.get("flaky3").get(), "three in-process attempts, then one redelivery");
        assertEquals(4, metrics.get("consumer.retry"));
        assertEquals(1, metrics.get("consumer.retry_exhausted"));
    }

    // Review Focus 4: a revoke while groups are in flight commits only the completed prefix.
    @Test
    void revokeMidFlightCommitsOnlyTheCompletedPrefix() throws Exception {
        String topic = topic(1);
        TopicPartition tp = new TopicPartition(topic, 0);
        send(topic, "a", "fast-a");
        send(topic, "s", "slow");
        send(topic, "c", "fast-c");
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean slowFinished = new AtomicBoolean();
        Set<String> done = ConcurrentHashMap.newKeySet();
        BatchConsumerLoop.Handler<String> handler = r -> {
            if (r.value().equals("slow")) {
                release.await();
                slowFinished.set(true);
            }
            done.add(r.value());
            return Verdict.DONE;
        };
        start(loop(topic, 100, 8, null, new StringDeserializer(), handler));
        Await.until(() -> done.containsAll(Set.of("fast-a", "fast-c")), WAIT);
        assertEquals(-1, committed(tp), "nothing is committed while the batch is in flight");

        start(loop(topic, 100, 8, null, new StringDeserializer(), handler));   // same group: forces a rebalance
        Await.until(() -> committed(tp) >= 0, WAIT);
        assertFalse(slowFinished.get());
        assertEquals(1, committed(tp), "commit stops at the unfinished record, never past it");

        release.countDown();
        Await.until(() -> committed(tp) == 3, WAIT);
    }

    @Test
    void pauseWhileHoldsEveryPartitionAndTheLoopStillBeats() throws Exception {
        String topic = topic(2);
        AtomicBoolean paused = new AtomicBoolean(true);
        List<String> handled = Collections.synchronizedList(new ArrayList<>());
        BatchConsumerLoop<String> loop = loop(topic, 100, 8, null, new StringDeserializer(), r -> {
            handled.add(r.value());
            return Verdict.DONE;
        });
        loop.pauseWhile(p -> paused.get());
        start(loop);
        for (int i = 0; i < 3; i++) send(topic, "k" + i, "v" + i);

        Thread.sleep(1500);
        assertTrue(handled.isEmpty(), "paused partitions deliver nothing");
        assertTrue(health.healthy(Duration.ofSeconds(1)), "a paused loop keeps beating");

        paused.set(false);
        Await.until(() -> totalCommitted(topic, 2) == 3, WAIT);
        assertEquals(3, handled.size());
    }

    @Test
    void afterCommitRunsEveryIterationEvenWhenNothingIsCommitted() throws Exception {
        String topic = topic(1);
        AtomicInteger emptyCalls = new AtomicInteger();
        BatchConsumerLoop<String> loop = new BatchConsumerLoop<>(
            Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
            new BatchConsumerLoop.Settings(group, List.of(topic), 100, Duration.ofMillis(200), 8, null),
            new StringDeserializer(), r -> Verdict.DONE,
            new BatchConsumerLoop.Hooks<String>() {
                @Override public void afterCommit(org.apache.kafka.clients.consumer.Consumer<String, String> consumer,
                                                  Map<TopicPartition, Long> committed, int generation) {
                    if (committed.isEmpty()) emptyCalls.incrementAndGet();
                }
            }, dlqProducer, health, metrics);
        loops.add(loop);
        start(loop);   // an empty topic: no record is ever committed
        Await.until(() -> emptyCalls.get() >= 3, WAIT);
    }

    @Test
    void closeLetsInFlightGroupsFinishAndCommits() throws Exception {
        String topic = topic(1);
        TopicPartition tp = new TopicPartition(topic, 0);
        send(topic, "a", "fast-a");
        send(topic, "s", "slow");
        send(topic, "c", "fast-c");
        CountDownLatch slowStarted = new CountDownLatch(1);
        AtomicBoolean slowDone = new AtomicBoolean();
        BatchConsumerLoop<String> loop = loop(topic, 100, 8, null, new StringDeserializer(), r -> {
            if (r.value().equals("slow")) {
                slowStarted.countDown();
                Thread.sleep(1000);
                slowDone.set(true);
            }
            return Verdict.DONE;
        });
        start(loop);
        assertTrue(slowStarted.await(30, TimeUnit.SECONDS));

        long t0 = System.nanoTime();
        loop.close();
        long elapsed = System.nanoTime() - t0;

        assertTrue(slowDone.get(), "the in-flight group finished before close returned");
        assertEquals(3, committed(tp));
        assertTrue(elapsed < Duration.ofSeconds(30).toNanos(), "closed within 30 s");
    }

    private BatchConsumerLoop<String> loop(String topic, int maxPollRecords, int maxInFlight, String dlq,
                                           Deserializer<String> deserializer, BatchConsumerLoop.Handler<String> handler) {
        Map<String, Object> props = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        BatchConsumerLoop<String> loop = new BatchConsumerLoop<>(props,
            new BatchConsumerLoop.Settings(group, List.of(topic), maxPollRecords, Duration.ofMillis(200), maxInFlight, dlq),
            deserializer, handler, new BatchConsumerLoop.Hooks<>() { }, dlqProducer, health, metrics);
        loops.add(loop);
        return loop;
    }

    private static void start(BatchConsumerLoop<String> loop) {
        Thread.ofPlatform().name("poll-loop").start(loop::run);
    }

    private static String topic(int partitions) throws Exception {
        String name = "t-" + UUID.randomUUID();
        admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get();
        return name;
    }

    private static void send(String topic, String key, String value) throws Exception {
        producer.send(new ProducerRecord<>(topic, key, value)).get();
    }

    /** Unchecked, so it can run inside Await.until's BooleanSupplier. */
    private long committed(TopicPartition tp) {
        try {
            OffsetAndMetadata o = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().get(tp);
            return o == null ? -1 : o.offset();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof GroupIdNotFoundException) return -1;
            throw new IllegalStateException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private long totalCommitted(String topic, int partitions) {
        long total = 0;
        for (int p = 0; p < partitions; p++) total += Math.max(0, committed(new TopicPartition(topic, p)));
        return total;
    }

    private static List<ConsumerRecord<String, byte[]>> readAll(String topic, int expected) {
        Map<String, Object> props = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer())) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c.assign(List.of(tp));
            c.seekToBeginning(List.of(tp));
            List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (out.size() < expected && System.nanoTime() < deadline) c.poll(Duration.ofMillis(200)).forEach(out::add);
            return out;
        }
    }

    private static String header(ConsumerRecord<String, byte[]> r, String name) {
        return new String(r.headers().lastHeader(name).value(), UTF_8);
    }

}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.app.BatchConsumerLoopTest' --console=plain`
Expected: FAIL at `:compileIntegrationTestJava` with `cannot find symbol ... class BatchConsumerLoop` and `class PoisonException`.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/com/quince/cartrecovery/app/PoisonException.java`:

```java
package com.quince.cartrecovery.app;

/** A deterministic failure for one record: dead-lettered and committed, never retried. */
public final class PoisonException extends RuntimeException {
    public PoisonException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

Create `src/main/java/com/quince/cartrecovery/app/BatchConsumerLoop.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * One consumer and its poll thread (spec §6.3). Each batch is grouped by record key; groups run
 * concurrently on virtual threads, at most {@code maxInFlight} at once, records within a group in
 * order. After the batch each partition is committed up to its lowest held or unfinished offset,
 * never past it. While a batch is in flight the poll thread keeps polling with every partition
 * paused, so membership, hooks, health and rebalances continue; a revoke commits only completed
 * prefixes.
 */
public final class BatchConsumerLoop<V> implements AutoCloseable {

    public enum Verdict { DONE, HOLD }

    public interface Handler<V> {
        Verdict handle(ConsumerRecord<String, V> record) throws Exception;
    }

    /** Called on the poll thread. Hooks may read offsets and metadata but must not call poll. */
    public interface Hooks<V> {
        default void beforePoll(Consumer<String, V> consumer) {}

        /** {@code committed} maps each partition to the next offset to consume. */
        default void afterCommit(Consumer<String, V> consumer, Map<TopicPartition, Long> committed, int generation) {}
    }

    public record Settings(String groupId, List<String> topics, int maxPollRecords, Duration pollTimeout,
                           int maxInFlight, String dlqTopic /* nullable */) {
        public Settings {
            topics = List.copyOf(topics);
            if (maxPollRecords < 1 || maxInFlight < 1) throw new IllegalArgumentException("maxPollRecords and maxInFlight must be >= 1");
        }
    }

    private static final System.Logger LOG = System.getLogger(BatchConsumerLoop.class.getName());
    static final int ATTEMPTS = 3;
    static final long RETRY_BACKOFF_MS = 100;
    static final long MAX_BACKOFF_MS = 30_000;
    static final Duration DRAIN_BUDGET = Duration.ofSeconds(25);
    static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private enum State { PENDING, DONE, HELD, FAILED }

    private static final class Rec {
        final ConsumerRecord<String, byte[]> raw;
        volatile State state = State.PENDING;

        Rec(ConsumerRecord<String, byte[]> raw) {
            this.raw = raw;
        }
    }

    private static final class Batch {
        final Map<TopicPartition, List<Rec>> byPartition = new HashMap<>();
        final Set<TopicPartition> revoked = new HashSet<>();   // committed by the revoke; skipped at batch end
        CountDownLatch done;
    }

    private final KafkaConsumer<String, byte[]> consumer;
    private final Consumer<String, V> view;
    private final Settings settings;
    private final Deserializer<V> valueDeserializer;
    private final Handler<V> handler;
    private final Hooks<V> hooks;
    private final Producer<String, byte[]> dlqProducer;
    private final Health health;
    private final Metrics metrics;
    private final String loopName;
    private final Semaphore permits;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<TopicPartition, Long> notBefore = new HashMap<>();   // poll thread only; System.nanoTime
    private final Map<TopicPartition, Integer> failures = new HashMap<>(); // poll thread only
    private final AtomicBoolean started = new AtomicBoolean();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private volatile Predicate<TopicPartition> shouldPause = tp -> false;
    private volatile boolean closing;
    private boolean committedThisIteration;                                 // poll thread only
    private Batch batch;                                                    // poll thread only

    public BatchConsumerLoop(Map<String, Object> consumerProps, Settings settings, Deserializer<V> valueDeserializer,
                             Handler<V> handler, Hooks<V> hooks, Producer<String, byte[]> dlqProducer,
                             Health health, Metrics metrics) {
        Map<String, Object> props = new HashMap<>(consumerProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, settings.groupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");   // watermark fencing needs classic generations
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, settings.maxPollRecords());
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer());
        this.view = viewOf(consumer);
        this.settings = settings;
        this.valueDeserializer = valueDeserializer;
        this.handler = handler;
        this.hooks = hooks;
        this.dlqProducer = dlqProducer;
        this.health = health;
        this.metrics = metrics;
        this.loopName = "consumer:" + settings.groupId() + ":" + String.join(",", settings.topics());
        this.permits = new Semaphore(settings.maxInFlight());
        // Fetch DLQ metadata now, on this platform thread, so a first poison send from a virtual
        // thread does not block on metadata; also fails fast if the DLQ topic is missing.
        if (settings.dlqTopic() != null) dlqProducer.partitionsFor(settings.dlqTopic());
    }

    /** Evaluated for every assigned partition on every iteration; a partition resumes when it returns false. */
    public void pauseWhile(Predicate<TopicPartition> shouldPause) {
        this.shouldPause = shouldPause;
    }

    /** Runs on the calling thread until {@link #close()}. */
    public void run() {
        if (!started.compareAndSet(false, true)) throw new IllegalStateException("run() already called or loop closed");
        try {
            consumer.subscribe(settings.topics(), new Rebalance());
            while (!closing) {
                committedThisIteration = false;
                hooks.beforePoll(view);
                applyPauses();
                ConsumerRecords<String, byte[]> records;
                if (batch == null) {
                    records = consumer.poll(settings.pollTimeout());
                } else {
                    awaitBatch(settings.pollTimeout());
                    records = consumer.poll(Duration.ZERO);   // everything paused: keeps membership alive
                }
                health.beat(loopName);
                if (batch == null) {
                    if (!records.isEmpty()) start(records);
                } else {
                    rewind(records);
                    if (batch.done.getCount() == 0) finishBatch();
                }
                if (!committedThisIteration) {   // idle or in-flight iteration: hooks still run every loop
                    hooks.afterCommit(view, Map.of(), consumer.groupMetadata().generationId());
                }
            }
            if (batch != null) {
                awaitBatch(DRAIN_BUDGET);
                finishBatch();                                // commits completed prefixes only
            }
        } finally {
            release();
            stopped.countDown();
        }
    }

    /** SIGTERM path: stop polling, let in-flight groups finish, commit, close; returns within 30 s. */
    @Override
    public void close() {
        closing = true;
        if (started.compareAndSet(false, true)) {   // never ran: nothing in flight
            release();
            stopped.countDown();
            return;
        }
        try {
            if (!stopped.await(30, TimeUnit.SECONDS)) LOG.log(Level.WARNING, loopName + ": did not stop within 30 s");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void start(ConsumerRecords<String, byte[]> records) {
        Batch b = new Batch();
        Map<String, List<Rec>> groups = new LinkedHashMap<>();   // a null key forms one serial group
        for (TopicPartition tp : records.partitions()) {
            List<Rec> recs = new ArrayList<>();
            for (ConsumerRecord<String, byte[]> r : records.records(tp)) {
                Rec rec = new Rec(r);
                recs.add(rec);
                groups.computeIfAbsent(r.key(), k -> new ArrayList<>()).add(rec);
            }
            b.byPartition.put(tp, recs);
        }
        b.done = new CountDownLatch(groups.size());
        batch = b;
        for (List<Rec> group : groups.values()) workers.execute(() -> runGroup(group, b.done));
    }

    private void runGroup(List<Rec> group, CountDownLatch done) {
        try {
            permits.acquire();
            try {
                for (Rec rec : group) {
                    rec.state = process(rec);
                    if (rec.state != State.DONE) return;   // later records of this key wait for redelivery
                }
            } finally {
                permits.release();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();          // shutdown past the drain budget: rest stays PENDING
        } catch (RuntimeException e) {
            LOG.log(Level.ERROR, loopName + ": unexpected group failure", e);
        } finally {
            done.countDown();
        }
    }

    private State process(Rec rec) throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            try {
                if (handler.handle(typed(rec.raw)) == Verdict.HOLD) {
                    metrics.increment("consumer.hold");
                    return State.HELD;
                }
                return State.DONE;
            } catch (PoisonException e) {
                if (deadLetter(rec.raw, e)) return State.DONE;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                LOG.log(Level.WARNING, loopName + ": attempt " + attempt + " failed at " + position(rec.raw), e);
            }
            if (attempt >= ATTEMPTS) {
                metrics.increment("consumer.retry_exhausted");
                return State.FAILED;
            }
            metrics.increment("consumer.retry");
            Thread.sleep(RETRY_BACKOFF_MS << (attempt - 1));
        }
    }

    private ConsumerRecord<String, V> typed(ConsumerRecord<String, byte[]> r) {
        V value;
        try {
            value = valueDeserializer.deserialize(r.topic(), r.headers(), r.value());
        } catch (RuntimeException e) {
            throw new PoisonException("undeserializable value: " + e.getMessage(), e);
        }
        return new ConsumerRecord<>(r.topic(), r.partition(), r.offset(), r.timestamp(), r.timestampType(),
            r.serializedKeySize(), r.serializedValueSize(), r.key(), value, r.headers(), r.leaderEpoch());
    }

    /** True when the record is dead-lettered (or dropped for lack of a DLQ); false to retry. */
    private boolean deadLetter(ConsumerRecord<String, byte[]> r, PoisonException e) {
        if (settings.dlqTopic() == null) {
            metrics.increment("consumer.poison_dropped");
            LOG.log(Level.WARNING, loopName + ": dropped poison record at " + position(r) + ": " + e.getMessage());
            return true;
        }
        ProducerRecord<String, byte[]> out = new ProducerRecord<>(settings.dlqTopic(), r.key(), r.value());
        out.headers().add("error", String.valueOf(e.getMessage()).getBytes(StandardCharsets.UTF_8));
        out.headers().add("source-offset", position(r).getBytes(StandardCharsets.UTF_8));
        try {
            dlqProducer.send(out).get();
            metrics.increment("consumer.poison");
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | RuntimeException ex) {
            LOG.log(Level.WARNING, loopName + ": DLQ produce failed for " + position(r), ex);
            return false;
        }
    }

    private void finishBatch() {
        Batch b = batch;
        batch = null;
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        long now = System.nanoTime();
        b.byPartition.forEach((tp, recs) -> {
            if (b.revoked.contains(tp)) return;
            Rec first = firstUnfinished(recs);
            long commitAt = commitPoint(recs, first);
            commits.put(tp, new OffsetAndMetadata(commitAt));
            if (first == null) {
                failures.remove(tp);
                return;
            }
            consumer.seek(tp, commitAt);   // redeliver from the first held, failed or unstarted record
            if (first.state == State.HELD) {
                notBefore.put(tp, now + settings.pollTimeout().toNanos());
                consumer.pause(List.of(tp));
            } else if (first.state == State.FAILED) {
                int n = failures.merge(tp, 1, Integer::sum);
                long backoffMs = Math.min(MAX_BACKOFF_MS, 1000L << Math.min(n - 1, 15));
                notBefore.put(tp, now + TimeUnit.MILLISECONDS.toNanos(backoffMs));
                consumer.pause(List.of(tp));
            }
        });
        commit(commits);
    }

    private static Rec firstUnfinished(List<Rec> recs) {
        for (Rec r : recs) if (r.state != State.DONE) return r;
        return null;
    }

    private static long commitPoint(List<Rec> recs, Rec firstUnfinished) {
        return firstUnfinished == null ? recs.get(recs.size() - 1).raw.offset() + 1 : firstUnfinished.raw.offset();
    }

    private void commit(Map<TopicPartition, OffsetAndMetadata> commits) {
        if (commits.isEmpty()) return;
        try {
            consumer.commitSync(commits);
        } catch (KafkaException e) {   // lost membership, rebalance, timeout: the records are redelivered
            metrics.increment("consumer.commit_failed");
            LOG.log(Level.WARNING, loopName + ": commit failed for " + commits, e);
            return;
        }
        Map<TopicPartition, Long> offsets = new HashMap<>();
        commits.forEach((tp, o) -> offsets.put(tp, o.offset()));
        committedThisIteration = true;
        hooks.afterCommit(view, Map.copyOf(offsets), consumer.groupMetadata().generationId());
    }

    private void applyPauses() {
        Set<TopicPartition> assigned = consumer.assignment();
        if (assigned.isEmpty()) return;
        long now = System.nanoTime();
        List<TopicPartition> pause = new ArrayList<>();
        List<TopicPartition> resume = new ArrayList<>();
        for (TopicPartition tp : assigned) {
            Long until = notBefore.get(tp);
            boolean waiting = until != null && until - now > 0;
            if (until != null && !waiting) notBefore.remove(tp);
            boolean wanted = pauseWanted(tp);
            (batch != null || waiting || wanted ? pause : resume).add(tp);
        }
        consumer.pause(pause);
        consumer.resume(resume);
    }

    private boolean pauseWanted(TopicPartition tp) {
        try {
            return shouldPause.test(tp);
        } catch (RuntimeException e) {   // an unreadable gate is a closed gate
            metrics.increment("consumer.pause_check_failed");
            LOG.log(Level.WARNING, loopName + ": pause check failed for " + tp, e);
            return true;
        }
    }

    private void awaitBatch(Duration timeout) {
        try {
            batch.done.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            closing = true;   // an interrupted poll thread shuts the loop down through the normal drain
        }
    }

    /** Records polled while a batch is in flight (a partition assigned mid-batch) are fetched again later. */
    private void rewind(ConsumerRecords<String, byte[]> records) {
        for (TopicPartition tp : records.partitions()) consumer.seek(tp, records.records(tp).get(0).offset());
    }

    private void release() {
        workers.shutdownNow();
        try {
            consumer.close(CloseOptions.timeout(CLOSE_TIMEOUT));
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, loopName + ": consumer close failed", e);
        }
    }

    private void forget(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            notBefore.remove(tp);
            failures.remove(tp);
        }
    }

    private static String position(ConsumerRecord<String, ?> r) {
        return new TopicPartition(r.topic(), r.partition()) + "@" + r.offset();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <V> Consumer<String, V> viewOf(Consumer<String, byte[]> consumer) {
        return (Consumer) consumer;   // hooks never read values, so the value type is irrelevant to them
    }

    /** Runs on the poll thread, inside poll(). */
    private final class Rebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            forget(partitions);
            Batch b = batch;
            if (b == null) return;   // nothing in flight: the last batch end already committed
            Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
            for (TopicPartition tp : partitions) {
                List<Rec> recs = b.byPartition.get(tp);
                if (recs == null || !b.revoked.add(tp)) continue;
                commits.put(tp, new OffsetAndMetadata(commitPoint(recs, firstUnfinished(recs))));
            }
            commit(commits);   // completed prefixes only; still-running records are redelivered to the new owner
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            forget(partitions);
            if (batch != null) batch.revoked.addAll(partitions);   // no longer ours: never commit them
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (batch != null && !partitions.isEmpty()) consumer.pause(partitions);
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.app.BatchConsumerLoopTest' --console=plain`
Expected: `BUILD SUCCESSFUL`; 9 tests PASSED (with Docker running; without Docker the class reports SKIPPED). No `jdk.tracePinnedThreads` report.

- [ ] **Step 5: Confirm the unit suite is still green without Docker**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/app/PoisonException.java \
        src/main/java/com/quince/cartrecovery/app/BatchConsumerLoop.java \
        src/integrationTest/java/com/quince/cartrecovery/app/BatchConsumerLoopTest.java
git commit -F - <<'EOF'
Add BatchConsumerLoop: per-key virtual-thread groups, commit below held or unfinished offsets

Pauses, seeks back and commits below HOLD verdicts; dead-letters poison records with their
original bytes; retries transient failures in process then backs off per partition; commits
only completed prefixes on revoke and shutdown.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task C0c: Dockerfile, docker-compose.yml, demo.env

**Model:** haiku (master §2). Every file's full content is below; copy it exactly.

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`
- Create: `docker-compose.yml`
- Create: `demo.env`
- Test: `src/test/java/com/quince/cartrecovery/app/DemoEnvTest.java`

**Interfaces:**
- Consumes: `InfraConfig.fromEnv(Map<String, String>)` (C0a); the Gradle `installDist` launcher `build/install/abandoned-cart-recovery/bin/abandoned-cart-recovery` (from the `application` plugin and `rootProject.name = "abandoned-cart-recovery"`).
- Produces:
  - Image `cart-recovery:local`, entrypoint the launcher; compose `command` passes `--role=<name>`, so `docker compose run --rm dispatcher --role=replay` works.
  - Services `kafka` (in-network `kafka:9092`, host `127.0.0.1:29092`), `redis` (`redis:6379`, host `127.0.0.1:6379`), `dynamodb` (`http://dynamodb:8000`, host `127.0.0.1:8000`), `init`, `detector` ×2, `scheduler` ×2, `dispatcher` ×2, `reconciler` ×1, `loadgen` (profile `load`; env `RATE` default 5000, `DURATION` default `PT5M`, optional `RUN_PREFIX`; reports mounted at `./build/reports/load`). This task owns the whole file, including the `loadgen` service; D1 does not edit it.
  - Every role reads `HEALTH_PORT=8081` and is health-checked with `wget -qO- http://localhost:8081/health` (the runtime image ships `wget`, so no `--role=healthcheck` mode is needed).
  - `demo.env` for `docker compose --env-file demo.env up -d --build`.

The roles do not exist yet: this task only builds the image and validates compose syntax. It must not start the stack.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/quince/cartrecovery/app/DemoEnvTest.java`:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DemoEnvTest {
    @Test
    void demoEnvIsAValidConfigurationWithShortTimings() throws Exception {
        Map<String, String> env = new HashMap<>();
        for (String line : Files.readAllLines(Path.of("demo.env"))) {
            String l = line.strip();
            if (l.isEmpty() || l.startsWith("#")) continue;
            int eq = l.indexOf('=');
            env.put(l.substring(0, eq), l.substring(eq + 1));
        }

        InfraConfig c = InfraConfig.fromEnv(env);

        assertEquals(Duration.ofSeconds(30), c.recovery().window());
        assertEquals(List.of(Duration.ofSeconds(30), Duration.ofSeconds(60), Duration.ofSeconds(120)),
            c.recovery().offsets());
        assertEquals(3, c.recovery().latenessBounds().size());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.DemoEnvTest' --console=plain`
Expected: FAIL with `java.nio.file.NoSuchFileException: demo.env`.

- [ ] **Step 3: Create `demo.env`**

```
# Short timings for a visible local demo:
#   docker compose --env-file demo.env up -d --build
# First offset must be >= WINDOW; one lateness bound per offset, each well above local jitter.
WINDOW=PT30S
OFFSETS=PT30S,PT60S,PT120S
LATENESS_BOUNDS=PT20S,PT20S,PT30S
RETRY_BASE=PT2S
RECONCILE_INTERVAL=PT30S
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.DemoEnvTest' --console=plain`
Expected: `BUILD SUCCESSFUL`; 1 test PASSED.

- [ ] **Step 5: Create `.dockerignore` and `Dockerfile`**

`.dockerignore`:

```
.git
.gradle
.idea
.claude
build
out
docs
*.pdf
```

`Dockerfile`:

```dockerfile
# syntax=docker/dockerfile:1.7

# Build stage: the Gradle image's own gradle 8.10.2 (same as the wrapper), dependency cache in a BuildKit mount.
FROM gradle:8.10.2-jdk21 AS build
ENV GRADLE_USER_HOME=/gradle-cache
WORKDIR /src
COPY settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
COPY src ./src
RUN --mount=type=cache,target=/gradle-cache \
    gradle --no-daemon --console=plain installDist -x test

# Runtime stage: JRE only. The image ships wget, which the compose healthcheck uses.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/build/install/abandoned-cart-recovery/ /app/
ENTRYPOINT ["/app/bin/abandoned-cart-recovery"]
```

- [ ] **Step 6: Build the image and check the entrypoint**

Run: `docker build -t cart-recovery:local .`
Expected: ends with `naming to docker.io/library/cart-recovery:local` and exit code 0.

Run: `docker run --rm cart-recovery:local | head -1`
Expected: the in-memory demo's first line, starting with `virtual start` (the default mode stays the in-memory demo).

Run: `docker run --rm --entrypoint sh cart-recovery:local -c 'command -v wget'`
Expected: `/usr/bin/wget`.

- [ ] **Step 7: Create `docker-compose.yml`**

```yaml
name: cart-recovery

# Shared by every role (spec §7.1, §7.3). Values interpolate from the shell or --env-file demo.env.
x-recovery-env: &recovery-env
  WINDOW: ${WINDOW:-PT30M}
  OFFSETS: ${OFFSETS:-PT30M,PT1H,PT24H}
  LATENESS_BOUNDS: ${LATENESS_BOUNDS:-PT5M,PT5M,PT30M}
  FREQUENCY_CAP: ${FREQUENCY_CAP:-3}
  FREQUENCY_WINDOW: ${FREQUENCY_WINDOW:-P7D}
  HOLDOUT_PERCENT: ${HOLDOUT_PERCENT:-10}
  MAX_SEND_ATTEMPTS: ${MAX_SEND_ATTEMPTS:-5}
  RETRY_BASE: ${RETRY_BASE:-PT1M}
  FAST_OFFSETS: ${FAST_OFFSETS:-2}
  KAFKA_BOOTSTRAP: kafka:9092
  REDIS_URL: redis://redis:6379
  DYNAMO_ENDPOINT: http://dynamodb:8000
  AWS_REGION: us-east-1
  AWS_ACCESS_KEY_ID: local
  AWS_SECRET_ACCESS_KEY: local
  SHARDS: ${SHARDS:-8}
  PARTITIONS: ${PARTITIONS:-8}
  REPLICATION_FACTOR: "1"
  MIN_INSYNC_REPLICAS: "1"
  LEASE: ${LEASE:-PT90S}
  GATEWAY_TIMEOUT: ${GATEWAY_TIMEOUT:-PT30S}
  CLOCK_SKEW: ${CLOCK_SKEW:-PT5S}
  MAX_SEND_RATE: ${MAX_SEND_RATE:-1000}
  FAST_RESERVE: ${FAST_RESERVE:-0.3}
  SEND_FAILURE_RATE: ${SEND_FAILURE_RATE:-0}
  RECONCILE_INTERVAL: ${RECONCILE_INTERVAL:-PT5M}
  RETRY_POLL: ${RETRY_POLL:-PT1S}
  MAX_IN_FLIGHT: ${MAX_IN_FLIGHT:-256}
  HEALTH_PORT: "8081"
  JAVA_TOOL_OPTIONS: -Xmx256m -XX:+UseSerialGC

x-app: &app
  build: .
  image: cart-recovery:local
  environment: *recovery-env
  stop_grace_period: 40s

# Long-running roles: no published ports; health via the JDK server inside the container.
x-role: &role
  <<: *app
  restart: unless-stopped
  depends_on:
    init:
      condition: service_completed_successfully
  healthcheck:
    test: ["CMD", "wget", "-qO-", "http://localhost:8081/health"]
    interval: 10s
    timeout: 3s
    retries: 3
    start_period: 30s

services:
  kafka:
    image: apache/kafka:4.3.1
    environment:
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,HOST://:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    ports:
      - "127.0.0.1:29092:29092"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 30
      start_period: 15s

  redis:
    image: redis:7.4-alpine
    command: ["redis-server", "--appendonly", "yes"]
    volumes:
      - redis-data:/data
    ports:
      - "127.0.0.1:6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 20

  # -sharedDb on a named volume by default; for load runs:
  #   DYNAMO_STORAGE=-inMemory docker compose --profile load up -d
  dynamodb:
    image: amazon/dynamodb-local:3.3.1
    user: root   # the image user cannot write a fresh named volume
    command: -jar DynamoDBLocal.jar -sharedDb ${DYNAMO_STORAGE:--dbPath /home/dynamodblocal/data}
    volumes:
      - dynamodb-data:/home/dynamodblocal/data
    ports:
      - "127.0.0.1:8000:8000"

  init:
    <<: *app
    command: ["--role=init"]
    restart: "no"
    depends_on:
      kafka:
        condition: service_healthy
      redis:
        condition: service_healthy
      dynamodb:
        condition: service_started

  detector:
    <<: *role
    command: ["--role=detector"]
    deploy:
      replicas: 2

  scheduler:
    <<: *role
    command: ["--role=scheduler"]
    deploy:
      replicas: 2

  dispatcher:
    <<: *role
    command: ["--role=dispatcher"]
    deploy:
      replicas: 2

  reconciler:
    <<: *role
    command: ["--role=reconciler"]
    deploy:
      replicas: 1

  # Load runs: docker compose --profile load run --rm -e RATE=50 -e DURATION=PT60S loadgen
  # (LoadgenRole takes no CLI args; it reads RATE, DURATION and optional RUN_PREFIX from the environment.)
  loadgen:
    <<: *app
    environment:
      <<: *recovery-env
      RATE: ${RATE:-5000}
      DURATION: ${DURATION:-PT5M}
    command: ["--role=loadgen"]
    profiles: ["load"]
    restart: "no"
    depends_on:
      init:
        condition: service_completed_successfully
    volumes:
      - ./build/reports/load:/app/build/reports/load

volumes:
  redis-data:
  dynamodb-data:
```

- [ ] **Step 8: Validate compose syntax (does not start anything)**

Run: `docker compose config -q && echo valid`
Expected: `valid` and nothing else.

Run: `docker compose config --services | sort | tr '\n' ' '`
Expected: `detector dispatcher dynamodb init kafka reconciler redis scheduler ` (no `loadgen` outside its profile).

Run: `docker compose --profile load config --services | grep -x loadgen`
Expected: `loadgen`.

Run: `docker compose --env-file demo.env config | grep -oE '\b(WINDOW|OFFSETS): .*' | sort -u`
Expected:
```
OFFSETS: PT30S,PT60S,PT120S
WINDOW: PT30S
```

Run: `DYNAMO_STORAGE=-inMemory docker compose --profile load config | grep -c -- '-inMemory'` and `docker compose config | grep -c -- '-dbPath'`
Expected: `1` for each.

Run: `docker compose config | grep -E 'published|host_ip'`
Expected: only `host_ip: 127.0.0.1` lines and `published: "29092"`, `"6379"`, `"8000"`; no role service has a `ports` entry.

- [ ] **Step 9: Run the unit suite**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 10: Commit**

```bash
git add Dockerfile .dockerignore docker-compose.yml demo.env \
        src/test/java/com/quince/cartrecovery/app/DemoEnvTest.java
git commit -F - <<'EOF'
Add multi-stage Dockerfile, docker-compose stack and demo.env

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
```

---

## Notes for thread C1

- `Main` (C1c) catches `ConfigException`, prints `invalid configuration: <message>` on one line to stderr, and exits with status 2; it logs `config hash <hash()>` at startup.
- Pass `maxSilence` = 3 × the loop's poll interval (spec §7.4) to `HealthServer`; `Health` registers a loop on its first beat, so non-consumer loops (scheduler claim loop, retry loop, reconciler) must call `beat` every iteration, including backoff and pause.
- A HOLD partition resumes only when `pauseWhile` returns false for it, and at most once per `pollTimeout`. The dispatcher's predicate must therefore cover every reason it holds: empty bucket (`!anyAvailable()` for fast, `!slowAllowed()` for slow), breaker `isOpen()`, the guardrail switch, and the lagging `srcPartition`s recorded for held records on that intent partition.
- `afterCommit` offsets are next-to-consume offsets; the detector compares them with its end-offset snapshots. It also fires from inside `onPartitionsRevoked` with the old generation, and once per iteration that committed nothing (empty map), so idle partitions are refreshed every poll loop.
- Hooks get a `Consumer<String, V>` view of a byte-valued consumer; use only `assignment`, `position`, `committed`, `endOffsets`, `groupMetadata`, never `poll`.
- `BatchConsumerLoop` defaults `auto.offset.reset=earliest`; set it explicitly if a role needs `latest`.
- When a breaker is open, `CircuitBreaker.send` returns `TRANSIENT_FAILURE` without calling the sink, so a few in-flight dispatches during half-open become ordinary retries.

## Self-review

1. **Spec coverage.** §5.1 consumer settings: classic protocol, manual commits, commit on revoke and shutdown (C0b). §6.3: per-batch grouping by key on virtual threads, in-order within a group, commit at the lowest held or unfinished offset, `MAX_IN_FLIGHT` bound, deterministic → DLQ + commit, transient → in-process retry then seek back with backoff capped at 30 s, poll thread keeps beating, shutdown within 30 s (C0b); bucket priority and per-lane pausing signals (C0a `TokenBucket`); thread safety of bucket, breaker, and `Metrics` (C0a, A1). §6.2 breaker: 100/50%/30 s are C1's constructor arguments; mechanism in C0a. §7.1: every variable parsed, `LEASE ≥ 3 × GATEWAY_TIMEOUT`, config hash (C0a); the `recovery-meta` and topic-count startup checks are C1c's. §7.2 and §7.3 (C0c), §7.4 `/health` `/ready` `/metrics` (C0a); the 10 s metrics log line is C1's.
2. **Placeholders.** None: every code step has complete code, every run step an exact command and expected result.
3. **Type consistency.** Names and signatures match master §1.5; tests use only public members plus the package-private `Health(LongSupplier)` and `stale`.
4. **Review Focus.** Item 4 is pinned by `revokeMidFlightCommitsOnlyTheCompletedPrefix`; item 5 by `rejectsMalformedValuesWithOneLineNamingTheVariable` (`WINDOW=30m`, `SHARDS=abc`). Also covered: a paused partition is not re-polled (`holds == 1`), undeserializable bytes reach the DLQ unchanged, and a pause check that throws counts as paused.
