# Thread B: Infra Adapters (Redis, DynamoDB, Kafka)

> Part of the master plan `docs/superpowers/plans/2026-09-26-production-infra.md`. Steps use checkbox (`- [ ]`) syntax for tracking. Executors read the spec sections each task cites: `docs/superpowers/specs/2026-09-25-production-infra-design.md` (§4, §5.1 to §5.3, §5.5, §6.1).

**Goal:** Implement the frozen ports from master §1.2 on real infrastructure (Redis Lua scripts, DynamoDB conditional writes, Kafka producers) and prove parity with the in-memory adapters through thread A's shared contract tests.

**Tasks:**

| Task | Scope | Depends on | Model |
|---|---|---|---|
| B0 | Redis Lua scripts `upsert`, `claim`, `release`, `ack`, `remove`, `wmSet`, `wmGet` as resources, tested with raw Lettuce | T0 | opus |
| B1 | `DynamoTables`, `DynamoCartStateStore`, `DynamoSendLedger`, `RecoveryMetaStore`, contract subclasses | A1 | opus |
| B2 | `RedisScripts` (EVALSHA + NOSCRIPT fallback), `RedisTimerStore`, `RedisWatermark`, `RedisMeta`, contract subclasses | A1, B0 | sonnet |
| B3 | `JsonCodec`, `Topics`, `KafkaClients`, `TopicAdmin`, `KafkaIntentPublisher`, `KafkaOutcomeRecorder`, `KafkaDeadLetterQueue`, `KafkaRecordingSink` | A1 | sonnet |

**File ownership (no other task writes these files):**

- B0: `src/main/resources/redis/{upsert,claim,release,ack,remove,wmSet,wmGet}.lua`; `src/integrationTest/java/com/quince/cartrecovery/infra/redis/TestRedis.java`; `src/integrationTest/java/com/quince/cartrecovery/infra/redis/LuaScriptsTest.java`
- B1: `src/main/java/com/quince/cartrecovery/infra/dynamo/{Attrs,DynamoTables,DynamoCartStateStore,DynamoSendLedger,RecoveryMetaStore}.java`; `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/{TestDynamo,DynamoTablesTest,DynamoCartStateStoreTest,DynamoSendLedgerTest,RecoveryMetaStoreTest,DynamoCartStateStoreContractTest,DynamoSendLedgerContractTest}.java`
- B2: `src/main/java/com/quince/cartrecovery/infra/redis/{RedisScripts,RedisTimerStore,RedisWatermark,RedisMeta}.java`; `src/integrationTest/java/com/quince/cartrecovery/infra/redis/{RedisTimerStoreTest,RedisWatermarkTest,RedisMetaTest,RedisTimerStoreContractTest,RedisWatermarkContractTest}.java` (reads B0's `TestRedis` and scripts, does not modify them)
- B3: `src/main/java/com/quince/cartrecovery/infra/kafka/{JsonCodec,Topics,KafkaClients,TopicAdmin,KafkaIntentPublisher,KafkaOutcomeRecorder,KafkaDeadLetterQueue,KafkaRecordingSink}.java`; `src/test/java/com/quince/cartrecovery/infra/kafka/{JsonCodecTest,KafkaClientsTest}.java`; `src/integrationTest/java/com/quince/cartrecovery/infra/kafka/{TestKafka,KafkaAdaptersTest,TopicAdminTest}.java`

B1, B2 and B3 run in parallel in wave W2 and touch disjoint files. No task in this thread edits `build.gradle.kts` (owned by T0).

**Binding note:** master §1 (frozen contracts) is binding. Code against master §1, never against the current `model`/`ports` files, which A1 reshapes in parallel (for example `Timer` gains `srcPartition`, `CartStateStore.put` becomes `applyEvent`, `SendLedger` becomes the fenced claim API). If a frozen contract cannot work, stop and report `BLOCKED` with the reason; the frictions found while planning are resolved under "Contract issues" at the end of this file.

**Global constraints (from master):** Java 21; package root `com.quince.cartrecovery`, new packages `infra.redis`, `infra.dynamo`, `infra.kafka`; nothing in `core` imports these; infra tests live in `src/integrationTest` with `@Testcontainers(disabledWithoutDocker = true)`; `./gradlew test` needs only a JDK and stays green after every task; every payload carries `schemaVersion` 1 and Jackson ignores unknown properties; ledger key `cartId:version:offsetIndex` parsed from the right; every commit ends with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.

**Command prefix:** every Gradle command runs from the repo root as `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew ...`.

## Dependencies (from T0)

T0's pinned set applies (controller ruling R2): `kafka-clients` 4.3.1, `lettuce-core` 6.7.1.RELEASE, AWS SDK v2 BOM 2.54.17 (`dynamodb`, `apache-client`), Jackson BOM 2.19.1 (`jackson-databind`; wire records carry epoch millis as `long`, so `jackson-datatype-jsr310` is not needed here), Testcontainers BOM 1.21.2 (`junit-jupiter`, `kafka`, which bring in `testcontainers`), JUnit BOM 5.10.2. Container images (master Global Constraints): `redis:7.4-alpine`, `amazon/dynamodb-local:3.3.1`, `apache/kafka:4.3.1`.

T0's `integrationTest` source set sees `main` output (including `src/main/resources/redis/*.lua`) and `test` output (thread A's contract classes), runs JUnit 5 test classes sequentially in one JVM (tests share containers and some call `FLUSHALL`), and `check` depends on it.

APIs used: Lettuce 6.x `RedisCommands.eval/evalsha(String, ScriptOutputType, K[], V...)`, `scriptLoad(String)`, `RedisNoScriptException`, async `hmget` futures (commands issued without awaiting are pipelined on one connection; `setAutoFlushCommands(false)` is not used because the connection is shared across threads). AWS SDK v2 `DynamoDbClient.builder().httpClientBuilder(ApacheHttpClient.builder().maxConnections(n)).endpointOverride(uri)`, `UpdateItemRequest.updateExpression/conditionExpression/returnValues(ReturnValue.ALL_NEW)`, `ConditionalCheckFailedException`, `BatchGetItemRequest` with `KeysAndAttributes.consistentRead(true)` and `unprocessedKeys()`, `GlobalSecondaryIndex` with `Projection.projectionType(INCLUDE).nonKeyAttributes(...)`, `queryPaginator(...).items().stream()`, `DynamoDbWaiter.waitUntilTableExists`. Testcontainers 1.21: `org.testcontainers.kafka.KafkaContainer` for `apache/kafka` (KRaft), `org.testcontainers.containers.GenericContainer`, `org.testcontainers.junit.jupiter.Testcontainers`. Kafka producer: `acks=all`, `enable.idempotence=true`.

## Contract tests (thread A's actual signatures, controller ruling R3)

Thread A creates the abstract contracts in `src/test/java/com/quince/cartrecovery/contract/`; the `*ContractTest` subclasses here override exactly these members:

| Contract | Abstract members | Infra override |
|---|---|---|
| `CartStateStoreContract` | `protected abstract CartStateStore newStore(RecoveryConfig config, int shards)` | fresh table per call (no time hook: the port takes time as arguments) |
| `SendLedgerContract` | `protected abstract SendLedger newLedger(Duration lease, int shards)` | fresh table per call (no time hook) |
| `TimerStoreContract` | `protected abstract TimerStore newStore(Duration lease)`; `protected abstract Instant now()`; `protected abstract void advance(Duration d)` | `FLUSHALL` then 8 shards (`TimerStoreContract.SHARDS`); `now()` is Redis `TIME`; `advance` sleeps |
| `WatermarkContract` | `protected abstract Watermark newWatermark()`; `protected abstract void advance(Duration d)` | `FLUSHALL` then `RedisWatermark` over 3 partitions (the contract writes partitions 0 to 2); `advance` sleeps |

Every factory returns an empty store: no other test's timers, watermarks, rows or index entries. Millisecond-exact time boundaries run only in thread A's in-memory subclasses; the subclasses here add no time-boundary tests of their own (the DynamoDB ports take `now` as an argument, so B1's own tests pin the ledger boundaries exactly).

## Review Focus pins owned by this thread

- Master Review Focus 1 (cart id with `:` or `|`): B0 `LuaScriptsTest.cartIdsWithDelimitersAreHashFieldsAndMembersOnly`; B2 `RedisTimerStoreTest.packedValueNeverContainsTheCartId` and `cartIdWithDelimitersRoundTripsThroughClaimAndAck`; B1 `DynamoSendLedgerTest.retryDueExactlyAtNextAttemptAt` (cart id `a:b|c`).
- Master Review Focus 2 (no `firstName`, empty items): B1 `DynamoCartStateStoreTest.absentFirstNameAndEmptyItemsRoundTrip`; B3 `JsonCodecTest.absentFirstNameAndEmptyItemsStayAbsentAndEmpty` and `KafkaAdaptersTest.sinkRecordsNoNameAndNoItems`. DynamoDB accepts empty strings in non-key attributes (DynamoDB Local 3.x too), so `""` is stored as `""`; only `null` needs handling: the store never writes a null attribute (an edit without a name leaves the stored one, controller rulings R4 and R5) and reads a missing attribute back as `null`. Neither `firstName` nor item fields are index keys.
- Master Review Focus 3 (inclusive boundaries): B1 `DynamoSendLedgerTest.takeoverAllowedExactlyAtLeaseUntil`, `retryDueExactlyAtNextAttemptAt`, `sendingRowIsDueForTakeoverAtLeaseUntil`, and `DynamoCartStateStoreTest.openIndexUsesShardAndHourRoundedOpenUntil` (`openUntil >= now` inclusive).

---

### Task B0: Redis Lua scripts

**Model:** opus (atomicity and ordering in Lua; packed-value format).

**Spec:** §5.2 (binding), §3 rows "While a timer entry exists, it never regresses" and "Watermark safety".

**Files:**
- Create: `src/integrationTest/java/com/quince/cartrecovery/infra/redis/TestRedis.java`
- Create: `src/integrationTest/java/com/quince/cartrecovery/infra/redis/LuaScriptsTest.java`
- Create: `src/main/resources/redis/upsert.lua`, `claim.lua`, `release.lua`, `ack.lua`, `remove.lua`, `wmSet.lua`, `wmGet.lua`

**Interfaces:**
- Consumes: T0's `integrationTest` source set and the Lettuce and Testcontainers dependencies. Nothing from thread A.
- Produces (script contracts that B2 calls by resource name `/redis/<name>.lua`; all times epoch milliseconds from Redis `TIME`):
  - Keys: `timers:{s}` (sorted set, member cartId, score due or lease-expiry millis), `timerdata:{s}` (hash, field cartId, value packed), `watermarks` (hash, field partition, value `generation|eventTime|updatedAt`).
  - Packed timer value: `kind|version|offsetIndex|srcPartition|dueAtMillis`, `kind` is `CHECK_ABANDON` or `REMINDER`, `version` a non-negative decimal without leading zeros, `offsetIndex` `-1` for `CHECK_ABANDON`. The cart id is never inside the packed value; it is only the hash field and the sorted-set member, so a cart id may contain `:` or `|`.
  - `upsert`: KEYS `[timers:{s}, timerdata:{s}]`, ARGV `[cartId, packed, version, offsetIndex, dueAtMillis]` → integer 1 written, 0 not (stored `(version, offsetIndex)` greater or equal; equal leaves the score, so a lease is kept).
  - `claim`: KEYS `[timers:{s}, timerdata:{s}]`, ARGV `[n, leaseMs]` → flat list `[cartId1, packed1, cartId2, packed2, ...]` of members with score ≤ `TIME`, lowest score first, each re-scored to `TIME + leaseMs`.
  - `release`: KEYS as above, ARGV `[cartId, packed, delayMs]` → 1 if the stored value equals `packed` (score set to `TIME + delayMs`), else 0.
  - `ack`: KEYS as above, ARGV `[cartId, packed]` → 1 if the stored value equals `packed` (removed from both keys), else 0.
  - `remove`: KEYS as above, ARGV `[cartId, version]` → 1 if the stored version ≤ `version` (removed), else 0.
  - `wmSet`: KEYS `[watermarks]`, ARGV `[partition, generation, eventTimeMillis]` → 0 if `generation` < stored, else 1 after storing `generation|max-within-generation eventTime|TIME`.
  - `wmGet`: KEYS `[watermarks]`, ARGV `[staleAfterMs, p1, p2, ...]` → `[minEventTimeMillis, nowMillis]` as strings, where any partition that is missing or has `now − updatedAt > staleAfterMs` counts as `0`.

- [ ] **Step 1: Write the shared Redis test container helper**

Create `src/integrationTest/java/com/quince/cartrecovery/infra/redis/TestRedis.java`:

```java
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
```

- [ ] **Step 2: Write the failing script tests**

Create `src/integrationTest/java/com/quince/cartrecovery/infra/redis/LuaScriptsTest.java`:

```java
package com.quince.cartrecovery.infra.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class LuaScriptsTest {
    private static final String Z = "timers:{0}";
    private static final String H = "timerdata:{0}";
    private static final String WM = "watermarks";

    private RedisCommands<String, String> redis;

    @BeforeEach
    void setUp() {
        redis = TestRedis.sync();
        redis.flushall();
    }

    private static String lua(String name) {
        try (InputStream in = LuaScriptsTest.class.getResourceAsStream("/redis/" + name + ".lua")) {
            if (in == null) throw new IllegalStateException("missing /redis/" + name + ".lua");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String packed(String kind, long version, int offset, int src, long dueAt) {
        return kind + "|" + version + "|" + offset + "|" + src + "|" + dueAt;
    }

    private long upsert(String id, String kind, long version, int offset, int src, long dueAt) {
        Long r = redis.eval(lua("upsert"), ScriptOutputType.INTEGER, new String[] {Z, H},
                id, packed(kind, version, offset, src, dueAt), Long.toString(version), Integer.toString(offset), Long.toString(dueAt));
        return r;
    }

    private List<Object> claim(int n, long leaseMs) {
        return redis.eval(lua("claim"), ScriptOutputType.MULTI, new String[] {Z, H}, Integer.toString(n), Long.toString(leaseMs));
    }

    private long release(String id, String data, long delayMs) {
        Long r = redis.eval(lua("release"), ScriptOutputType.INTEGER, new String[] {Z, H}, id, data, Long.toString(delayMs));
        return r;
    }

    private long ack(String id, String data) {
        Long r = redis.eval(lua("ack"), ScriptOutputType.INTEGER, new String[] {Z, H}, id, data);
        return r;
    }

    private long remove(String id, long version) {
        Long r = redis.eval(lua("remove"), ScriptOutputType.INTEGER, new String[] {Z, H}, id, Long.toString(version));
        return r;
    }

    private long wmSet(int partition, long generation, long eventTime) {
        Long r = redis.eval(lua("wmSet"), ScriptOutputType.INTEGER, new String[] {WM},
                Integer.toString(partition), Long.toString(generation), Long.toString(eventTime));
        return r;
    }

    private List<Object> wmGet(long staleMs, int... partitions) {
        List<String> args = new ArrayList<>();
        args.add(Long.toString(staleMs));
        for (int p : partitions) args.add(Integer.toString(p));
        return redis.eval(lua("wmGet"), ScriptOutputType.MULTI, new String[] {WM}, args.toArray(String[]::new));
    }

    private long redisNow() { return TestRedis.now().toEpochMilli(); }

    @Test
    void upsertWritesDataAndScore() {
        assertEquals(1, upsert("c1", "CHECK_ABANDON", 1, -1, 3, 5_000));
        assertEquals("CHECK_ABANDON|1|-1|3|5000", redis.hget(H, "c1"));
        assertEquals(5_000.0, redis.zscore(Z, "c1"));
    }

    @Test
    void upsertIsMonotonicWithCheckAbandonBelowReminderZero() {
        assertEquals(1, upsert("c1", "REMINDER", 5, 1, 0, 1_000));
        assertEquals(0, upsert("c1", "REMINDER", 4, 2, 0, 2_000), "older version");
        assertEquals(0, upsert("c1", "REMINDER", 5, 0, 0, 2_000), "same version, lower offset");
        assertEquals(0, upsert("c1", "CHECK_ABANDON", 5, -1, 0, 2_000), "CHECK_ABANDON counts as -1");
        assertEquals(1, upsert("c1", "REMINDER", 5, 2, 0, 3_000));
        assertEquals(1, upsert("c1", "CHECK_ABANDON", 6, -1, 0, 4_000), "newer version beats any offset");
        assertEquals("CHECK_ABANDON|6|-1|0|4000", redis.hget(H, "c1"));
        assertEquals(4_000.0, redis.zscore(Z, "c1"));

        assertEquals(1, upsert("c2", "CHECK_ABANDON", 1, -1, 0, 1_000));
        assertEquals(1, upsert("c2", "REMINDER", 1, 0, 0, 2_000), "REMINDER 0 is greater than CHECK_ABANDON");
    }

    @Test
    void upsertComparesVersionsExactlyBeyondDoublePrecision() {
        // 2^53 and 2^53 + 1 are the same Lua double; the script compares decimal strings instead.
        assertEquals(1, upsert("c1", "CHECK_ABANDON", 9_007_199_254_740_992L, -1, 0, 1_000));
        assertEquals(1, upsert("c1", "CHECK_ABANDON", 9_007_199_254_740_993L, -1, 0, 1_000));
        assertEquals(0, upsert("c1", "CHECK_ABANDON", 9_007_199_254_740_992L, -1, 0, 1_000));
        assertEquals(0, upsert("c1", "CHECK_ABANDON", 999_999_999_999_999L, -1, 0, 1_000), "fewer digits is smaller");
        assertEquals(1, upsert("c1", "CHECK_ABANDON", 10_000_000_000_000_000L, -1, 0, 1_000), "more digits is greater");
    }

    @Test
    void equalKeyIsANoOpThatKeepsTheLeaseScore() {
        upsert("c1", "REMINDER", 2, 0, 0, 1_000);
        assertEquals(List.of("c1", "REMINDER|2|0|0|1000"), claim(10, 60_000));
        Double leased = redis.zscore(Z, "c1");
        assertTrue(leased >= redisNow() + 50_000, "claimed member is re-scored to TIME + lease");

        assertEquals(0, upsert("c1", "REMINDER", 2, 0, 0, 1_000), "identical data");
        assertEquals(0, upsert("c1", "REMINDER", 2, 0, 7, 9_000), "same (version, offset), different data");
        assertEquals(leased, redis.zscore(Z, "c1"));
        assertEquals("REMINDER|2|0|0|1000", redis.hget(H, "c1"));
    }

    @Test
    void claimTakesOnlyDueMembersLowestScoreFirstUpToLimit() {
        long now = redisNow();
        upsert("a", "CHECK_ABANDON", 1, -1, 0, 1_000);
        upsert("b", "CHECK_ABANDON", 1, -1, 0, 2_000);
        upsert("c", "CHECK_ABANDON", 1, -1, 0, now + 600_000);

        assertEquals(List.of("a", "CHECK_ABANDON|1|-1|0|1000"), claim(1, 30_000));
        assertEquals(List.of("b", "CHECK_ABANDON|1|-1|0|2000"), claim(10, 30_000), "a is leased, c not due");
        assertEquals(List.of(), claim(10, 30_000));
    }

    @Test
    void claimDropsIndexEntriesWithoutData() {
        redis.zadd(Z, 1, "ghost");
        assertEquals(List.of(), claim(10, 30_000));
        assertNull(redis.zscore(Z, "ghost"));
    }

    @Test
    void releaseReschedulesOnlyIfUnchanged() {
        upsert("c1", "REMINDER", 3, 0, 0, 1_000);
        String data = "REMINDER|3|0|0|1000";
        claim(10, 60_000);
        Double leased = redis.zscore(Z, "c1");

        assertEquals(0, release("c1", "REMINDER|2|0|0|1000", 0));
        assertEquals(leased, redis.zscore(Z, "c1"));

        assertEquals(1, release("c1", data, 0));
        assertEquals(List.of("c1", data), claim(10, 60_000), "due again at once");

        assertEquals(1, release("c1", data, 600_000));
        assertTrue(redis.zscore(Z, "c1") >= redisNow() + 590_000);
        assertEquals(0, release("missing", data, 0));
    }

    @Test
    void ackRemovesOnlyIfUnchanged() {
        upsert("c1", "CHECK_ABANDON", 1, -1, 0, 1_000);
        claim(10, 60_000);
        upsert("c1", "CHECK_ABANDON", 2, -1, 0, 5_000); // newer event arrived while the timer was processed

        assertEquals(0, ack("c1", "CHECK_ABANDON|1|-1|0|1000"));
        assertEquals("CHECK_ABANDON|2|-1|0|5000", redis.hget(H, "c1"));
        assertEquals(5_000.0, redis.zscore(Z, "c1"));

        assertEquals(1, ack("c1", "CHECK_ABANDON|2|-1|0|5000"));
        assertNull(redis.hget(H, "c1"));
        assertNull(redis.zscore(Z, "c1"));
    }

    @Test
    void removeOnlyIfStoredVersionIsNotGreater() {
        upsert("c1", "REMINDER", 5, 1, 0, 1_000);
        assertEquals(0, remove("c1", 4));
        assertEquals("REMINDER|5|1|0|1000", redis.hget(H, "c1"));
        assertEquals(1, remove("c1", 5));
        assertNull(redis.hget(H, "c1"));
        assertNull(redis.zscore(Z, "c1"));

        upsert("c2", "CHECK_ABANDON", 5, -1, 0, 1_000);
        assertEquals(1, remove("c2", 9));
        assertEquals(0, remove("missing", 1));
    }

    @Test
    void cartIdsWithDelimitersAreHashFieldsAndMembersOnly() {
        String id = "shop:42|cart:7";
        assertEquals(1, upsert(id, "REMINDER", 3, 1, 2, 1_000));
        String data = redis.hget(H, id);
        assertEquals("REMINDER|3|1|2|1000", data);
        assertEquals(5, data.split("\\|", -1).length, "packed value holds exactly five fields and no cart id");
        assertEquals(List.of(id, data), claim(10, 30_000));
        assertEquals(1, release(id, data, 0));
        assertEquals(0, upsert(id, "REMINDER", 3, 0, 2, 1_000));
        assertEquals(1, ack(id, data));
        assertNull(redis.zscore(Z, id));
    }

    @Test
    void wmSetFencesOlderGenerationsAndKeepsMaxWithinOne() {
        assertEquals(1, wmSet(0, 5, 1_000));
        assertEquals(1, wmSet(0, 5, 900), "same generation is accepted but keeps the max");
        assertEquals("1000", wmGet(5_000, 0).get(0));

        assertEquals(0, wmSet(0, 4, 9_000), "zombie writer from an older generation");
        assertEquals("1000", wmGet(5_000, 0).get(0));

        assertEquals(1, wmSet(0, 6, 500), "a newer generation overwrites");
        assertEquals("500", wmGet(5_000, 0).get(0));
    }

    @Test
    void wmSetStampsRedisTime() {
        long before = redisNow();
        wmSet(3, 1, 42);
        String[] f = redis.hget(WM, "3").split("\\|");
        assertEquals("1", f[0]);
        assertEquals("42", f[1]);
        long updatedAt = Long.parseLong(f[2]);
        assertTrue(updatedAt >= before && updatedAt <= redisNow());
    }

    @Test
    void wmGetIsZeroWhenMissingOrStaleAndMinOverPartitions() throws InterruptedException {
        wmSet(0, 1, 1_000);
        wmSet(1, 1, 2_000);

        List<Object> r = wmGet(5_000, 0, 1);
        assertEquals("1000", r.get(0));
        assertTrue(Math.abs(Long.parseLong((String) r.get(1)) - redisNow()) < 1_000, "second element is Redis TIME");
        assertEquals("0", wmGet(5_000, 0, 1, 2).get(0), "partition 2 never written");

        Thread.sleep(300);
        assertEquals("0", wmGet(200, 0).get(0), "stale after staleAfterMs without writes");
        wmSet(0, 1, 1_000);
        assertEquals("1000", wmGet(200, 0).get(0), "any write refreshes updatedAt");
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.redis.LuaScriptsTest' --console=plain`
Expected: FAIL, every test with `java.lang.IllegalStateException: missing /redis/upsert.lua` (or the matching script name). If Docker is not running, the class is reported skipped instead; start Docker before continuing.

- [ ] **Step 4: Write the timer scripts**

Create `src/main/resources/redis/upsert.lua`:

```lua
-- KEYS[1] timers:{s}   KEYS[2] timerdata:{s}
-- ARGV[1] cartId  ARGV[2] packed kind|version|offsetIndex|srcPartition|dueAtMillis
-- ARGV[3] version (non-negative decimal, no leading zeros)  ARGV[4] offsetIndex (-1 for CHECK_ABANDON)
-- ARGV[5] dueAtMillis
-- Writes only if (version, offsetIndex) is greater than the stored pair. Equal is a no-op that keeps
-- the score, so a claimed timer keeps its lease. Returns 1 if written, 0 otherwise.
-- Versions compare as decimal strings: Lua numbers are doubles and lose precision above 2^53.
local function cmpver(a, b)
  if #a ~= #b then return (#a < #b) and -1 or 1 end
  if a == b then return 0 end
  return (a < b) and -1 or 1
end

local cur = redis.call('HGET', KEYS[2], ARGV[1])
if cur then
  local v, o = string.match(cur, '^[^|]*|(%d+)|(-?%d+)|')
  local c = cmpver(ARGV[3], v)
  if c < 0 or (c == 0 and tonumber(ARGV[4]) <= tonumber(o)) then return 0 end
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[5], ARGV[1])
return 1
```

Create `src/main/resources/redis/claim.lua`:

```lua
-- KEYS[1] timers:{s}   KEYS[2] timerdata:{s}
-- ARGV[1] max members  ARGV[2] leaseMs
-- Takes up to ARGV[1] members with score <= Redis TIME (lowest first), re-scores each to TIME + lease,
-- and returns a flat list {cartId1, packed1, cartId2, packed2, ...}. Index entries without data are dropped.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local leased = string.format('%.0f', now + tonumber(ARGV[2]))
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', string.format('%.0f', now), 'LIMIT', 0, tonumber(ARGV[1]))
local out = {}
for _, id in ipairs(ids) do
  local data = redis.call('HGET', KEYS[2], id)
  if data then
    redis.call('ZADD', KEYS[1], leased, id)
    out[#out + 1] = id
    out[#out + 1] = data
  else
    redis.call('ZREM', KEYS[1], id)
  end
end
return out
```

Create `src/main/resources/redis/release.lua`:

```lua
-- KEYS[1] timers:{s}   KEYS[2] timerdata:{s}
-- ARGV[1] cartId  ARGV[2] packed value as claimed  ARGV[3] delayMs
-- If the stored value still equals the claimed one, makes it due at Redis TIME + delay. Returns 1 or 0.
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZADD', KEYS[1], string.format('%.0f', now + tonumber(ARGV[3])), ARGV[1])
return 1
```

Create `src/main/resources/redis/ack.lua`:

```lua
-- KEYS[1] timers:{s}   KEYS[2] timerdata:{s}
-- ARGV[1] cartId  ARGV[2] packed value as claimed
-- Removes the timer only if the stored value still equals the claimed one. Returns 1 or 0.
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
```

Create `src/main/resources/redis/remove.lua`:

```lua
-- KEYS[1] timers:{s}   KEYS[2] timerdata:{s}
-- ARGV[1] cartId  ARGV[2] version (non-negative decimal)
-- Removes the timer only if its stored version is <= ARGV[2]. Returns 1 or 0.
local function cmpver(a, b)
  if #a ~= #b then return (#a < #b) and -1 or 1 end
  if a == b then return 0 end
  return (a < b) and -1 or 1
end

local cur = redis.call('HGET', KEYS[2], ARGV[1])
if not cur then return 0 end
local v = string.match(cur, '^[^|]*|(%d+)|')
if cmpver(v, ARGV[2]) > 0 then return 0 end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
```

- [ ] **Step 5: Write the watermark scripts**

Create `src/main/resources/redis/wmSet.lua`:

```lua
-- KEYS[1] watermarks
-- ARGV[1] partition  ARGV[2] generation (consumer group generation id)  ARGV[3] eventTimeMillis
-- Rejects a lower generation than stored. The same generation keeps max(stored, new). A higher
-- generation overwrites. updatedAt is Redis TIME. Returns 1 if stored, 0 if rejected.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local gen = tonumber(ARGV[2])
local et = tonumber(ARGV[3])
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur then
  local g, e = string.match(cur, '^(-?%d+)|(-?%d+)|')
  g = tonumber(g)
  e = tonumber(e)
  if gen < g then return 0 end
  if gen == g and e > et then et = e end
end
redis.call('HSET', KEYS[1], ARGV[1], string.format('%.0f|%.0f|%.0f', gen, et, now))
return 1
```

Create `src/main/resources/redis/wmGet.lua`:

```lua
-- KEYS[1] watermarks
-- ARGV[1] staleAfterMs  ARGV[2..n] partitions
-- Returns {minEventTimeMillis, nowMillis} as strings. A partition that is missing, or whose
-- updatedAt is more than staleAfterMs before Redis TIME, counts as 0.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local stale = tonumber(ARGV[1])
local min = nil
for i = 2, #ARGV do
  local et = 0
  local cur = redis.call('HGET', KEYS[1], ARGV[i])
  if cur then
    local e, u = string.match(cur, '^-?%d+|(-?%d+)|(-?%d+)$')
    if now - tonumber(u) <= stale then et = tonumber(e) end
  end
  if min == nil or et < min then min = et end
end
if min == nil then min = 0 end
return {string.format('%.0f', min), string.format('%.0f', now)}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.redis.LuaScriptsTest' --console=plain`
Expected: PASS, 13 tests, `BUILD SUCCESSFUL`.

- [ ] **Step 7: Confirm the JDK-only build is still green**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 8: Commit**

```bash
git add src/main/resources/redis src/integrationTest/java/com/quince/cartrecovery/infra/redis/TestRedis.java src/integrationTest/java/com/quince/cartrecovery/infra/redis/LuaScriptsTest.java
git commit -m "$(cat <<'EOF'
Add Redis Lua scripts for timers and watermarks

Monotonic upsert, lease claim on Redis TIME, release/ack only if unchanged,
version-guarded remove, generation-fenced watermark set and staleness read.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---
### Task B1: DynamoDB cart store, send ledger, tables and recovery meta

**Model:** opus (conditional expressions and fencing are correctness-critical).

**Spec:** §5.3 (binding), §3 rows "Per-cart state never regresses", "Frequency cap holds", "At most one send per key", "A final ledger status stays final", §5.5 (index shapes), §6.1 rows `CartStateStore` and `SendLedger`, §6.3 "Client sizing".

**Files:**
- Create: `src/main/java/com/quince/cartrecovery/infra/dynamo/Attrs.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/dynamo/DynamoTables.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/dynamo/DynamoCartStateStore.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/dynamo/DynamoSendLedger.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/dynamo/RecoveryMetaStore.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/TestDynamo.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoTablesTest.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoCartStateStoreTest.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoSendLedgerTest.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/RecoveryMetaStoreTest.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoCartStateStoreContractTest.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoSendLedgerContractTest.java`

**Interfaces:**
- Consumes (master §1, created by A1): `model.CartEvent` (sealed; `CartEdited(cartId, shopperKey, version, occurredAt, items, firstName)`, `CartResumed`, `CartCleared`, `CartPurchased` with `(cartId, shopperKey, version, occurredAt)`), `CartItem(sku, name, quantity, priceCents)`, `CartRecord(cartId, shopperKey, status, version, lastActivityAt, items, arm, sequenceStarts, firstName, srcPartition)`, `CartStatus`, `Arm`, `RecoveryConfig` (`offsets()`, `latenessBounds()`), `Shards.of(String, int)`, `LedgerKey(cartId, version, offsetIndex)` with `toString()` and `static parse(String)`, `OutcomeKind`, `ClaimResult.Claimed(token, attempts, sendBy, srcPartition, leaseUntil)`, `ClaimResult.NotClaimed(reason)`, `DueRetry(key, srcPartition)`; ports `CartStateStore` and `SendLedger` exactly as master §1.2; test contracts `contract.CartStateStoreContract`, `contract.SendLedgerContract` (see "Contract tests").
- Produces (package `com.quince.cartrecovery.infra.dynamo`):
  - `public final class DynamoTables` with constants `CARTS = "carts"`, `SEND_LEDGER = "send-ledger"`, `RECOVERY_META = "recovery-meta"`, `OPEN_BY_SHARD = "open-by-shard"`, `RETRYING_BY_SHARD = "retrying-by-shard"`; `static DynamoDbClient client(String endpoint /* null or blank for real AWS */, int maxConnections)`; `static void createAll(DynamoDbClient)`; `static void createCarts(DynamoDbClient, String table)`; `static void createLedger(DynamoDbClient, String table)`; `static void createMeta(DynamoDbClient, String table)`. All creates are idempotent and wait until the table is active; `carts` and `send-ledger` get TTL on attribute `ttl`.
  - `public final class DynamoCartStateStore implements CartStateStore`: `DynamoCartStateStore(DynamoDbClient ddb, String table, RecoveryConfig config, int shards)`; `static Instant openUntil(Instant lastActivityAt, RecoveryConfig config)`.
  - `public final class DynamoSendLedger implements SendLedger`: `DynamoSendLedger(DynamoDbClient ddb, String table, Duration lease, int shards)`; `static String sk(long version, int offsetIndex)`.
  - `public final class RecoveryMetaStore` (controller ruling R6): `RecoveryMetaStore(DynamoDbClient ddb)` (table `recovery-meta`; package-private `RecoveryMetaStore(DynamoDbClient ddb, String table)` for tests); `static void createTable(DynamoDbClient ddb)` (idempotent); `void init(int shards, int partitions)` (writes S and P only if absent, never overwrites; callers compare `read()` with their config); `Meta read()` (consistent; throws `IllegalStateException("recovery-meta missing: run --role=init first")` when the item is absent); `public record Meta(int shards, int partitions, boolean paused, String redisRunId, String redisRole, Instant redisChangeAt)` (nullable strings and instant); `void setPaused(boolean paused)`; `void setRedisIdentity(String runId, String role)` (stores both and clears `redisChangeAt`); `void markRedisChange(Instant at)` (sets `redisChangeAt` only if absent, keeping the earliest unrepaired change).
  - Test helper `TestDynamo`: `static DynamoDbClient client()`, `static String endpoint()`, `static String table(String base)`.

**Semantics pinned here (thread A's semantics, controller ruling R4; `CartStateStoreContract` and `SendLedgerContract` check them on both adapters):**
- `openUntil = lastActivityAt + lastOffset + lastLatenessBound`, rounded up to the next whole hour (a value already on the hour stays). `openCartIds(shard, now)` returns carts with `openShard = "s#<shard>"` and `openUntil >= now`.
- `CartEdited` with `firstName == null` keeps the stored first name (no attribute write); `CartResumed` leaves it. Shopper key and arm are fixed on first write. Items are capped at 50. `getAll` returns records in input order, each id once.
- `claim` on an existing row keeps its stored `sendBy` and `srcPartition` (the retry loop does not know them); only creation takes them from the arguments. `NotClaimed("final")` for a final row, `NotClaimed("leased")` otherwise.
- `reopen` sets `attempts` to 0 so the next claim reports `attempts = 1`.
- All instants are stored as epoch milliseconds (sub-millisecond precision is dropped).

- [ ] **Step 1: Write the DynamoDB test container helper and the failing table test**

Create `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/TestDynamo.java`:

```java
package com.quince.cartrecovery.infra.dynamo;

import java.util.UUID;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** One DynamoDB Local container and client shared by every integration test in the JVM, started on first use. */
public final class TestDynamo {
    private static GenericContainer<?> container;
    private static DynamoDbClient client;

    private TestDynamo() {}

    public static synchronized DynamoDbClient client() {
        if (client == null) {
            container = new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local:3.3.1"))
                    .withExposedPorts(8000)
                    .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");
            container.start();
            client = DynamoTables.client(endpoint(), 64);
        }
        return client;
    }

    public static synchronized String endpoint() {
        if (container == null) client();
        return "http://" + container.getHost() + ":" + container.getMappedPort(8000);
    }

    /** A fresh table name per test, so tests never see each other's rows or index entries. */
    public static String table(String base) {
        return base + "-" + UUID.randomUUID();
    }
}
```

Create `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoTablesTest.java`:

```java
package com.quince.cartrecovery.infra.dynamo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus;

@Testcontainers(disabledWithoutDocker = true)
class DynamoTablesTest {
    private final DynamoDbClient ddb = TestDynamo.client();

    @Test
    void cartsHasSparseKeysOnlyOpenIndexAndTtl() {
        String table = TestDynamo.table("carts");
        DynamoTables.createCarts(ddb, table);
        DynamoTables.createCarts(ddb, table); // idempotent

        GlobalSecondaryIndexDescription gsi = ddb.describeTable(b -> b.tableName(table)).table().globalSecondaryIndexes().get(0);
        assertEquals(DynamoTables.OPEN_BY_SHARD, gsi.indexName());
        assertEquals("openShard", gsi.keySchema().get(0).attributeName());
        assertEquals(KeyType.HASH, gsi.keySchema().get(0).keyType());
        assertEquals("openUntil", gsi.keySchema().get(1).attributeName());
        assertEquals(ProjectionType.KEYS_ONLY, gsi.projection().projectionType());
        assertEquals(TimeToLiveStatus.ENABLED,
                ddb.describeTimeToLive(b -> b.tableName(table)).timeToLiveDescription().timeToLiveStatus());
    }

    @Test
    void ledgerHasRetryIndexProjectingOnlySrcPartitionAndTtl() {
        String table = TestDynamo.table("send-ledger");
        DynamoTables.createLedger(ddb, table);
        DynamoTables.createLedger(ddb, table);

        var description = ddb.describeTable(b -> b.tableName(table)).table();
        assertEquals("cartId", description.keySchema().get(0).attributeName());
        assertEquals("sk", description.keySchema().get(1).attributeName());
        GlobalSecondaryIndexDescription gsi = description.globalSecondaryIndexes().get(0);
        assertEquals(DynamoTables.RETRYING_BY_SHARD, gsi.indexName());
        assertEquals("retryShard", gsi.keySchema().get(0).attributeName());
        assertEquals("nextAttemptAt", gsi.keySchema().get(1).attributeName());
        assertEquals(ProjectionType.INCLUDE, gsi.projection().projectionType());
        assertEquals(List.of("srcPartition"), gsi.projection().nonKeyAttributes());
        assertEquals(TimeToLiveStatus.ENABLED,
                ddb.describeTimeToLive(b -> b.tableName(table)).timeToLiveDescription().timeToLiveStatus());
    }

    @Test
    void createAllCreatesTheThreeProductionTablesIdempotently() {
        DynamoTables.createAll(ddb);
        DynamoTables.createAll(ddb);
        List<String> names = ddb.listTables().tableNames();
        assertTrue(names.containsAll(List.of(DynamoTables.CARTS, DynamoTables.SEND_LEDGER, DynamoTables.RECOVERY_META)), names.toString());
    }
}
```

- [ ] **Step 2: Run the table test to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.dynamo.DynamoTablesTest' --console=plain`
Expected: FAIL at `compileIntegrationTestJava` with `cannot find symbol ... class DynamoTables`.

- [ ] **Step 3: Write the attribute helpers and table creation**

Create `src/main/java/com/quince/cartrecovery/infra/dynamo/Attrs.java`:

```java
package com.quince.cartrecovery.infra.dynamo;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

/** Attribute-value helpers shared by the DynamoDB adapters. Every attribute name goes through #name placeholders. */
final class Attrs {
    private static final Pattern NAME = Pattern.compile("#[A-Za-z]+");

    private Attrs() {}

    static AttributeValue s(String v) { return AttributeValue.fromS(v); }

    static AttributeValue n(long v) { return AttributeValue.fromN(Long.toString(v)); }

    static AttributeValue millis(Instant t) { return n(t.toEpochMilli()); }

    static AttributeValue instants(List<Instant> ts) {
        return AttributeValue.fromL(ts.stream().map(Attrs::millis).toList());
    }

    static String str(Map<String, AttributeValue> item, String name) {
        AttributeValue v = item.get(name);
        return v == null ? null : v.s();
    }

    static long num(Map<String, AttributeValue> item, String name) {
        return Long.parseLong(item.get(name).n());
    }

    static Instant instant(Map<String, AttributeValue> item, String name) {
        return Instant.ofEpochMilli(num(item, name));
    }

    static List<Instant> instantList(AttributeValue v) {
        return v == null ? List.of() : v.l().stream().map(x -> Instant.ofEpochMilli(Long.parseLong(x.n()))).toList();
    }

    /** Placeholder map for every #name used in the expressions; DynamoDB rejects unused placeholders. */
    static Map<String, String> names(String... expressions) {
        Map<String, String> names = new HashMap<>();
        Matcher m = NAME.matcher(String.join(" ", expressions));
        while (m.find()) names.put(m.group(), m.group().substring(1));
        return names;
    }

    /** Conditional UpdateItem; false when the condition failed. */
    static boolean conditionalUpdate(DynamoDbClient ddb, String table, Map<String, AttributeValue> key,
                                     String update, String condition, Map<String, AttributeValue> values) {
        try {
            ddb.updateItem(b -> b.tableName(table).key(key).updateExpression(update).conditionExpression(condition)
                    .expressionAttributeNames(names(update, condition)).expressionAttributeValues(values));
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    static void backoff(int round) {
        try {
            Thread.sleep(Math.min(1_000L, 25L << Math.min(round, 6)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted during DynamoDB backoff", e);
        }
    }
}
```

Create `src/main/java/com/quince/cartrecovery/infra/dynamo/DynamoTables.java`:

```java
package com.quince.cartrecovery.infra.dynamo;

import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

/** Table and index definitions (spec §5.3) and the shared client factory. */
public final class DynamoTables {
    public static final String CARTS = "carts";
    public static final String SEND_LEDGER = "send-ledger";
    public static final String RECOVERY_META = "recovery-meta";
    public static final String OPEN_BY_SHARD = "open-by-shard";
    public static final String RETRYING_BY_SHARD = "retrying-by-shard";

    private DynamoTables() {}

    /**
     * maxConnections should match MAX_IN_FLIGHT: the SDK default of 50 caps a JVM near 5k events per second.
     * A non-blank endpoint means DynamoDB Local, which ignores region and credentials.
     */
    public static DynamoDbClient client(String endpoint, int maxConnections) {
        DynamoDbClientBuilder b = DynamoDbClient.builder()
                .httpClientBuilder(ApacheHttpClient.builder().maxConnections(maxConnections));
        if (endpoint != null && !endpoint.isBlank()) {
            b.endpointOverride(URI.create(endpoint))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")));
        }
        return b.build();
    }

    public static void createAll(DynamoDbClient ddb) {
        createCarts(ddb, CARTS);
        createLedger(ddb, SEND_LEDGER);
        createMeta(ddb, RECOVERY_META);
    }

    public static void createCarts(DynamoDbClient ddb, String table) {
        create(ddb, CreateTableRequest.builder()
                .tableName(table)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(attr("cartId", ScalarAttributeType.S), attr("openShard", ScalarAttributeType.S),
                        attr("openUntil", ScalarAttributeType.N))
                .keySchema(key("cartId", KeyType.HASH))
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                        .indexName(OPEN_BY_SHARD)
                        .keySchema(key("openShard", KeyType.HASH), key("openUntil", KeyType.RANGE))
                        .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                        .build())
                .build(), true);
    }

    public static void createLedger(DynamoDbClient ddb, String table) {
        create(ddb, CreateTableRequest.builder()
                .tableName(table)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(attr("cartId", ScalarAttributeType.S), attr("sk", ScalarAttributeType.S),
                        attr("retryShard", ScalarAttributeType.S), attr("nextAttemptAt", ScalarAttributeType.N))
                .keySchema(key("cartId", KeyType.HASH), key("sk", KeyType.RANGE))
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                        .indexName(RETRYING_BY_SHARD)
                        .keySchema(key("retryShard", KeyType.HASH), key("nextAttemptAt", KeyType.RANGE))
                        .projection(Projection.builder().projectionType(ProjectionType.INCLUDE)
                                .nonKeyAttributes("srcPartition").build())
                        .build())
                .build(), true);
    }

    public static void createMeta(DynamoDbClient ddb, String table) {
        create(ddb, CreateTableRequest.builder()
                .tableName(table)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(attr("id", ScalarAttributeType.S))
                .keySchema(key("id", KeyType.HASH))
                .build(), false);
    }

    private static void create(DynamoDbClient ddb, CreateTableRequest request, boolean ttl) {
        String table = request.tableName();
        try {
            ddb.createTable(request);
        } catch (ResourceInUseException alreadyExists) {
            // idempotent: init may run more than once
        }
        try (DynamoDbWaiter waiter = ddb.waiter()) {
            waiter.waitUntilTableExists(b -> b.tableName(table));
        }
        if (ttl) {
            TimeToLiveStatus status = ddb.describeTimeToLive(b -> b.tableName(table)).timeToLiveDescription().timeToLiveStatus();
            if (status != TimeToLiveStatus.ENABLED && status != TimeToLiveStatus.ENABLING) {
                ddb.updateTimeToLive(b -> b.tableName(table).timeToLiveSpecification(t -> t.enabled(true).attributeName("ttl")));
            }
        }
    }

    private static AttributeDefinition attr(String name, ScalarAttributeType type) {
        return AttributeDefinition.builder().attributeName(name).attributeType(type).build();
    }

    private static KeySchemaElement key(String name, KeyType type) {
        return KeySchemaElement.builder().attributeName(name).keyType(type).build();
    }
}
```

- [ ] **Step 4: Run the table test to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.dynamo.DynamoTablesTest' --console=plain`
Expected: PASS, 3 tests.

- [ ] **Step 5: Write the failing cart store tests**

Create `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoCartStateStoreTest.java`:

```java
package com.quince.cartrecovery.infra.dynamo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Shards;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@Testcontainers(disabledWithoutDocker = true)
class DynamoCartStateStoreTest {
    private static final Instant T = Instant.parse("2026-01-01T09:00:00Z");
    private static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));
    private static final int SHARDS = 8;

    private final DynamoDbClient ddb = TestDynamo.client();
    private final RecoveryConfig config = RecoveryConfig.defaults();
    private String table;
    private DynamoCartStateStore store;

    @BeforeEach
    void setUp() {
        table = TestDynamo.table("carts");
        DynamoTables.createCarts(ddb, table);
        store = new DynamoCartStateStore(ddb, table, config, SHARDS);
    }

    private static CartEvent.CartEdited edit(String id, long version, Instant at, List<CartItem> items, String firstName) {
        return new CartEvent.CartEdited(id, "shopper-" + id, version, at, items, firstName);
    }

    private Map<String, AttributeValue> raw(String id) {
        return ddb.getItem(b -> b.tableName(table).key(Map.of("cartId", AttributeValue.fromS(id))).consistentRead(true)).item();
    }

    @Test
    void absentFirstNameAndEmptyItemsRoundTrip() {
        CartRecord written = store.applyEvent(edit("c1", 1, T, List.of(), null), Arm.TREATMENT, 3).orElseThrow();
        assertNull(written.firstName());
        assertEquals(List.of(), written.items());
        assertFalse(raw("c1").containsKey("firstName"), "null is never written as an attribute");

        CartRecord read = store.get("c1").orElseThrow();
        assertNull(read.firstName());
        assertEquals(List.of(), read.items());
        assertEquals(List.of(), read.sequenceStarts());
        assertEquals(3, read.srcPartition());

        store.applyEvent(edit("c1", 2, T, List.of(new CartItem("SKU-2", null, 2, 100)), ""), Arm.TREATMENT, 3);
        CartRecord blank = store.get("c1").orElseThrow();
        assertEquals("", blank.firstName(), "empty string is a legal non-key attribute value");
        assertEquals(List.of(new CartItem("SKU-2", null, 2, 100)), blank.items());

        store.applyEvent(edit("c1", 3, T, ITEMS, "Ada"), Arm.TREATMENT, 3);
        assertEquals("Ada", store.get("c1").orElseThrow().firstName());
        store.applyEvent(edit("c1", 4, T, ITEMS, null), Arm.TREATMENT, 3);
        assertEquals("Ada", store.get("c1").orElseThrow().firstName(), "an edit without a name keeps the stored one");
    }

    @Test
    void resumeKeepsItemsAndFirstName() {
        store.applyEvent(edit("c1", 1, T, ITEMS, "Ada"), Arm.TREATMENT, 0);
        CartRecord r = store.applyEvent(new CartEvent.CartResumed("c1", "shopper-c1", 2, T.plusSeconds(60)), Arm.TREATMENT, 5).orElseThrow();
        assertEquals(CartStatus.ACTIVE, r.status());
        assertEquals(ITEMS, r.items());
        assertEquals("Ada", r.firstName());
        assertEquals(T.plusSeconds(60), r.lastActivityAt());
        assertEquals(5, r.srcPartition(), "srcPartition follows the partition the latest event came from");
    }

    @Test
    void staleOrDuplicateEventReturnsEmptyAndLeavesTheRecord() {
        store.applyEvent(edit("c2", 5, T, ITEMS, "Ada"), Arm.TREATMENT, 1);
        assertTrue(store.applyEvent(edit("c2", 5, T.plusSeconds(1), List.of(), null), Arm.TREATMENT, 1).isEmpty());
        assertTrue(store.applyEvent(edit("c2", 4, T.plusSeconds(1), List.of(), null), Arm.TREATMENT, 1).isEmpty());
        assertTrue(store.applyEvent(new CartEvent.CartPurchased("c2", "shopper-c2", 3, T), Arm.TREATMENT, 1).isEmpty());
        CartRecord r = store.get("c2").orElseThrow();
        assertEquals(5, r.version());
        assertEquals(CartStatus.ACTIVE, r.status());
        assertEquals(ITEMS, r.items());
        assertEquals("Ada", r.firstName());
    }

    @Test
    void firstWriteFixesShopperKeyAndArm() {
        store.applyEvent(edit("c3", 1, T, ITEMS, null), Arm.HOLDOUT, 0);
        CartRecord r = store.applyEvent(new CartEvent.CartEdited("c3", "someone-else", 2, T, ITEMS, null), Arm.TREATMENT, 0).orElseThrow();
        assertEquals(Arm.HOLDOUT, r.arm());
        assertEquals("shopper-c3", r.shopperKey());
    }

    @Test
    void openIndexUsesShardAndHourRoundedOpenUntil() {
        // defaults: last offset 24 h, last bound 30 min -> 2026-01-02T09:30Z, rounded up to 10:00Z
        store.applyEvent(edit("c4", 1, T, ITEMS, null), Arm.TREATMENT, 0);
        int shard = Shards.of("c4", SHARDS);
        Instant openUntil = Instant.parse("2026-01-02T10:00:00Z");
        Map<String, AttributeValue> raw = raw("c4");
        assertEquals("s#" + shard, raw.get("openShard").s());
        assertEquals(openUntil.toEpochMilli(), Long.parseLong(raw.get("openUntil").n()));
        assertEquals(T.plus(Duration.ofDays(30)).getEpochSecond(), Long.parseLong(raw.get("ttl").n()));

        assertEquals(List.of("c4"), store.openCartIds(shard, T).toList());
        assertEquals(List.of("c4"), store.openCartIds(shard, openUntil).toList(), "openUntil >= now is inclusive");
        assertEquals(List.of(), store.openCartIds(shard, openUntil.plusMillis(1)).toList());
        assertEquals(List.of(), store.openCartIds((shard + 1) % SHARDS, T).toList());
    }

    @Test
    void openUntilOnAnExactHourIsNotRoundedFurther() {
        assertEquals(Instant.parse("2026-01-02T10:00:00Z"),
                DynamoCartStateStore.openUntil(Instant.parse("2026-01-01T09:30:00Z"), config));
        assertEquals(Instant.parse("2026-01-02T11:00:00Z"),
                DynamoCartStateStore.openUntil(Instant.parse("2026-01-01T09:30:00.001Z"), config));
    }

    @Test
    void purchaseClosesAndLeavesTheOpenIndex() {
        store.applyEvent(edit("c5", 1, T, ITEMS, null), Arm.TREATMENT, 0);
        CartRecord r = store.applyEvent(new CartEvent.CartPurchased("c5", "shopper-c5", 2, T.plusSeconds(60)), Arm.TREATMENT, 0).orElseThrow();
        assertEquals(CartStatus.CLOSED, r.status());
        assertEquals(2, r.version());
        assertEquals(ITEMS, r.items());
        assertFalse(raw("c5").containsKey("openShard"));
        assertFalse(raw("c5").containsKey("openUntil"));
        assertEquals(List.of(), store.openCartIds(Shards.of("c5", SHARDS), T).toList());
    }

    @Test
    void purchaseAsTheFirstEventCreatesAClosedRecord() {
        CartRecord r = store.applyEvent(new CartEvent.CartCleared("c6", "shopper-c6", 1, T), Arm.TREATMENT, 2).orElseThrow();
        assertEquals(CartStatus.CLOSED, r.status());
        assertEquals(List.of(), r.items());
        assertEquals(List.of(), r.sequenceStarts());
        assertEquals("shopper-c6", r.shopperKey());
    }

    @Test
    void markAbandonedAndEndSequenceAreConditionedOnVersionAndStatus() {
        CartRecord active = store.applyEvent(edit("c7", 1, T, ITEMS, null), Arm.TREATMENT, 0).orElseThrow();
        List<Instant> starts = List.of(T);
        assertTrue(store.markAbandoned(active, starts, true));
        assertFalse(store.markAbandoned(active, starts, true), "no longer ACTIVE");

        CartRecord abandoned = store.get("c7").orElseThrow();
        assertEquals(CartStatus.ABANDONED, abandoned.status());
        assertEquals(starts, abandoned.sequenceStarts());
        assertTrue(raw("c7").containsKey("openShard"), "eligible cart keeps its next step");

        assertFalse(store.endSequence("c7", 2), "wrong version");
        assertTrue(store.endSequence("c7", 1));
        assertFalse(raw("c7").containsKey("openShard"));
        assertEquals(CartStatus.ABANDONED, store.get("c7").orElseThrow().status());
        assertFalse(store.endSequence("c1-missing", 1));
    }

    @Test
    void ineligibleAbandonmentLeavesTheOpenIndex() {
        CartRecord active = store.applyEvent(edit("c8", 1, T, ITEMS, null), Arm.HOLDOUT, 0).orElseThrow();
        assertTrue(store.markAbandoned(active, List.of(T), false));
        assertFalse(raw("c8").containsKey("openShard"));
        assertFalse(raw("c8").containsKey("openUntil"));
        assertEquals(List.of(), store.openCartIds(Shards.of("c8", SHARDS), T).toList());
    }

    @Test
    void detectorWriteBetweenReloadAndAbandonmentWins() {
        CartRecord reloaded = store.applyEvent(edit("c9", 1, T, ITEMS, null), Arm.TREATMENT, 0).orElseThrow();
        store.applyEvent(edit("c9", 2, T.plusSeconds(5), ITEMS, null), Arm.TREATMENT, 0);
        assertFalse(store.markAbandoned(reloaded, List.of(T), true));
        CartRecord r = store.get("c9").orElseThrow();
        assertEquals(CartStatus.ACTIVE, r.status());
        assertEquals(2, r.version());
        assertEquals(List.of(), r.sequenceStarts(), "sequenceStarts untouched by the losing write");
    }

    @Test
    void itemsAreCappedAtFifty() {
        List<CartItem> many = IntStream.range(0, 60).mapToObj(i -> new CartItem("SKU-" + i, "Item " + i, 1, 100)).toList();
        store.applyEvent(edit("c10", 1, T, many, null), Arm.TREATMENT, 0);
        assertEquals(many.subList(0, 50), store.get("c10").orElseThrow().items());
    }

    @Test
    void getAllReadsManyCartsInInputOrderSkippingMissingAndDuplicates() {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 130; i++) {
            store.applyEvent(edit("g" + i, 1, T, ITEMS, null), Arm.TREATMENT, 0);
            ids.add("g" + i);
        }
        List<String> request = new ArrayList<>(ids);
        request.add("missing");
        request.add("g0");
        List<CartRecord> found = store.getAll(request);
        assertEquals(ids, found.stream().map(CartRecord::cartId).toList());
        assertEquals(List.of(), store.getAll(List.of()));
    }

    @Test
    void recordWrittenBeforeSrcPartitionExistedReadsMinusOne() {
        ddb.putItem(b -> b.tableName(table).item(Map.of(
                "cartId", AttributeValue.fromS("legacy"),
                "shopperKey", AttributeValue.fromS("s"),
                "status", AttributeValue.fromS("ACTIVE"),
                "version", AttributeValue.fromN("1"),
                "lastActivityAt", AttributeValue.fromN(Long.toString(T.toEpochMilli())),
                "items", AttributeValue.fromL(List.of()),
                "arm", AttributeValue.fromS("TREATMENT"),
                "sequenceStarts", AttributeValue.fromL(List.of()))));
        CartRecord r = store.get("legacy").orElseThrow();
        assertEquals(-1, r.srcPartition());
        assertNull(r.firstName());
        assertTrue(store.get("never-written").isEmpty());
    }
}
```

- [ ] **Step 6: Run the cart store tests to verify they fail**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.dynamo.DynamoCartStateStoreTest' --console=plain`
Expected: FAIL at `compileIntegrationTestJava` with `cannot find symbol ... class DynamoCartStateStore`.

- [ ] **Step 7: Write the cart store**

Create `src/main/java/com/quince/cartrecovery/infra/dynamo/DynamoCartStateStore.java`:

```java
package com.quince.cartrecovery.infra.dynamo;

import static com.quince.cartrecovery.infra.dynamo.Attrs.backoff;
import static com.quince.cartrecovery.infra.dynamo.Attrs.conditionalUpdate;
import static com.quince.cartrecovery.infra.dynamo.Attrs.instant;
import static com.quince.cartrecovery.infra.dynamo.Attrs.instantList;
import static com.quince.cartrecovery.infra.dynamo.Attrs.instants;
import static com.quince.cartrecovery.infra.dynamo.Attrs.millis;
import static com.quince.cartrecovery.infra.dynamo.Attrs.n;
import static com.quince.cartrecovery.infra.dynamo.Attrs.names;
import static com.quince.cartrecovery.infra.dynamo.Attrs.num;
import static com.quince.cartrecovery.infra.dynamo.Attrs.s;
import static com.quince.cartrecovery.infra.dynamo.Attrs.str;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.CartStateStore;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

/**
 * The {@code carts} table (spec §5.3): field-scoped conditional UpdateItem, never read-modify-write.
 * openShard/openUntil form the sparse {@code open-by-shard} index and are present only while the cart has a next step.
 */
public final class DynamoCartStateStore implements CartStateStore {
    static final int MAX_ITEMS = 50;
    private static final int BATCH_GET = 100;
    private static final Duration TTL = Duration.ofDays(30);
    private static final String NEWER = "attribute_not_exists(#cartId) OR #version < :v";

    private final DynamoDbClient ddb;
    private final String table;
    private final RecoveryConfig config;
    private final int shards;

    public DynamoCartStateStore(DynamoDbClient ddb, String table, RecoveryConfig config, int shards) {
        this.ddb = ddb;
        this.table = table;
        this.config = config;
        this.shards = shards;
    }

    /** lastActivityAt + last offset + its lateness bound, rounded up to the next whole hour. */
    public static Instant openUntil(Instant lastActivityAt, RecoveryConfig config) {
        int last = config.offsets().size() - 1;
        Instant t = lastActivityAt.plus(config.offsets().get(last)).plus(config.latenessBounds().get(last));
        Instant hour = t.truncatedTo(ChronoUnit.HOURS);
        return hour.equals(t) ? t : hour.plus(Duration.ofHours(1));
    }

    @Override
    public Optional<CartRecord> get(String cartId) {
        Map<String, AttributeValue> item = ddb.getItem(b -> b.tableName(table).key(key(cartId)).consistentRead(true)).item();
        return item == null || item.isEmpty() ? Optional.empty() : Optional.of(toRecord(item));
    }

    @Override
    public List<CartRecord> getAll(Collection<String> cartIds) {
        List<String> ids = List.copyOf(new LinkedHashSet<>(cartIds)); // BatchGetItem rejects duplicate keys
        Map<String, CartRecord> found = new HashMap<>();
        for (int i = 0; i < ids.size(); i += BATCH_GET) {
            List<Map<String, AttributeValue>> keys = ids.subList(i, Math.min(ids.size(), i + BATCH_GET)).stream()
                    .map(DynamoCartStateStore::key).toList();
            Map<String, KeysAndAttributes> pending =
                    Map.of(table, KeysAndAttributes.builder().keys(keys).consistentRead(true).build());
            for (int round = 0; !pending.isEmpty(); round++) {
                if (round > 0) backoff(round);
                BatchGetItemResponse r = ddb.batchGetItem(BatchGetItemRequest.builder().requestItems(pending).build());
                r.responses().getOrDefault(table, List.of()).forEach(item -> found.put(str(item, "cartId"), toRecord(item)));
                pending = r.unprocessedKeys();
            }
        }
        return ids.stream().map(found::get).filter(Objects::nonNull).toList();
    }

    @Override
    public Optional<CartRecord> applyEvent(CartEvent event, Arm arm, int srcPartition) {
        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":v", n(event.version()));
        v.put(":at", millis(event.occurredAt()));
        v.put(":sp", n(srcPartition));
        v.put(":sk", s(event.shopperKey()));
        v.put(":arm", s(arm.name()));
        v.put(":empty", AttributeValue.fromL(List.of()));
        v.put(":ttl", n(event.occurredAt().plus(TTL).getEpochSecond()));
        List<String> set = new ArrayList<>(List.of(
                "#version = :v", "#lastActivityAt = :at", "#srcPartition = :sp", "#ttl = :ttl",
                "#shopperKey = if_not_exists(#shopperKey, :sk)", "#arm = if_not_exists(#arm, :arm)",
                "#sequenceStarts = if_not_exists(#sequenceStarts, :empty)", "#status = :status"));
        List<String> remove = new ArrayList<>();
        if (event instanceof CartEvent.CartPurchased || event instanceof CartEvent.CartCleared) {
            v.put(":status", s(CartStatus.CLOSED.name()));
            set.add("#items = if_not_exists(#items, :empty)");
            remove.add("#openShard");
            remove.add("#openUntil");
        } else {
            v.put(":status", s(CartStatus.ACTIVE.name()));
            v.put(":shard", s("s#" + Shards.of(event.cartId(), shards)));
            v.put(":until", millis(openUntil(event.occurredAt(), config)));
            set.add("#openShard = :shard");
            set.add("#openUntil = :until");
            if (event instanceof CartEvent.CartEdited e) {
                v.put(":items", items(e.items()));
                set.add("#items = :items");
                if (e.firstName() != null) {   // an edit without a name keeps the stored one (controller ruling R4)
                    v.put(":fn", s(e.firstName()));
                    set.add("#firstName = :fn");
                }
            } else {
                set.add("#items = if_not_exists(#items, :empty)");
            }
        }
        String update = "SET " + String.join(", ", set) + (remove.isEmpty() ? "" : " REMOVE " + String.join(", ", remove));
        try {
            Map<String, AttributeValue> item = ddb.updateItem(b -> b.tableName(table).key(key(event.cartId()))
                    .updateExpression(update).conditionExpression(NEWER)
                    .expressionAttributeNames(names(update, NEWER)).expressionAttributeValues(v)
                    .returnValues(ReturnValue.ALL_NEW)).attributes();
            return Optional.of(toRecord(item));
        } catch (ConditionalCheckFailedException staleOrDuplicate) {
            return Optional.empty();
        }
    }

    @Override
    public boolean markAbandoned(CartRecord record, List<Instant> sequenceStarts, boolean eligible) {
        String update = "SET #status = :abandoned, #sequenceStarts = :starts" + (eligible ? "" : " REMOVE #openShard, #openUntil");
        return conditionalUpdate(ddb, table, key(record.cartId()), update, "#version = :v AND #status = :active", Map.of(
                ":abandoned", s(CartStatus.ABANDONED.name()),
                ":starts", instants(sequenceStarts),
                ":v", n(record.version()),
                ":active", s(CartStatus.ACTIVE.name())));
    }

    @Override
    public boolean endSequence(String cartId, long version) {
        return conditionalUpdate(ddb, table, key(cartId), "REMOVE #openShard, #openUntil", "#version = :v AND #status = :abandoned",
                Map.of(":v", n(version), ":abandoned", s(CartStatus.ABANDONED.name())));
    }

    /** Eventually consistent index read; every consumer reloads or conditions its write (spec §5.3). */
    @Override
    public Stream<String> openCartIds(int shard, Instant now) {
        String kc = "#openShard = :shard AND #openUntil >= :now";
        QueryRequest q = QueryRequest.builder().tableName(table).indexName(DynamoTables.OPEN_BY_SHARD)
                .keyConditionExpression(kc).expressionAttributeNames(names(kc))
                .expressionAttributeValues(Map.of(":shard", s("s#" + shard), ":now", millis(now)))
                .build();
        return ddb.queryPaginator(q).items().stream().map(item -> str(item, "cartId"));
    }

    private static Map<String, AttributeValue> key(String cartId) {
        return Map.of("cartId", s(cartId));
    }

    private static AttributeValue items(List<CartItem> items) {
        return AttributeValue.fromL(items.stream().limit(MAX_ITEMS).map(DynamoCartStateStore::item).toList());
    }

    private static AttributeValue item(CartItem i) {
        Map<String, AttributeValue> m = new HashMap<>();
        if (i.sku() != null) m.put("sku", s(i.sku()));
        if (i.name() != null) m.put("name", s(i.name()));
        m.put("quantity", n(i.quantity()));
        m.put("priceCents", n(i.priceCents()));
        return AttributeValue.fromM(m);
    }

    private static List<CartItem> itemsOf(AttributeValue v) {
        if (v == null) return List.of();
        return v.l().stream().map(AttributeValue::m)
                .map(m -> new CartItem(str(m, "sku"), str(m, "name"), (int) num(m, "quantity"), num(m, "priceCents")))
                .toList();
    }

    static CartRecord toRecord(Map<String, AttributeValue> i) {
        return new CartRecord(str(i, "cartId"), str(i, "shopperKey"), CartStatus.valueOf(str(i, "status")), num(i, "version"),
                instant(i, "lastActivityAt"), itemsOf(i.get("items")), Arm.valueOf(str(i, "arm")),
                instantList(i.get("sequenceStarts")), str(i, "firstName"),
                i.containsKey("srcPartition") ? (int) num(i, "srcPartition") : -1);
    }
}
```

- [ ] **Step 8: Run the cart store tests to verify they pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.dynamo.DynamoCartStateStoreTest' --console=plain`
Expected: PASS, 14 tests.

- [ ] **Step 9: Write the failing ledger tests**

Create `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoSendLedgerTest.java`:

```java
package com.quince.cartrecovery.infra.dynamo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.Shards;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Testcontainers(disabledWithoutDocker = true)
class DynamoSendLedgerTest {
    private static final Instant T = Instant.parse("2026-01-01T09:00:00Z");
    private static final Instant SEND_BY = T.plusSeconds(600);
    private static final Duration LEASE = Duration.ofSeconds(90);
    private static final int SHARDS = 8;

    private final DynamoDbClient ddb = TestDynamo.client();
    private DynamoSendLedger ledger;

    @BeforeEach
    void setUp() {
        String table = TestDynamo.table("send-ledger");
        DynamoTables.createLedger(ddb, table);
        ledger = new DynamoSendLedger(ddb, table, LEASE, SHARDS);
    }

    private static String key(String cartId, long version, int offset) {
        return new LedgerKey(cartId, version, offset).toString();
    }

    private ClaimResult.Claimed claimed(String key, Instant now) {
        return (ClaimResult.Claimed) ledger.claim(key, SEND_BY, 4, now);
    }

    @Test
    void takeoverAllowedExactlyAtLeaseUntil() {
        String key = key("c1", 7, 0);
        ClaimResult.Claimed first = claimed(key, T);
        assertEquals(1, first.attempts());
        assertEquals(T.plus(LEASE), first.leaseUntil());
        assertEquals(SEND_BY, first.sendBy());
        assertEquals(4, first.srcPartition());

        assertEquals(new ClaimResult.NotClaimed("leased"), ledger.claim(key, SEND_BY, 4, first.leaseUntil().minusMillis(1)));

        ClaimResult.Claimed second = (ClaimResult.Claimed) ledger.claim(key, Instant.EPOCH, 9, first.leaseUntil());
        assertEquals(2, second.attempts());
        assertNotEquals(first.token(), second.token());
        assertEquals(SEND_BY, second.sendBy(), "stored sendBy wins on takeover");
        assertEquals(4, second.srcPartition(), "stored srcPartition wins on takeover");

        assertFalse(ledger.finish(key, first.token(), OutcomeKind.SENT, null), "stale token is fenced");
        assertFalse(ledger.markRetry(key, first.token(), T), "stale token is fenced");
        assertTrue(ledger.finish(key, second.token(), OutcomeKind.SENT, null));
    }

    @Test
    void retryDueExactlyAtNextAttemptAt() {
        String cartId = "a:b|c";
        String key = key(cartId, 3, 1);
        int shard = Shards.of(cartId, SHARDS);
        ClaimResult.Claimed c = claimed(key, T);
        Instant next = T.plusSeconds(30);
        assertTrue(ledger.markRetry(key, c.token(), next));

        assertEquals(List.of(), ledger.dueRetries(shard, next.minusMillis(1), 10));
        assertEquals(List.of(new DueRetry(key, 4)), ledger.dueRetries(shard, next, 10), "key with ':' and '|' round-trips");

        assertEquals(new ClaimResult.NotClaimed("leased"), ledger.claim(key, SEND_BY, 4, next.minusMillis(1)));
        ClaimResult.Claimed again = claimed(key, next);
        assertEquals(2, again.attempts());
        assertFalse(ledger.markRetry(key, c.token(), next), "the retrying holder's old token is dead");
    }

    @Test
    void sendingRowIsDueForTakeoverAtLeaseUntil() {
        String key = key("c2", 1, 0);
        ClaimResult.Claimed c = claimed(key, T);
        int shard = Shards.of("c2", SHARDS);
        assertEquals(List.of(), ledger.dueRetries(shard, c.leaseUntil().minusMillis(1), 10));
        assertEquals(List.of(new DueRetry(key, 4)), ledger.dueRetries(shard, c.leaseUntil(), 10));
    }

    @Test
    void finalRowsLeaveTheRetryIndexAndStayFinal() {
        String key = key("c3", 1, 2);
        ClaimResult.Claimed c = claimed(key, T);
        assertTrue(ledger.finish(key, c.token(), OutcomeKind.CANCELLED, "purchased"));
        assertEquals(List.of(), ledger.dueRetries(Shards.of("c3", SHARDS), T.plus(Duration.ofDays(1)), 10));
        assertEquals(new ClaimResult.NotClaimed("final"), ledger.claim(key, SEND_BY, 4, T.plus(Duration.ofDays(1))));
        assertFalse(ledger.finish(key, c.token(), OutcomeKind.SENT, null));
        assertFalse(ledger.reopen(key, T), "only DEAD reopens");
    }

    @Test
    void reopenMovesDeadToRetryingDueNowWithAttemptsReset() {
        String key = key("c4", 2, 0);
        ClaimResult.Claimed c1 = claimed(key, T);
        assertTrue(ledger.markRetry(key, c1.token(), T.plusSeconds(10)));
        ClaimResult.Claimed c2 = claimed(key, T.plusSeconds(10));
        assertEquals(2, c2.attempts());
        assertTrue(ledger.finish(key, c2.token(), OutcomeKind.DEAD, "exhausted"));

        Instant replayAt = T.plusSeconds(100);
        assertTrue(ledger.reopen(key, replayAt));
        assertFalse(ledger.reopen(key, replayAt), "already RETRYING; replaying twice is harmless");
        assertEquals(List.of(new DueRetry(key, 4)), ledger.dueRetries(Shards.of("c4", SHARDS), replayAt, 10));
        ClaimResult.Claimed c3 = claimed(key, replayAt);
        assertEquals(1, c3.attempts());
        assertEquals(SEND_BY, c3.sendBy());
    }

    @Test
    void dueRetriesHonoursTheLimit() {
        for (int i = 0; i < 5; i++) {
            String key = key("same-cart", 1, i);
            ClaimResult.Claimed c = claimed(key, T);
            ledger.markRetry(key, c.token(), T);
        }
        assertEquals(3, ledger.dueRetries(Shards.of("same-cart", SHARDS), T, 3).size());
        assertEquals(5, ledger.dueRetries(Shards.of("same-cart", SHARDS), T, 10).size());
        assertEquals(List.of(), ledger.dueRetries(Shards.of("same-cart", SHARDS), T, 0));
    }

    @Test
    void highestOffsetIndexIsPerVersion() {
        claimed(key("c5", 5, 0), T);
        claimed(key("c5", 5, 2), T);
        claimed(key("c5", 50, 1), T);
        assertEquals(2, ledger.highestOffsetIndex("c5", 5));
        assertEquals(1, ledger.highestOffsetIndex("c5", 50), "version 5 is not a prefix match for 50");
        assertEquals(-1, ledger.highestOffsetIndex("c5", 6));
        assertEquals(-1, ledger.highestOffsetIndex("nobody", 5));
    }

    @Test
    void finishRejectsANonLedgerOutcome() {
        String key = key("c6", 1, 0);
        ClaimResult.Claimed c = claimed(key, T);
        assertThrows(IllegalArgumentException.class, () -> ledger.finish(key, c.token(), OutcomeKind.ABANDONED, null));
    }
}
```

- [ ] **Step 10: Run the ledger tests to verify they fail**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.dynamo.DynamoSendLedgerTest' --console=plain`
Expected: FAIL at `compileIntegrationTestJava` with `cannot find symbol ... class DynamoSendLedger`.

- [ ] **Step 11: Write the ledger**

Create `src/main/java/com/quince/cartrecovery/infra/dynamo/DynamoSendLedger.java`:

```java
package com.quince.cartrecovery.infra.dynamo;

import static com.quince.cartrecovery.infra.dynamo.Attrs.conditionalUpdate;
import static com.quince.cartrecovery.infra.dynamo.Attrs.instant;
import static com.quince.cartrecovery.infra.dynamo.Attrs.millis;
import static com.quince.cartrecovery.infra.dynamo.Attrs.n;
import static com.quince.cartrecovery.infra.dynamo.Attrs.names;
import static com.quince.cartrecovery.infra.dynamo.Attrs.num;
import static com.quince.cartrecovery.infra.dynamo.Attrs.s;
import static com.quince.cartrecovery.infra.dynamo.Attrs.str;

import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.SendLedger;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

/**
 * The {@code send-ledger} table (spec §5.3). Every claim is one conditional write with a fresh fencing token;
 * every later transition is conditioned on {@code status = SENDING AND leaseToken = :token}. Every non-final row
 * carries {@code retryShard}, so the sparse {@code retrying-by-shard} index holds exactly the rows a retry loop may take over.
 */
public final class DynamoSendLedger implements SendLedger {
    private static final String SENDING = "SENDING";
    private static final String RETRYING = "RETRYING";
    private static final Set<String> FINAL = Set.of("SENT", "SKIPPED_LATE", "CANCELLED", "DEAD");
    private static final Duration TTL = Duration.ofDays(30);
    private static final String OWNED = "#status = :sending AND #leaseToken = :token";

    private final DynamoDbClient ddb;
    private final String table;
    private final Duration lease;
    private final int shards;

    public DynamoSendLedger(DynamoDbClient ddb, String table, Duration lease, int shards) {
        this.ddb = ddb;
        this.table = table;
        this.lease = lease;
        this.shards = shards;
    }

    /** Sort key: version as 20 digits, '#', offset as 2 digits, so begins_with("<version>#") never matches a longer version. */
    public static String sk(long version, int offsetIndex) {
        return String.format("%020d#%02d", version, offsetIndex);
    }

    @Override
    public ClaimResult claim(String key, Instant sendBy, int srcPartition, Instant now) {
        Objects.requireNonNull(sendBy, "sendBy");
        LedgerKey k = LedgerKey.parse(key);
        String token = UUID.randomUUID().toString();
        Instant leaseUntil = now.plus(lease);
        String update = "SET #status = :sending, #leaseToken = :token, #leaseUntil = :leaseUntil, "
                + "#nextAttemptAt = :leaseUntil, #retryShard = :retryShard, "
                + "#sendBy = if_not_exists(#sendBy, :sendBy), #srcPartition = if_not_exists(#srcPartition, :srcPartition), "
                + "#ttl = if_not_exists(#ttl, :ttl), #attempts = if_not_exists(#attempts, :zero) + :one "
                + "REMOVE #reason";
        String condition = "attribute_not_exists(#cartId) "
                + "OR (#status = :retrying AND #nextAttemptAt <= :now) "
                + "OR (#status = :sending AND #leaseUntil <= :now)";
        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":sending", s(SENDING));
        v.put(":retrying", s(RETRYING));
        v.put(":token", s(token));
        v.put(":leaseUntil", millis(leaseUntil));
        v.put(":retryShard", s(retryShard(k.cartId())));
        v.put(":sendBy", millis(sendBy));
        v.put(":srcPartition", n(srcPartition));
        v.put(":ttl", n(now.plus(TTL).getEpochSecond()));
        v.put(":zero", n(0));
        v.put(":one", n(1));
        v.put(":now", millis(now));
        try {
            Map<String, AttributeValue> row = ddb.updateItem(b -> b.tableName(table).key(key(k))
                    .updateExpression(update).conditionExpression(condition)
                    .expressionAttributeNames(names(update, condition)).expressionAttributeValues(v)
                    .returnValues(ReturnValue.ALL_NEW)).attributes();
            return new ClaimResult.Claimed(token, (int) num(row, "attempts"), instant(row, "sendBy"),
                    (int) num(row, "srcPartition"), leaseUntil);
        } catch (ConditionalCheckFailedException e) {
            // Rare path; one consistent read tells a final row from one that is leased or not yet due.
            String status = str(row(k), "status");
            return new ClaimResult.NotClaimed(status != null && FINAL.contains(status) ? "final" : "leased");
        }
    }

    @Override
    public boolean markRetry(String key, String token, Instant nextAt) {
        return conditionalUpdate(ddb, table, key(LedgerKey.parse(key)),
                "SET #status = :retrying, #nextAttemptAt = :next REMOVE #leaseToken, #leaseUntil", OWNED,
                Map.of(":retrying", s(RETRYING), ":next", millis(nextAt), ":sending", s(SENDING), ":token", s(token)));
    }

    @Override
    public boolean finish(String key, String token, OutcomeKind outcome, String reason) {
        if (!FINAL.contains(outcome.name())) throw new IllegalArgumentException("not a final ledger status: " + outcome);
        String update = "SET #status = :final" + (reason == null ? "" : ", #reason = :reason")
                + " REMOVE #leaseToken, #leaseUntil, #retryShard, #nextAttemptAt";
        Map<String, AttributeValue> v = new HashMap<>(Map.of(":final", s(outcome.name()), ":sending", s(SENDING), ":token", s(token)));
        if (reason != null) v.put(":reason", s(reason));
        return conditionalUpdate(ddb, table, key(LedgerKey.parse(key)), update, OWNED, v);
    }

    /** Eventually consistent index read; the conditional claim settles races between retry pollers. */
    @Override
    public List<DueRetry> dueRetries(int shard, Instant now, int limit) {
        if (limit <= 0) return List.of();
        String kc = "#retryShard = :retryShard AND #nextAttemptAt <= :now";
        QueryRequest q = QueryRequest.builder().tableName(table).indexName(DynamoTables.RETRYING_BY_SHARD)
                .keyConditionExpression(kc).expressionAttributeNames(names(kc))
                .expressionAttributeValues(Map.of(":retryShard", s("s#" + shard), ":now", millis(now)))
                .limit(limit)
                .build();
        return ddb.queryPaginator(q).items().stream().limit(limit)
                .map(row -> new DueRetry(keyOf(row), (int) num(row, "srcPartition")))
                .toList();
    }

    @Override
    public boolean reopen(String key, Instant now) {
        LedgerKey k = LedgerKey.parse(key);
        return conditionalUpdate(ddb, table, key(k),
                "SET #status = :retrying, #nextAttemptAt = :now, #attempts = :zero, #retryShard = :retryShard REMOVE #reason",
                "#status = :dead",
                Map.of(":retrying", s(RETRYING), ":now", millis(now), ":zero", n(0),
                        ":retryShard", s(retryShard(k.cartId())), ":dead", s("DEAD")));
    }

    @Override
    public int highestOffsetIndex(String cartId, long version) {
        String kc = "#cartId = :cartId AND begins_with(#sk, :prefix)";
        QueryResponse r = ddb.query(b -> b.tableName(table).keyConditionExpression(kc).expressionAttributeNames(names(kc))
                .expressionAttributeValues(Map.of(":cartId", s(cartId), ":prefix", s(String.format("%020d#", version))))
                .scanIndexForward(false).limit(1).consistentRead(true));
        return r.items().isEmpty() ? -1 : Integer.parseInt(str(r.items().get(0), "sk").substring(21));
    }

    private String retryShard(String cartId) {
        return "s#" + Shards.of(cartId, shards);
    }

    private Map<String, AttributeValue> row(LedgerKey k) {
        return ddb.getItem(b -> b.tableName(table).key(key(k)).consistentRead(true)).item();
    }

    private static Map<String, AttributeValue> key(LedgerKey k) {
        return Map.of("cartId", s(k.cartId()), "sk", s(sk(k.version(), k.offsetIndex())));
    }

    private static String keyOf(Map<String, AttributeValue> row) {
        String sk = str(row, "sk");
        int hash = sk.indexOf('#');
        return new LedgerKey(str(row, "cartId"), Long.parseLong(sk.substring(0, hash)),
                Integer.parseInt(sk.substring(hash + 1))).toString();
    }
}
```

- [ ] **Step 12: Run the ledger tests to verify they pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.dynamo.DynamoSendLedgerTest' --console=plain`
Expected: PASS, 8 tests.

- [ ] **Step 13: Write the failing recovery-meta test**

Create `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/RecoveryMetaStoreTest.java`:

```java
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
```

- [ ] **Step 14: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.dynamo.RecoveryMetaStoreTest' --console=plain`
Expected: FAIL at `compileIntegrationTestJava` with `cannot find symbol ... class RecoveryMetaStore`.

- [ ] **Step 15: Write the recovery-meta store**

Create `src/main/java/com/quince/cartrecovery/infra/dynamo/RecoveryMetaStore.java`:

```java
package com.quince.cartrecovery.infra.dynamo;

import static com.quince.cartrecovery.infra.dynamo.Attrs.instant;
import static com.quince.cartrecovery.infra.dynamo.Attrs.millis;
import static com.quince.cartrecovery.infra.dynamo.Attrs.n;
import static com.quince.cartrecovery.infra.dynamo.Attrs.names;
import static com.quince.cartrecovery.infra.dynamo.Attrs.num;
import static com.quince.cartrecovery.infra.dynamo.Attrs.s;
import static com.quince.cartrecovery.infra.dynamo.Attrs.str;

import java.time.Instant;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * The single {@code recovery-meta} item (spec §5.3): S, P, the guardrail pause switch, and the last seen Redis
 * identity with the earliest unrepaired Redis change (the reconciler's failover replay restarts from it).
 */
public final class RecoveryMetaStore {
    public record Meta(int shards, int partitions, boolean paused, String redisRunId, String redisRole, Instant redisChangeAt) {}

    private static final Map<String, AttributeValue> KEY = Map.of("id", s("meta"));

    private final DynamoDbClient ddb;
    private final String table;

    public RecoveryMetaStore(DynamoDbClient ddb) {
        this(ddb, DynamoTables.RECOVERY_META);
    }

    /** Tests use a fresh table per case. */
    RecoveryMetaStore(DynamoDbClient ddb, String table) {
        this.ddb = ddb;
        this.table = table;
    }

    /** Creates the {@code recovery-meta} table if absent and waits until it is active. */
    public static void createTable(DynamoDbClient ddb) {
        DynamoTables.createMeta(ddb, DynamoTables.RECOVERY_META);
    }

    /** Writes S and P only if absent and never overwrites them; callers compare {@link #read()} with their config. */
    public void init(int shards, int partitions) {
        set("SET #shards = if_not_exists(#shards, :shards), #partitions = if_not_exists(#partitions, :partitions), "
                + "#paused = if_not_exists(#paused, :false)",
                Map.of(":shards", n(shards), ":partitions", n(partitions), ":false", AttributeValue.fromBool(false)));
    }

    /** Consistent read; throws {@link IllegalStateException} when init has not run. */
    public Meta read() {
        Map<String, AttributeValue> i = ddb.getItem(b -> b.tableName(table).key(KEY).consistentRead(true)).item();
        if (i == null || !i.containsKey("shards")) throw new IllegalStateException("recovery-meta missing: run --role=init first");
        return new Meta((int) num(i, "shards"), (int) num(i, "partitions"),
                i.containsKey("paused") && i.get("paused").bool(),
                str(i, "redisRunId"), str(i, "redisRole"),
                i.containsKey("redisChangeAt") ? instant(i, "redisChangeAt") : null);
    }

    public void setPaused(boolean paused) {
        set("SET #paused = :paused", Map.of(":paused", AttributeValue.fromBool(paused)));
    }

    /** Stores the Redis identity (on first sight or after a completed failover replay) and clears the pending change. */
    public void setRedisIdentity(String runId, String role) {
        set("SET #redisRunId = :runId, #redisRole = :role REMOVE #redisChangeAt", Map.of(":runId", s(runId), ":role", s(role)));
    }

    /** Records a detected Redis restart or failover; keeps the earliest unrepaired change. */
    public void markRedisChange(Instant at) {
        set("SET #redisChangeAt = if_not_exists(#redisChangeAt, :at)", Map.of(":at", millis(at)));
    }

    private void set(String update, Map<String, AttributeValue> values) {
        ddb.updateItem(b -> b.tableName(table).key(KEY).updateExpression(update)
                .expressionAttributeNames(names(update)).expressionAttributeValues(values));
    }
}
```

- [ ] **Step 16: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.dynamo.RecoveryMetaStoreTest' --console=plain`
Expected: PASS, 3 tests.

- [ ] **Step 17: Write the contract subclasses**

Create `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoCartStateStoreContractTest.java`:

```java
package com.quince.cartrecovery.infra.dynamo;

import com.quince.cartrecovery.contract.CartStateStoreContract;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.CartStateStore;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Runs thread A's cart store contract against DynamoDB Local, one fresh table per store. */
@Testcontainers(disabledWithoutDocker = true)
class DynamoCartStateStoreContractTest extends CartStateStoreContract {
    @Override
    protected CartStateStore newStore(RecoveryConfig config, int shards) {
        String table = TestDynamo.table("carts");
        DynamoTables.createCarts(TestDynamo.client(), table);
        return new DynamoCartStateStore(TestDynamo.client(), table, config, shards);
    }
}
```

Create `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoSendLedgerContractTest.java`:

```java
package com.quince.cartrecovery.infra.dynamo;

import com.quince.cartrecovery.contract.SendLedgerContract;
import com.quince.cartrecovery.ports.SendLedger;
import java.time.Duration;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Runs thread A's send ledger contract against DynamoDB Local, one fresh table per ledger. */
@Testcontainers(disabledWithoutDocker = true)
class DynamoSendLedgerContractTest extends SendLedgerContract {
    @Override
    protected SendLedger newLedger(Duration lease, int shards) {
        String table = TestDynamo.table("send-ledger");
        DynamoTables.createLedger(TestDynamo.client(), table);
        return new DynamoSendLedger(TestDynamo.client(), table, lease, shards);
    }
}
```

These overrides match thread A's abstract signatures exactly (see "Contract tests" above).

- [ ] **Step 18: Run the whole Dynamo package including the contracts**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.dynamo.*' --console=plain`
Expected: PASS, every test in `DynamoTablesTest`, `DynamoCartStateStoreTest`, `DynamoSendLedgerTest`, `RecoveryMetaStoreTest`, and every inherited contract test. A contract failure means the adapter differs from thread A's semantics: fix the adapter (controller ruling R4); if the contract asserts something spec §5.3 contradicts, stop and report `BLOCKED`.

- [ ] **Step 19: Confirm the JDK-only build is still green**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 20: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/infra/dynamo src/integrationTest/java/com/quince/cartrecovery/infra/dynamo
git commit -m "$(cat <<'EOF'
Add DynamoDB cart store, fenced send ledger, tables and recovery-meta store

Field-scoped conditional UpdateItem for carts with the sparse open-by-shard
index; ledger claim with fresh fencing tokens, token-conditioned transitions,
sparse retrying-by-shard index; contract tests on DynamoDB Local.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---
### Task B2: Redis timer store, watermark and meta over the B0 scripts

**Model:** sonnet (thin adapter over reviewed scripts).

**Spec:** §5.2 (binding), §5.4 (watermark read and write rules), §6.1 rows `TimerStore` and `Watermark`, §6.2 "Reconciler" (epoch sentinel, `run_id` and role).

**Files:**
- Create: `src/main/java/com/quince/cartrecovery/infra/redis/RedisScripts.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/redis/RedisTimerStore.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/redis/RedisWatermark.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/redis/RedisMeta.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisTimerStoreTest.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisWatermarkTest.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisMetaTest.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisTimerStoreContractTest.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisWatermarkContractTest.java`

**Interfaces:**
- Consumes: B0 scripts under `/redis/<name>.lua` and their KEYS/ARGV contracts (see Task B0 "Produces"); B0 `TestRedis` (`connection()`, `sync()`, `flushAll()`, `now()`, `sleep(Duration)`); master §1: `model.Timer(cartId, kind, version, offsetIndex, dueAt, srcPartition)`, `Timer.checkAbandon(cartId, version, dueAt, srcPartition)`, `Timer.reminder(cartId, version, offsetIndex, dueAt, srcPartition)`, `TimerKind`, `Shards.of(String, int)`; ports `TimerStore`, `Watermark` exactly as master §1.2; contracts `contract.TimerStoreContract`, `contract.WatermarkContract`.
- Produces (package `com.quince.cartrecovery.infra.redis`):
  - `public final class RedisTimerStore implements TimerStore`: `RedisTimerStore(StatefulRedisConnection<String, String> connection, int shards, Duration lease)`; `static String pack(Timer)`; `static Timer unpack(String cartId, String packed)`. `existing(shard, ids)` checks each id in its own shard (`Shards.of(id, shards)`), so the `shard` argument is only a hint and a mixed list is safe. `claimDue(limit)` walks all shards from a rotating start.
  - `public final class RedisWatermark implements Watermark`: `RedisWatermark(StatefulRedisConnection<String, String> connection, int partitions)` (5 s staleness); `RedisWatermark(StatefulRedisConnection<String, String> connection, int partitions, Duration staleAfter)`. `current(-1)` is the minimum over partitions `0..partitions-1`, where an unpublished or stale partition counts as `Instant.EPOCH`.
  - `public final class RedisMeta` (for the `init` role and the reconciler in C1c; the only Redis identity helper, controller ruling R6): `public static final String EPOCH = "epoch"`; `RedisMeta(StatefulRedisConnection<String, String> connection)`; `boolean epochPresent()`; `void writeEpoch()`; `String runId()` (INFO `run_id`); `String role()` (INFO `role`, for example `master`).
  - Package-private `RedisScripts(RedisCommands<String, String> redis, String... names)` with `<T> T run(String name, ScriptOutputType type, String[] keys, String... args)`: EVALSHA, and on `NOSCRIPT` (Redis restarted or `SCRIPT FLUSH`) reloads the script and retries once.

- [ ] **Step 1: Write the failing timer store tests**

Create `src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisTimerStoreTest.java`:

```java
package com.quince.cartrecovery.infra.redis;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.model.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class RedisTimerStoreTest {
    private static final int SHARDS = 8;
    private static final Instant LONG_AGO = Instant.ofEpochMilli(1_000);
    private static final Instant FAR_FUTURE = Instant.ofEpochMilli(9_999_999_999_999L);

    private RedisTimerStore store;

    @BeforeEach
    void setUp() {
        TestRedis.flushAll();
        store = new RedisTimerStore(TestRedis.connection(), SHARDS, Duration.ofSeconds(30));
    }

    @Test
    void packedValueNeverContainsTheCartId() {
        Timer reminder = Timer.reminder("a:b|c", 12, 1, LONG_AGO, 5);
        assertEquals("REMINDER|12|1|5|1000", RedisTimerStore.pack(reminder));
        assertEquals(reminder, RedisTimerStore.unpack("a:b|c", RedisTimerStore.pack(reminder)));

        Timer check = Timer.checkAbandon("x", 3, Instant.ofEpochMilli(7), -1);
        assertEquals("CHECK_ABANDON|3|-1|-1|7", RedisTimerStore.pack(check));
        assertEquals(check, RedisTimerStore.unpack("x", RedisTimerStore.pack(check)));
    }

    @Test
    void cartIdWithDelimitersRoundTripsThroughClaimAndAck() {
        String id = "x|1:2";
        Timer t = Timer.checkAbandon(id, 3, LONG_AGO, 0);
        assertTrue(store.upsert(t));
        assertEquals(Set.of(id), store.existing(Shards.of(id, SHARDS), List.of(id)));
        assertEquals(List.of(t), store.claimDue(10));
        store.ack(t);
        assertEquals(Set.of(), store.existing(Shards.of(id, SHARDS), List.of(id)));
    }

    @Test
    void upsertIsMonotonicAndEqualDataIsANoOp() {
        Timer r0 = Timer.reminder("c", 2, 0, LONG_AGO, 1);
        assertTrue(store.upsert(r0));
        assertFalse(store.upsert(r0));
        assertFalse(store.upsert(Timer.checkAbandon("c", 2, LONG_AGO, 1)));
        assertTrue(store.upsert(Timer.checkAbandon("c", 3, LONG_AGO, 1)));
    }

    @Test
    void upsertRejectsANegativeVersion() {
        assertThrows(IllegalArgumentException.class, () -> store.upsert(Timer.checkAbandon("c", -1, LONG_AGO, 0)));
    }

    @Test
    void claimDueWalksAllShardsUpToTheLimitAndLeases() {
        Set<String> ids = IntStream.range(0, 20).mapToObj(i -> "cart-" + i).collect(toSet());
        ids.forEach(id -> store.upsert(Timer.checkAbandon(id, 1, LONG_AGO, 0)));
        assertTrue(ids.stream().map(id -> Shards.of(id, SHARDS)).distinct().count() > 1, "ids spread over shards");

        List<Timer> first = store.claimDue(15);
        List<Timer> second = store.claimDue(15);
        assertEquals(15, first.size());
        assertEquals(5, second.size());
        assertEquals(ids, Stream.concat(first.stream(), second.stream()).map(Timer::cartId).collect(toSet()));
        assertEquals(List.of(), store.claimDue(15), "everything is leased");
    }

    @Test
    void releaseAckAndRemoveApplyOnlyToUnchangedOrOlderTimers() {
        Timer t = Timer.checkAbandon("c", 1, LONG_AGO, 0);
        store.upsert(t);
        Timer claimed = store.claimDue(1).get(0);
        store.release(claimed, Duration.ZERO);
        assertEquals(List.of(t), store.claimDue(1), "released with no delay: due again");

        store.upsert(Timer.checkAbandon("c", 2, FAR_FUTURE, 0));
        store.ack(t);
        assertEquals(Set.of("c"), store.existing(0, List.of("c")), "stale ack leaves the newer timer");
        store.remove("c", 1);
        assertEquals(Set.of("c"), store.existing(0, List.of("c")), "stored version 2 is greater than 1");
        store.remove("c", 2);
        assertEquals(Set.of(), store.existing(0, List.of("c")));
    }

    @Test
    void existingChecksEachIdInItsOwnShardAcrossChunks() {
        List<String> ids = IntStream.range(0, 1_200).mapToObj(i -> "e-" + i).toList();
        ids.forEach(id -> store.upsert(Timer.checkAbandon(id, 1, FAR_FUTURE, 0)));
        List<String> query = new ArrayList<>(ids);
        query.add("absent-1");
        query.add("absent-2");
        assertEquals(new HashSet<>(ids), store.existing(0, query));
        assertEquals(Set.of(), store.existing(0, List.of()));
    }

    @Test
    void reloadsScriptsAfterScriptFlush() {
        TestRedis.sync().scriptFlush();
        assertTrue(store.upsert(Timer.checkAbandon("c", 1, LONG_AGO, 0)));
        assertEquals(1, store.claimDue(10).size());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.redis.RedisTimerStoreTest' --console=plain`
Expected: FAIL at `compileIntegrationTestJava` with `cannot find symbol ... class RedisTimerStore`.

- [ ] **Step 3: Write the script runner and the timer store**

Create `src/main/java/com/quince/cartrecovery/infra/redis/RedisScripts.java`:

```java
package com.quince.cartrecovery.infra.redis;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Loads the Lua scripts from /redis/*.lua once and runs them by SHA, reloading after NOSCRIPT. */
final class RedisScripts {
    private final RedisCommands<String, String> redis;
    private final Map<String, String> bodies = new ConcurrentHashMap<>();
    private final Map<String, String> shas = new ConcurrentHashMap<>();

    RedisScripts(RedisCommands<String, String> redis, String... names) {
        this.redis = redis;
        for (String name : names) {
            String body = load(name);
            bodies.put(name, body);
            shas.put(name, redis.scriptLoad(body));
        }
    }

    <T> T run(String name, ScriptOutputType type, String[] keys, String... args) {
        try {
            return redis.evalsha(shas.get(name), type, keys, args);
        } catch (RedisNoScriptException e) {
            // Redis restarted or SCRIPT FLUSH ran: the script cache is empty. Reload and retry once.
            shas.put(name, redis.scriptLoad(bodies.get(name)));
            return redis.evalsha(shas.get(name), type, keys, args);
        }
    }

    private static String load(String name) {
        try (InputStream in = RedisScripts.class.getResourceAsStream("/redis/" + name + ".lua")) {
            if (in == null) throw new IllegalStateException("missing script /redis/" + name + ".lua");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
```

Create `src/main/java/com/quince/cartrecovery/infra/redis/RedisTimerStore.java`:

```java
package com.quince.cartrecovery.infra.redis;

import static java.util.stream.Collectors.groupingBy;

import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerKind;
import com.quince.cartrecovery.ports.TimerStore;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
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

    private final RedisAsyncCommands<String, String> async;
    private final RedisScripts scripts;
    private final int shards;
    private final long leaseMs;
    private final AtomicInteger nextShard = new AtomicInteger();

    public RedisTimerStore(StatefulRedisConnection<String, String> connection, int shards, Duration lease) {
        RedisCommands<String, String> sync = connection.sync();
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

    static Instant redisTime(RedisCommands<String, String> redis) {
        List<String> t = redis.time();
        return Instant.ofEpochMilli(Long.parseLong(t.get(0)) * 1000 + Long.parseLong(t.get(1)) / 1000);
    }
}
```

- [ ] **Step 4: Run the timer store tests to verify they pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.redis.RedisTimerStoreTest' --console=plain`
Expected: PASS, 8 tests.

- [ ] **Step 5: Write the failing watermark and meta tests**

Create `src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisWatermarkTest.java`:

```java
package com.quince.cartrecovery.infra.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class RedisWatermarkTest {
    @BeforeEach
    void setUp() {
        TestRedis.flushAll();
    }

    @Test
    void unknownSourceGatesOnTheMinimumOverAllPartitions() {
        RedisWatermark wm = new RedisWatermark(TestRedis.connection(), 3);
        assertEquals(Instant.EPOCH, wm.current(0));
        Instant t = wm.now();
        wm.publish(0, 1, t);
        wm.publish(1, 1, t.plusMillis(5));
        assertEquals(t, wm.current(0));
        assertEquals(t.plusMillis(5), wm.current(1));
        assertEquals(Instant.EPOCH, wm.current(-1), "partition 2 never published");
        wm.publish(2, 1, t.plusMillis(9));
        assertEquals(t, wm.current(-1));
    }

    @Test
    void olderGenerationIsRejectedAndSameGenerationKeepsTheMax() {
        RedisWatermark wm = new RedisWatermark(TestRedis.connection(), 1);
        Instant t = wm.now();
        wm.publish(0, 5, t);
        wm.publish(0, 5, t.minusSeconds(1));
        assertEquals(t, wm.current(0));
        wm.publish(0, 4, t.plusSeconds(1));
        assertEquals(t, wm.current(0));
        wm.publish(0, 6, t.minusSeconds(2));
        assertEquals(t.minusSeconds(2), wm.current(0));
    }

    @Test
    void goesStaleAfterSilence() {
        RedisWatermark wm = new RedisWatermark(TestRedis.connection(), 1, Duration.ofMillis(200));
        Instant t = wm.now();
        wm.publish(0, 1, t);
        assertEquals(t, wm.current(0));
        TestRedis.sleep(Duration.ofMillis(300));
        assertEquals(Instant.EPOCH, wm.current(0));
    }

    @Test
    void nowIsRedisTime() {
        RedisWatermark wm = new RedisWatermark(TestRedis.connection(), 1);
        assertTrue(Duration.between(wm.now(), TestRedis.now()).abs().toMillis() < 1_000);
    }
}
```

Create `src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisMetaTest.java`:

```java
package com.quince.cartrecovery.infra.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class RedisMetaTest {
    @Test
    void epochSentinelIsMissingAfterFlushAndPresentAfterWrite() {
        TestRedis.flushAll();
        RedisMeta meta = new RedisMeta(TestRedis.connection());
        assertFalse(meta.epochPresent());
        meta.writeEpoch();
        assertTrue(meta.epochPresent());
    }

    @Test
    void reportsRunIdAndRole() {
        RedisMeta meta = new RedisMeta(TestRedis.connection());
        assertEquals(40, meta.runId().length());
        assertEquals("master", meta.role());
    }
}
```

- [ ] **Step 6: Run them to verify they fail**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.redis.RedisWatermarkTest' --tests 'com.quince.cartrecovery.infra.redis.RedisMetaTest' --console=plain`
Expected: FAIL at `compileIntegrationTestJava` with `cannot find symbol ... class RedisWatermark` and `class RedisMeta`.

- [ ] **Step 7: Write the watermark and meta adapters**

Create `src/main/java/com/quince/cartrecovery/infra/redis/RedisWatermark.java`:

```java
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

    /** srcPartition -1 (a cart record written before the field existed) gates on the minimum over all partitions. */
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
```

Create `src/main/java/com/quince/cartrecovery/infra/redis/RedisMeta.java`:

```java
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
```

- [ ] **Step 8: Run them to verify they pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.redis.RedisWatermarkTest' --tests 'com.quince.cartrecovery.infra.redis.RedisMetaTest' --console=plain`
Expected: PASS, 6 tests.

- [ ] **Step 9: Write the contract subclasses**

Create `src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisTimerStoreContractTest.java`:

```java
package com.quince.cartrecovery.infra.redis;

import com.quince.cartrecovery.contract.TimerStoreContract;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Duration;
import java.time.Instant;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Runs thread A's timer store contract against Redis 7. Time is Redis TIME, so advancing time sleeps. */
@Testcontainers(disabledWithoutDocker = true)
class RedisTimerStoreContractTest extends TimerStoreContract {
    @Override
    protected TimerStore newStore(Duration lease) {
        TestRedis.flushAll();
        return new RedisTimerStore(TestRedis.connection(), 8, lease);
    }

    @Override
    protected Instant now() {
        return TestRedis.now();
    }

    @Override
    protected void advance(Duration d) {
        TestRedis.sleep(d);
    }
}
```

Create `src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisWatermarkContractTest.java`:

```java
package com.quince.cartrecovery.infra.redis;

import com.quince.cartrecovery.contract.WatermarkContract;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Runs thread A's watermark contract against Redis 7 with the production 5 s staleness. */
@Testcontainers(disabledWithoutDocker = true)
class RedisWatermarkContractTest extends WatermarkContract {
    /** The contract writes partitions 0 to 2, so current(-1) is the minimum over exactly those. */
    @Override
    protected Watermark newWatermark() {
        TestRedis.flushAll();
        return new RedisWatermark(TestRedis.connection(), 3);
    }

    @Override
    protected void advance(Duration d) {
        TestRedis.sleep(d);
    }
}
```

These overrides match thread A's abstract signatures exactly (see "Contract tests" above). Both factories `FLUSHALL` first, so no other test's timers or watermarks are visible; no time-boundary test is added here (controller ruling R3).

- [ ] **Step 10: Run the whole Redis package including the contracts**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.redis.*' --console=plain`
Expected: PASS for `LuaScriptsTest`, `RedisTimerStoreTest`, `RedisWatermarkTest`, `RedisMetaTest`, and every inherited contract test (the watermark staleness case sleeps about 6 s, the timer lease cases about 1.1 s each). The contracts keep at least 100 ms of margin on every time assertion; a failure is an adapter bug.

- [ ] **Step 11: Confirm the JDK-only build is still green**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 12: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/infra/redis src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisTimerStoreTest.java src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisWatermarkTest.java src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisMetaTest.java src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisTimerStoreContractTest.java src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisWatermarkContractTest.java
git commit -m "$(cat <<'EOF'
Add Redis timer store, watermark and meta over the Lua scripts

EVALSHA with NOSCRIPT reload, shard-walking claims, pipelined HMGET for the
reconciler's existence check, epoch sentinel and run_id/role for failover
detection; contract tests on Redis 7.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---
### Task B3: JSON codec, Kafka producers, recording sink and topic admin

**Model:** sonnet (straightforward client wiring).

**Spec:** §5.1 (binding: topics, keys, payloads, retention, producer settings), §6.1 rows `IntentPublisher`, `NotificationSink`, `OutcomeRecorder`, `DeadLetterQueue`, §6.3 "publish blocks until the broker acks".

**Files:**
- Create: `src/main/java/com/quince/cartrecovery/infra/kafka/JsonCodec.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/kafka/Topics.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/kafka/KafkaClients.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/kafka/TopicAdmin.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/kafka/KafkaIntentPublisher.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/kafka/KafkaOutcomeRecorder.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/kafka/KafkaDeadLetterQueue.java`
- Create: `src/main/java/com/quince/cartrecovery/infra/kafka/KafkaRecordingSink.java`
- Test: `src/test/java/com/quince/cartrecovery/infra/kafka/JsonCodecTest.java` (JDK only)
- Test: `src/test/java/com/quince/cartrecovery/infra/kafka/KafkaClientsTest.java` (JDK only)
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/kafka/TestKafka.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/kafka/KafkaAdaptersTest.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/kafka/TopicAdminTest.java`

**Interfaces:**
- Consumes (master §1, created by A1): `CartEvent` and its four records, `CartItem`, `ReminderIntent(key, cartId, version, offsetIndex, srcPartition, scheduledFor, sendBy)`, `ReminderMessage(key, cartId, shopperKey, firstName, items)`, `Outcome(key, cartId, version, arm, kind, at, attempts)`, `OutcomeKind`, `Arm`, `DeadLetter(intent, reason, at)`, `Lane.of(int offsetIndex, int fastOffsets)`, `SendResult`, `LedgerKey`; ports `Clock`, `IntentPublisher`, `OutcomeRecorder`, `DeadLetterQueue`, `NotificationSink`.
- Produces (package `com.quince.cartrecovery.infra.kafka`):
  - `public final class JsonCodec`: `SCHEMA_VERSION = 1`; `public record SinkSend(String key, String cartId, Instant at, boolean hasFirstName, int itemCount)`; `static byte[] encode(CartEvent)`, `encode(ReminderIntent)`, `encode(Outcome)`, `encode(DeadLetter)`, `encode(SinkSend)`; `static CartEvent decodeCartEvent(byte[])`, `ReminderIntent decodeIntent(byte[])`, `Outcome decodeOutcome(byte[])`, `DeadLetter decodeDeadLetter(byte[])`, `SinkSend decodeSinkSend(byte[])`. Decoders throw `IllegalArgumentException` for malformed JSON, a missing `cartId`, or an unknown event `type` (deterministic: callers in thread C wrap it in `PoisonException`). Cart event `type` values: `EDITED`, `RESUMED`, `CLEARED`, `PURCHASED`.
  - `public final class Topics`: `CART_EVENTS`, `CART_EVENTS_DLQ`, `INTENTS_FAST`, `INTENTS_SLOW`, `REMINDER_DLQ`, `OUTCOMES`, `SINK_SENDS`; `List<String> ALL` (the 7 topics); `static String intents(Lane)`; `static Duration retention(String topic)`.
  - `public final class KafkaClients`: `static Map<String, Object> producerProps(String bootstrap)` (`acks=all`, `enable.idempotence=true`, `StringSerializer` keys, `ByteArraySerializer` values); `static Producer<String, byte[]> producer(String bootstrap)`; `static Admin admin(String bootstrap)`; `static void sendAndWait(Producer<String, byte[]>, ProducerRecord<String, byte[]>)` (blocks until acknowledged, throws `IllegalStateException` on failure).
  - `public final class TopicAdmin`: `TopicAdmin(Admin admin)`; `void createAll(int partitions, int replicationFactor, int minInsyncReplicas)` (creates missing topics, leaves existing ones untouched, then verifies); `void verify(int partitions)` (throws `IllegalStateException` naming the first topic that is missing or has a different partition count); `Map<String, Integer> partitionCounts()` (partition count per existing topic of `Topics.ALL`; a missing topic is absent from the map; used by C1's startup check).
  - `KafkaIntentPublisher(Producer<String, byte[]> producer, int fastOffsets) implements IntentPublisher`, `KafkaOutcomeRecorder(Producer<String, byte[]> producer) implements OutcomeRecorder`, `KafkaDeadLetterQueue(Producer<String, byte[]> producer) implements DeadLetterQueue`, `KafkaRecordingSink(Producer<String, byte[]> producer, Clock clock, double failureRate, Random random) implements NotificationSink`. All key by `cartId` and block until the broker acknowledges. The producer type `Producer<String, byte[]>` is the same one `BatchConsumerLoop` takes for its DLQ (master §1.5), so one producer per process serves everything.
  - `KafkaRecordingSink` writes one `sink-sends` record per successful `send()` only (controller ruling R17): an injected transient failure (`random.nextDouble() < failureRate`) returns `TRANSIENT_FAILURE` and records nothing, because the gateway delivered nothing and a record would show up as a false duplicate; a failed produce also returns `TRANSIENT_FAILURE` (retried under the same key).
  - Test helper `TestKafka`: `PARTITIONS = 3`, `static String bootstrap()` (starts the container and creates the 7 topics once), `static Producer<String, byte[]> producer()`, `static Admin admin()`, `static List<ConsumerRecord<String, byte[]>> read(String topic, String keyPrefix, int expected, Duration timeout)`.

Wire formats (spec §5.1; times are epoch milliseconds; `null` fields are omitted):

| Payload | JSON fields |
|---|---|
| cart event | `schemaVersion, type, cartId, shopperKey, firstName?, version, occurredAt, items[]` (`items` is `[]` for non-edit events) |
| intent | `schemaVersion, key, cartId, version, offsetIndex, srcPartition, scheduledFor, sendBy` |
| outcome | `schemaVersion, key?, cartId, version, arm, kind, at, attempts` |
| dead letter | the intent fields plus `reason, failedAt` |
| sink send | `schemaVersion, key, cartId, at, hasFirstName, itemCount` |

- [ ] **Step 1: Write the failing codec and producer-config tests**

Create `src/test/java/com/quince/cartrecovery/infra/kafka/JsonCodecTest.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.ReminderIntent;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonCodecTest {
    private static final Instant T = Instant.parse("2026-01-01T09:00:00.123Z"); // 1767258000123
    private static final CartItem ITEM = new CartItem("SKU-1", "Linen Shirt", 1, 4990);
    private static final ReminderIntent INTENT =
            new ReminderIntent("a:b|c:3:1", "a:b|c", 3, 1, 5, T, T.plusSeconds(300));
    private static final ObjectMapper JSON = new ObjectMapper();

    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    @Test
    void everyCartEventTypeRoundTrips() throws Exception {
        List<CartEvent> events = List.of(
                new CartEvent.CartEdited("a:b|c", "s1", 3, T, List.of(ITEM), "Ada"),
                new CartEvent.CartResumed("c", "s1", 4, T),
                new CartEvent.CartCleared("c", "s1", 5, T),
                new CartEvent.CartPurchased("c", "s1", 6, T));
        List<String> types = List.of("EDITED", "RESUMED", "CLEARED", "PURCHASED");
        for (int i = 0; i < events.size(); i++) {
            byte[] json = JsonCodec.encode(events.get(i));
            assertEquals(types.get(i), JSON.readTree(json).get("type").asText());
            assertEquals(1767258000123L, JSON.readTree(json).get("occurredAt").asLong());
            assertEquals(events.get(i), JsonCodec.decodeCartEvent(json));
        }
    }

    @Test
    void absentFirstNameAndEmptyItemsStayAbsentAndEmpty() throws Exception {
        CartEvent.CartEdited e = new CartEvent.CartEdited("c", "s", 1, T, List.of(), null);
        JsonNode node = JSON.readTree(JsonCodec.encode(e));
        assertFalse(node.has("firstName"));
        assertEquals(0, node.get("items").size());
        assertEquals(e, JsonCodec.decodeCartEvent(JsonCodec.encode(e)));

        CartEvent minimal = JsonCodec.decodeCartEvent(bytes(
                "{\"schemaVersion\":1,\"type\":\"EDITED\",\"cartId\":\"c\",\"shopperKey\":\"s\",\"version\":1,\"occurredAt\":1767258000123}"));
        assertEquals(e, minimal, "a producer that omits items and firstName decodes to empty and absent");
    }

    @Test
    void everyPayloadCarriesSchemaVersionOne() throws Exception {
        List<byte[]> payloads = List.of(
                JsonCodec.encode(new CartEvent.CartResumed("c", "s", 1, T)),
                JsonCodec.encode(INTENT),
                JsonCodec.encode(new Outcome("k", "c", 1, Arm.TREATMENT, OutcomeKind.SENT, T, 1)),
                JsonCodec.encode(new DeadLetter(INTENT, "permanent", T)),
                JsonCodec.encode(new JsonCodec.SinkSend("k", "c", T, false, 0)));
        for (byte[] p : payloads) assertEquals(1, JSON.readTree(p).get("schemaVersion").asInt());
    }

    @Test
    void intentOutcomeDeadLetterAndSinkSendRoundTrip() {
        assertEquals(INTENT, JsonCodec.decodeIntent(JsonCodec.encode(INTENT)));

        Outcome sent = new Outcome("k:1:0", "c", 1, Arm.TREATMENT, OutcomeKind.SENT, T, 2);
        assertEquals(sent, JsonCodec.decodeOutcome(JsonCodec.encode(sent)));
        Outcome abandoned = new Outcome(null, "c", 1, Arm.HOLDOUT, OutcomeKind.ABANDONED, T, 0);
        assertEquals(abandoned, JsonCodec.decodeOutcome(JsonCodec.encode(abandoned)));

        DeadLetter letter = new DeadLetter(INTENT, "permanent", T.plusSeconds(1));
        assertEquals(letter, JsonCodec.decodeDeadLetter(JsonCodec.encode(letter)));

        JsonCodec.SinkSend send = new JsonCodec.SinkSend("k:1:0", "c", T, true, 2);
        assertEquals(send, JsonCodec.decodeSinkSend(JsonCodec.encode(send)));
    }

    @Test
    void outcomeWithoutKeyOmitsTheField() throws Exception {
        byte[] json = JsonCodec.encode(new Outcome(null, "c", 1, Arm.HOLDOUT, OutcomeKind.ABANDONED, T, 0));
        assertFalse(JSON.readTree(json).has("key"));
    }

    @Test
    void unknownPropertiesAreIgnored() {
        ReminderIntent decoded = JsonCodec.decodeIntent(bytes(
                "{\"schemaVersion\":2,\"key\":\"c:1:0\",\"cartId\":\"c\",\"version\":1,\"offsetIndex\":0,"
                        + "\"srcPartition\":2,\"scheduledFor\":1000,\"sendBy\":2000,\"futureField\":{\"x\":1}}"));
        assertEquals(new ReminderIntent("c:1:0", "c", 1, 0, 2, Instant.ofEpochMilli(1000), Instant.ofEpochMilli(2000)), decoded);
    }

    @Test
    void malformedJsonUnknownTypeAndMissingCartIdAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> JsonCodec.decodeCartEvent(bytes("not json")));
        assertThrows(IllegalArgumentException.class, () -> JsonCodec.decodeCartEvent(bytes(
                "{\"schemaVersion\":1,\"type\":\"TELEPORTED\",\"cartId\":\"c\",\"version\":1,\"occurredAt\":1}")));
        assertThrows(IllegalArgumentException.class, () -> JsonCodec.decodeCartEvent(bytes(
                "{\"schemaVersion\":1,\"type\":\"EDITED\",\"version\":1,\"occurredAt\":1}")));
        assertThrows(IllegalArgumentException.class, () -> JsonCodec.decodeIntent(bytes("{\"key\":")));
    }
}
```

Create `src/test/java/com/quince/cartrecovery/infra/kafka/KafkaClientsTest.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

class KafkaClientsTest {
    @Test
    void producerIsIdempotentAndWaitsForAllReplicas() {
        Map<String, Object> props = KafkaClients.producerProps("broker:9092");
        assertEquals("broker:9092", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("all", props.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(true, props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.infra.kafka.*' --console=plain`
Expected: FAIL at `compileTestJava` with `cannot find symbol ... class JsonCodec` and `class KafkaClients`.

- [ ] **Step 3: Write the codec, topic names and client helpers**

Create `src/main/java/com/quince/cartrecovery/infra/kafka/JsonCodec.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.ReminderIntent;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * JSON wire format for every Kafka payload (spec §5.1). Model records stay free of Jackson: private wire records
 * carry epoch-millisecond times and {@code schemaVersion}; unknown fields are ignored so writers can add fields first.
 */
public final class JsonCodec {
    public static final int SCHEMA_VERSION = 1;

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private JsonCodec() {}

    /** One record per delivered send at the recording sink. No personal data. */
    public record SinkSend(String key, String cartId, Instant at, boolean hasFirstName, int itemCount) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record EventJson(int schemaVersion, String type, String cartId, String shopperKey, String firstName,
                     long version, long occurredAt, List<CartItem> items) {}

    record IntentJson(int schemaVersion, String key, String cartId, long version, int offsetIndex, int srcPartition,
                      long scheduledFor, long sendBy) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record OutcomeJson(int schemaVersion, String key, String cartId, long version, Arm arm, OutcomeKind kind,
                       long at, int attempts) {}

    record DeadLetterJson(int schemaVersion, String key, String cartId, long version, int offsetIndex, int srcPartition,
                          long scheduledFor, long sendBy, String reason, long failedAt) {}

    record SinkSendJson(int schemaVersion, String key, String cartId, long at, boolean hasFirstName, int itemCount) {}

    public static byte[] encode(CartEvent e) {
        String type = switch (e) {
            case CartEvent.CartEdited x -> "EDITED";
            case CartEvent.CartResumed x -> "RESUMED";
            case CartEvent.CartCleared x -> "CLEARED";
            case CartEvent.CartPurchased x -> "PURCHASED";
        };
        List<CartItem> items = e instanceof CartEvent.CartEdited edit ? edit.items() : List.of();
        String firstName = e instanceof CartEvent.CartEdited edit ? edit.firstName() : null;
        return write(new EventJson(SCHEMA_VERSION, type, e.cartId(), e.shopperKey(), firstName, e.version(),
                e.occurredAt().toEpochMilli(), items));
    }

    public static CartEvent decodeCartEvent(byte[] bytes) {
        EventJson j = read(bytes, EventJson.class);
        require(j.cartId() != null && j.type() != null, "cart event needs cartId and type");
        Instant at = Instant.ofEpochMilli(j.occurredAt());
        return switch (j.type()) {
            case "EDITED" -> new CartEvent.CartEdited(j.cartId(), j.shopperKey(), j.version(), at,
                    j.items() == null ? List.of() : j.items(), j.firstName());
            case "RESUMED" -> new CartEvent.CartResumed(j.cartId(), j.shopperKey(), j.version(), at);
            case "CLEARED" -> new CartEvent.CartCleared(j.cartId(), j.shopperKey(), j.version(), at);
            case "PURCHASED" -> new CartEvent.CartPurchased(j.cartId(), j.shopperKey(), j.version(), at);
            default -> throw new IllegalArgumentException("unknown cart event type " + j.type());
        };
    }

    public static byte[] encode(ReminderIntent i) {
        return write(new IntentJson(SCHEMA_VERSION, i.key(), i.cartId(), i.version(), i.offsetIndex(), i.srcPartition(),
                i.scheduledFor().toEpochMilli(), i.sendBy().toEpochMilli()));
    }

    public static ReminderIntent decodeIntent(byte[] bytes) {
        IntentJson j = read(bytes, IntentJson.class);
        require(j.key() != null && j.cartId() != null, "intent needs key and cartId");
        return new ReminderIntent(j.key(), j.cartId(), j.version(), j.offsetIndex(), j.srcPartition(),
                Instant.ofEpochMilli(j.scheduledFor()), Instant.ofEpochMilli(j.sendBy()));
    }

    public static byte[] encode(Outcome o) {
        return write(new OutcomeJson(SCHEMA_VERSION, o.key(), o.cartId(), o.version(), o.arm(), o.kind(),
                o.at().toEpochMilli(), o.attempts()));
    }

    public static Outcome decodeOutcome(byte[] bytes) {
        OutcomeJson j = read(bytes, OutcomeJson.class);
        require(j.cartId() != null && j.kind() != null, "outcome needs cartId and kind");
        return new Outcome(j.key(), j.cartId(), j.version(), j.arm(), j.kind(), Instant.ofEpochMilli(j.at()), j.attempts());
    }

    public static byte[] encode(DeadLetter d) {
        ReminderIntent i = d.intent();
        return write(new DeadLetterJson(SCHEMA_VERSION, i.key(), i.cartId(), i.version(), i.offsetIndex(), i.srcPartition(),
                i.scheduledFor().toEpochMilli(), i.sendBy().toEpochMilli(), d.reason(), d.at().toEpochMilli()));
    }

    public static DeadLetter decodeDeadLetter(byte[] bytes) {
        DeadLetterJson j = read(bytes, DeadLetterJson.class);
        require(j.key() != null && j.cartId() != null, "dead letter needs key and cartId");
        ReminderIntent intent = new ReminderIntent(j.key(), j.cartId(), j.version(), j.offsetIndex(), j.srcPartition(),
                Instant.ofEpochMilli(j.scheduledFor()), Instant.ofEpochMilli(j.sendBy()));
        return new DeadLetter(intent, j.reason(), Instant.ofEpochMilli(j.failedAt()));
    }

    public static byte[] encode(SinkSend s) {
        return write(new SinkSendJson(SCHEMA_VERSION, s.key(), s.cartId(), s.at().toEpochMilli(), s.hasFirstName(), s.itemCount()));
    }

    public static SinkSend decodeSinkSend(byte[] bytes) {
        SinkSendJson j = read(bytes, SinkSendJson.class);
        require(j.key() != null && j.cartId() != null, "sink send needs key and cartId");
        return new SinkSend(j.key(), j.cartId(), Instant.ofEpochMilli(j.at()), j.hasFirstName(), j.itemCount());
    }

    private static byte[] write(Object value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot encode " + value.getClass().getSimpleName(), e);
        }
    }

    private static <T> T read(byte[] bytes, Class<T> type) {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (IOException e) {
            throw new IllegalArgumentException("malformed " + type.getSimpleName() + ": " + e.getMessage(), e);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }
}
```

Create `src/main/java/com/quince/cartrecovery/infra/kafka/Topics.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import com.quince.cartrecovery.model.Lane;
import java.time.Duration;
import java.util.List;

/** The seven topics of spec §5.1. All share one partition count P and are keyed by cartId. */
public final class Topics {
    public static final String CART_EVENTS = "cart-events";
    public static final String CART_EVENTS_DLQ = "cart-events-dlq";
    public static final String INTENTS_FAST = "reminder-intents-fast";
    public static final String INTENTS_SLOW = "reminder-intents-slow";
    public static final String REMINDER_DLQ = "reminder-dlq";
    public static final String OUTCOMES = "reminder-outcomes";
    public static final String SINK_SENDS = "sink-sends";

    public static final List<String> ALL =
            List.of(CART_EVENTS, CART_EVENTS_DLQ, INTENTS_FAST, INTENTS_SLOW, REMINDER_DLQ, OUTCOMES, SINK_SENDS);

    private Topics() {}

    public static String intents(Lane lane) {
        return lane == Lane.FAST ? INTENTS_FAST : INTENTS_SLOW;
    }

    /** DLQs keep 30 days, everything else 7 days. */
    public static Duration retention(String topic) {
        return topic.endsWith("-dlq") ? Duration.ofDays(30) : Duration.ofDays(7);
    }
}
```

Create `src/main/java/com/quince/cartrecovery/infra/kafka/KafkaClients.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Producer and admin client settings from spec §5.1. */
public final class KafkaClients {
    private KafkaClients() {}

    public static Map<String, Object> producerProps(String bootstrap) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    }

    /** Thread-safe; share one per process. */
    public static Producer<String, byte[]> producer(String bootstrap) {
        return new KafkaProducer<>(producerProps(bootstrap));
    }

    public static Admin admin(String bootstrap) {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
    }

    /** Blocks until the broker acknowledges; any failure is thrown so the caller retries or leaves work uncommitted. */
    public static void sendAndWait(Producer<String, byte[]> producer, ProducerRecord<String, byte[]> record) {
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted producing to " + record.topic(), e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("produce to " + record.topic() + " failed: " + e.getCause().getMessage(), e.getCause());
        }
    }
}
```

- [ ] **Step 4: Run the JDK-only tests to verify they pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.infra.kafka.*' --console=plain`
Expected: PASS, 8 tests (7 in `JsonCodecTest`, 1 in `KafkaClientsTest`).

- [ ] **Step 5: Write the Kafka test helper and the failing adapter and admin tests**

Create `src/integrationTest/java/com/quince/cartrecovery/infra/kafka/TestKafka.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** One single-node KRaft broker shared by every Kafka integration test in the JVM, with the 7 topics created once. */
public final class TestKafka {
    public static final int PARTITIONS = 3;

    private static KafkaContainer container;
    private static Producer<String, byte[]> producer;

    private TestKafka() {}

    public static synchronized String bootstrap() {
        if (container == null) {
            container = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));
            container.start();
            try (Admin admin = KafkaClients.admin(container.getBootstrapServers())) {
                new TopicAdmin(admin).createAll(PARTITIONS, 1, 1);
            }
        }
        return container.getBootstrapServers();
    }

    public static synchronized Producer<String, byte[]> producer() {
        if (producer == null) producer = KafkaClients.producer(bootstrap());
        return producer;
    }

    public static Admin admin() {
        return KafkaClients.admin(bootstrap());
    }

    /** Reads the topic from the beginning until {@code expected} records with the key prefix arrive or the timeout passes. */
    public static List<ConsumerRecord<String, byte[]>> read(String topic, String keyPrefix, int expected, Duration timeout) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
            long deadline = System.nanoTime() + timeout.toNanos();
            while (out.size() < expected && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(200))) {
                    if (r.key() != null && r.key().startsWith(keyPrefix)) out.add(r);
                }
            }
            return out;
        }
    }
}
```

Create `src/integrationTest/java/com/quince/cartrecovery/infra/kafka/KafkaAdaptersTest.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class KafkaAdaptersTest {
    private static final Instant T = Instant.parse("2026-01-01T09:00:00Z");
    private static final CartItem ITEM = new CartItem("SKU-1", "Linen Shirt", 1, 4990);
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final Producer<String, byte[]> producer = TestKafka.producer();
    private final String prefix = "b3-" + UUID.randomUUID() + "-";

    private static List<byte[]> values(List<ConsumerRecord<String, byte[]>> records) {
        return records.stream().map(ConsumerRecord::value).toList();
    }

    @Test
    void intentsGoToTheirLaneKeyedByCartId() {
        String cartId = prefix + "cart";
        List<ReminderIntent> intents = IntStream.range(0, 3)
                .mapToObj(i -> new ReminderIntent(new LedgerKey(cartId, 1, i).toString(), cartId, 1, i, 2, T, T.plusSeconds(300)))
                .toList();
        KafkaIntentPublisher publisher = new KafkaIntentPublisher(producer, 2);
        intents.forEach(publisher::publish);

        List<ConsumerRecord<String, byte[]>> fast = TestKafka.read(Topics.INTENTS_FAST, prefix, 2, WAIT);
        List<ConsumerRecord<String, byte[]>> slow = TestKafka.read(Topics.INTENTS_SLOW, prefix, 1, WAIT);
        assertEquals(intents.subList(0, 2), values(fast).stream().map(JsonCodec::decodeIntent).toList());
        assertEquals(intents.subList(2, 3), values(slow).stream().map(JsonCodec::decodeIntent).toList());
        assertEquals(cartId, fast.get(0).key());
    }

    @Test
    void outcomesAndDeadLettersRoundTrip() {
        String cartId = prefix + "cart";
        Outcome sent = new Outcome(cartId + ":3:0", cartId, 3, Arm.TREATMENT, OutcomeKind.SENT, T, 2);
        Outcome abandoned = new Outcome(null, cartId, 3, Arm.HOLDOUT, OutcomeKind.ABANDONED, T, 0);
        KafkaOutcomeRecorder recorder = new KafkaOutcomeRecorder(producer);
        recorder.record(sent);
        recorder.record(abandoned);
        assertEquals(List.of(sent, abandoned),
                values(TestKafka.read(Topics.OUTCOMES, prefix, 2, WAIT)).stream().map(JsonCodec::decodeOutcome).toList());

        ReminderIntent intent = new ReminderIntent(cartId + ":3:1", cartId, 3, 1, 0, T, T.plusSeconds(300));
        DeadLetter letter = new DeadLetter(intent, "permanent", T.plusSeconds(5));
        new KafkaDeadLetterQueue(producer).add(letter);
        assertEquals(List.of(letter),
                values(TestKafka.read(Topics.REMINDER_DLQ, prefix, 1, WAIT)).stream().map(JsonCodec::decodeDeadLetter).toList());
    }

    @Test
    void recordingSinkWritesOneRecordPerDeliveredSendWithoutPersonalData() throws Exception {
        Random scripted = new Random() {
            private final double[] draws = {0.1, 0.9};
            private int next;

            @Override
            public double nextDouble() { return draws[next++]; }
        };
        KafkaRecordingSink sink = new KafkaRecordingSink(producer, () -> T, 0.5, scripted);
        ReminderMessage message = new ReminderMessage(prefix + "cart:1:0", prefix + "cart", "shopper@example.com", "Ada", List.of(ITEM, ITEM));

        assertEquals(SendResult.TRANSIENT_FAILURE, sink.send(message), "draw 0.1 < 0.5 injects a failure");
        assertEquals(SendResult.SENT, sink.send(message));

        List<ConsumerRecord<String, byte[]>> records = TestKafka.read(Topics.SINK_SENDS, prefix, 2, Duration.ofSeconds(3));
        assertEquals(1, records.size(), "the injected failure delivered nothing and recorded nothing");
        Set<String> fields = new HashSet<>();
        new ObjectMapper().readTree(records.get(0).value()).fieldNames().forEachRemaining(fields::add);
        assertEquals(Set.of("schemaVersion", "key", "cartId", "at", "hasFirstName", "itemCount"), fields, "no name, shopper key or items");
        assertEquals(new JsonCodec.SinkSend(message.key(), message.cartId(), T, true, 2), JsonCodec.decodeSinkSend(records.get(0).value()));
    }

    @Test
    void sinkRecordsNoNameAndNoItems() {
        KafkaRecordingSink sink = new KafkaRecordingSink(producer, () -> T, 0.0, new Random(1));
        ReminderMessage message = new ReminderMessage(prefix + "cart:1:0", prefix + "cart", "s", null, List.of());
        assertEquals(SendResult.SENT, sink.send(message));
        List<ConsumerRecord<String, byte[]>> records = TestKafka.read(Topics.SINK_SENDS, prefix, 1, WAIT);
        assertEquals(new JsonCodec.SinkSend(message.key(), message.cartId(), T, false, 0), JsonCodec.decodeSinkSend(records.get(0).value()));
    }
}
```

Create `src/integrationTest/java/com/quince/cartrecovery/infra/kafka/TopicAdminTest.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class TopicAdminTest {
    @Test
    void createsAllSevenTopicsWithEqualPartitionsIdempotently() throws Exception {
        try (Admin admin = TestKafka.admin()) {
            new TopicAdmin(admin).createAll(TestKafka.PARTITIONS, 1, 1); // TestKafka already created them once

            Map<String, TopicDescription> topics = admin.describeTopics(Topics.ALL).allTopicNames().get();
            assertEquals(7, topics.size());
            topics.values().forEach(t -> assertEquals(TestKafka.PARTITIONS, t.partitions().size(), t.name()));

            ConfigResource dlq = new ConfigResource(ConfigResource.Type.TOPIC, Topics.REMINDER_DLQ);
            ConfigResource events = new ConfigResource(ConfigResource.Type.TOPIC, Topics.CART_EVENTS);
            Map<ConfigResource, Config> configs = admin.describeConfigs(List.of(dlq, events)).all().get();
            assertEquals(Long.toString(Duration.ofDays(30).toMillis()), configs.get(dlq).get("retention.ms").value());
            assertEquals(Long.toString(Duration.ofDays(7).toMillis()), configs.get(events).get("retention.ms").value());
            assertEquals("1", configs.get(events).get("min.insync.replicas").value());
        }
    }

    @Test
    void partitionCountsListsEveryExistingTopic() {
        try (Admin admin = TestKafka.admin()) {
            Map<String, Integer> counts = new TopicAdmin(admin).partitionCounts();
            assertEquals(Topics.ALL.size(), counts.size());
            for (String topic : Topics.ALL) assertEquals(TestKafka.PARTITIONS, counts.get(topic), topic);
        }
    }

    @Test
    void refusesAPartitionMismatchNamingTheTopic() {
        try (Admin admin = TestKafka.admin()) {
            TopicAdmin topics = new TopicAdmin(admin);
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> topics.verify(TestKafka.PARTITIONS + 1));
            assertTrue(e.getMessage().contains(Topics.CART_EVENTS), e.getMessage());
            assertThrows(IllegalStateException.class, () -> topics.createAll(TestKafka.PARTITIONS + 1, 1, 1));
        }
    }
}
```

- [ ] **Step 6: Run them to verify they fail**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.kafka.*' --console=plain`
Expected: FAIL at `compileIntegrationTestJava` with `cannot find symbol ... class TopicAdmin`, `class KafkaIntentPublisher`, `class KafkaOutcomeRecorder`, `class KafkaDeadLetterQueue`, `class KafkaRecordingSink`.

- [ ] **Step 7: Write the topic admin and the four adapters**

Create `src/main/java/com/quince/cartrecovery/infra/kafka/TopicAdmin.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/** Creates the 7 topics with equal P (spec §5.1) and refuses to proceed when an existing topic has a different P. */
public final class TopicAdmin {
    private final Admin admin;

    public TopicAdmin(Admin admin) {
        this.admin = admin;
    }

    public void createAll(int partitions, int replicationFactor, int minInsyncReplicas) {
        List<NewTopic> topics = Topics.ALL.stream()
                .map(t -> new NewTopic(t, partitions, (short) replicationFactor).configs(Map.of(
                        "retention.ms", Long.toString(Topics.retention(t).toMillis()),
                        "min.insync.replicas", Integer.toString(minInsyncReplicas))))
                .toList();
        admin.createTopics(topics).values().forEach((name, future) -> {
            try {
                future.get();
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw new IllegalStateException("cannot create topic " + name + ": " + e.getCause().getMessage(), e.getCause());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted creating topic " + name, e);
            }
        });
        verify(partitions);
    }

    public void verify(int partitions) {
        Map<String, Integer> counts = partitionCounts();
        for (String topic : Topics.ALL) {
            Integer actual = counts.get(topic);
            if (actual == null) throw new IllegalStateException("topic " + topic + " is missing");
            if (actual != partitions) {
                throw new IllegalStateException("topic " + topic + " has " + actual + " partitions, expected " + partitions);
            }
        }
    }

    /** Partition count per existing topic of {@link Topics#ALL}; a missing topic is absent from the map. */
    public Map<String, Integer> partitionCounts() {
        Map<String, Integer> counts = new HashMap<>();
        admin.describeTopics(Topics.ALL).topicNameValues().forEach((name, future) -> {
            try {
                counts.put(name, future.get().partitions().size());
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    throw new IllegalStateException("topic check failed: " + e.getCause().getMessage(), e.getCause());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted checking topics", e);
            }
        });
        return counts;
    }
}
```

Create `src/main/java/com/quince/cartrecovery/infra/kafka/KafkaIntentPublisher.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.ports.IntentPublisher;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Publishes to the fast lane for offsetIndex < fastOffsets, otherwise the slow lane; blocks until acknowledged. */
public final class KafkaIntentPublisher implements IntentPublisher {
    private final Producer<String, byte[]> producer;
    private final int fastOffsets;

    public KafkaIntentPublisher(Producer<String, byte[]> producer, int fastOffsets) {
        this.producer = producer;
        this.fastOffsets = fastOffsets;
    }

    @Override
    public void publish(ReminderIntent intent) {
        String topic = Topics.intents(Lane.of(intent.offsetIndex(), fastOffsets));
        KafkaClients.sendAndWait(producer, new ProducerRecord<>(topic, intent.cartId(), JsonCodec.encode(intent)));
    }
}
```

Create `src/main/java/com/quince/cartrecovery/infra/kafka/KafkaOutcomeRecorder.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/** reminder-outcomes; blocks so an outcome is durable before the ledger finish that follows it. */
public final class KafkaOutcomeRecorder implements OutcomeRecorder {
    private final Producer<String, byte[]> producer;

    public KafkaOutcomeRecorder(Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    @Override
    public void record(Outcome outcome) {
        KafkaClients.sendAndWait(producer, new ProducerRecord<>(Topics.OUTCOMES, outcome.cartId(), JsonCodec.encode(outcome)));
    }
}
```

Create `src/main/java/com/quince/cartrecovery/infra/kafka/KafkaDeadLetterQueue.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.ports.DeadLetterQueue;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/** reminder-dlq; blocks so every DEAD row has a replayable record before finish(DEAD). */
public final class KafkaDeadLetterQueue implements DeadLetterQueue {
    private final Producer<String, byte[]> producer;

    public KafkaDeadLetterQueue(Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    @Override
    public void add(DeadLetter letter) {
        KafkaClients.sendAndWait(producer,
                new ProducerRecord<>(Topics.REMINDER_DLQ, letter.intent().cartId(), JsonCodec.encode(letter)));
    }
}
```

Create `src/main/java/com/quince/cartrecovery/infra/kafka/KafkaRecordingSink.java`:

```java
package com.quince.cartrecovery.infra.kafka;

import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.NotificationSink;
import java.util.Random;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Stands in for the notification gateway: never sends, records one {@code sink-sends} record per delivered send
 * (key, cartId, at, hasFirstName, itemCount; no personal data). {@code failureRate} injects transient failures
 * for load tests; an injected failure delivers and records nothing.
 */
public final class KafkaRecordingSink implements NotificationSink {
    private final Producer<String, byte[]> producer;
    private final Clock clock;
    private final double failureRate;
    private final Random random;

    public KafkaRecordingSink(Producer<String, byte[]> producer, Clock clock, double failureRate, Random random) {
        this.producer = producer;
        this.clock = clock;
        this.failureRate = failureRate;
        this.random = random;
    }

    @Override
    public SendResult send(ReminderMessage m) {
        if (failureRate > 0 && random.nextDouble() < failureRate) return SendResult.TRANSIENT_FAILURE;
        boolean hasFirstName = m.firstName() != null && !m.firstName().isBlank();
        JsonCodec.SinkSend record = new JsonCodec.SinkSend(m.key(), m.cartId(), clock.now(), hasFirstName, m.items().size());
        try {
            KafkaClients.sendAndWait(producer, new ProducerRecord<>(Topics.SINK_SENDS, m.cartId(), JsonCodec.encode(record)));
        } catch (IllegalStateException unknownDelivery) {
            // Like a gateway timeout: the dispatcher retries under the same idempotency key.
            return SendResult.TRANSIENT_FAILURE;
        }
        return SendResult.SENT;
    }
}
```

- [ ] **Step 8: Run the Kafka integration tests to verify they pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.infra.kafka.*' --console=plain`
Expected: PASS, 7 tests (4 in `KafkaAdaptersTest`, 3 in `TopicAdminTest`).

- [ ] **Step 9: Confirm the JDK-only build is still green**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`, including `JsonCodecTest` and `KafkaClientsTest`.

- [ ] **Step 10: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/infra/kafka src/test/java/com/quince/cartrecovery/infra/kafka src/integrationTest/java/com/quince/cartrecovery/infra/kafka
git commit -m "$(cat <<'EOF'
Add JSON codec, Kafka producers, recording sink and topic admin

schemaVersion 1 payloads with unknown fields ignored; lane-routed intent
publisher, outcome recorder and DLQ that block until acknowledged; sink-sends
recorder without personal data; idempotent topic creation with equal P.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Self-review against the spec

- §5.2 scripts: all seven, Redis `TIME` only, monotonic upsert with `CHECK_ABANDON` as −1, equal no-op keeping the lease, ack/release if unchanged, remove if stored ≤ given, generation fencing with max-within-generation, 5 s staleness (B0, B2). Cluster-safe `{s}` hash tags (B0 keys). `epoch` sentinel and `run_id`/role (B2 `RedisMeta`).
- §5.3 carts: SET/REMOVE sets and conditions for edit, resume, purchase/clear, abandonment (with `REMOVE` when ineligible), end of sequence; `openShard`/`openUntil` hour rounding; KEYS_ONLY sparse GSI; `ttl` 30 days; `ConsistentRead` on base reads including `BatchGetItem` (B1).
- §5.3 ledger: create-or-takeover claim with `attempts` increment and fresh token, `nextAttemptAt = leaseUntil` while `SENDING`, `retryShard` on every non-final row, INCLUDE(`srcPartition`) sparse GSI, token-conditioned transitions, `reopen` only from `DEAD`, `highestOffsetIndex` via `begins_with` descending limit 1, `ttl` 30 days (B1). `recovery-meta` item (B1).
- §5.1: 7 topics, equal P, retention 7/30 days, `min.insync.replicas`, `acks=all` and idempotence, keyed by cartId, lane by `Lane.of`, `sink-sends` without personal data, `schemaVersion` everywhere (B3). The DLQ record for undeserializable intents (reason `poison`, original bytes) is written by C0b's loop, not by `KafkaDeadLetterQueue`.
- §6.3 client sizing: `DynamoTables.client(endpoint, maxConnections)`; C1 passes `MAX_IN_FLIGHT`.
- Not in this thread by design: `SystemClock`, consumers, the Redis-restart contract test (C2), `init` orchestration (C1c).

## Contract issues

All resolved by controller rulings (master plan):

1. Time source of `TimerStoreContract` and `WatermarkContract`: resolved by controller ruling R3 (store-time hooks, 1 s lease, Redis `advance` sleeps).
2. Millisecond boundaries on a real clock: resolved by controller ruling R3 (exact boundaries only in thread A's in-memory subclasses; none here).
3. Millisecond precision: the contracts use `at(...)` truncated to milliseconds; nothing to change.
4. `WatermarkContract` factory: resolved by controller ruling R3 (`newWatermark()`; the Redis subclass reads 3 partitions).
5. Adapter semantics: resolved by controller ruling R4 (thread A's semantics; B1 now keeps the stored `firstName` on an edit without one).
6. Review Focus 2 premise: resolved by controller ruling R5 (master Review Focus 2 corrected).
7. `sink-sends` records only successful sends: controller ruling R17.
