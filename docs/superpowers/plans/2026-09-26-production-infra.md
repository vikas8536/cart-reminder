# Production Infrastructure Implementation Plan (master)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. This master file holds the frozen contracts, the dependency graph, and model selection; each thread's detailed tasks live in `docs/superpowers/plans/2026-09-26-production-infra/<thread>.md`.

**Goal:** Evolve the in-memory abandoned-cart pipeline into separately deployable roles on real Kafka, Redis, and DynamoDB, with contract, end-to-end, and load tests, while keeping the JDK-only in-memory mode.

**Architecture:** Core classes stay shared and depend only on ports. Thread A reshapes the core and in-memory adapters to the spec's semantics; thread B implements the same ports on DynamoDB, Redis, and Kafka and proves parity with shared contract tests; thread C builds the runtime (config, health, batch consumer loop, send budget, breaker, roles, compose); thread D builds the load generator; thread E updates the docs and runs the load test. Threads run in parallel wherever the dependency graph allows, each task in its own git worktree.

**Tech Stack:** Java 21 (virtual threads), Gradle 8.10.2 wrapper, JUnit 5, `kafka-clients`, Lettuce, AWS SDK v2 DynamoDB, Jackson, Testcontainers, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-09-25-production-infra-design.md` (revision 5). Executors read the spec section each task cites; the spec is the binding authority, this plan is its argument.

## Global Constraints

- Java 21, Gradle wrapper 8.10.2, JUnit 5; `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current` on this machine.
- Package root `com.quince.cartrecovery`; existing sub-packages `model`, `ports`, `core`, `inmemory`; new sub-packages `infra.dynamo`, `infra.redis`, `infra.kafka`, `app`, `loadgen`.
- `./gradlew test` needs only a JDK and must stay green after every task; infra tests live in the `integrationTest` source set with `@Testcontainers(disabledWithoutDocker = true)`; `./gradlew check` runs both.
- Core classes depend only on `model` and `ports`; nothing in `core` imports a client library.
- No real sends: `NotificationSink` implementations only record.
- Recovery defaults unchanged: window 30 min, offsets 30 min / 1 h / 24 h, lateness bounds 5 min / 5 min / 30 min, frequency cap 3 per 7 days, holdout 10%, max send attempts 5, retry base 1 min.
- Runtime defaults (spec §7.1): `LEASE` 90 s, `GATEWAY_TIMEOUT` 30 s, `CLOCK_SKEW` 5 s, `FAST_OFFSETS` 2, `FAST_RESERVE` 0.3, `RETRY_POLL` 1 s, `RECONCILE_INTERVAL` 5 min, `MAX_IN_FLIGHT` 256, `SHARDS` and `PARTITIONS` 8 locally; startup rejects `LEASE < 3 × GATEWAY_TIMEOUT`.
- Every payload carries `schemaVersion` (value 1); Jackson with `FAIL_ON_UNKNOWN_PROPERTIES` off.
- Ledger key format `cartId:version:offsetIndex`, parsed from the right so a cart id may contain `:`.
- Pinned dependencies (T0's `build.gradle.kts`, authoritative for every thread): `kafka-clients` 4.3.1, `lettuce-core` 6.7.1.RELEASE, AWS SDK v2 BOM 2.54.17 (`dynamodb`, `apache5-client`), Jackson BOM 2.19.1, Testcontainers BOM 1.21.4 (`junit-jupiter`, `kafka`; brings `testcontainers`), JUnit BOM 5.10.2, `slf4j-simple` 2.0.16. `integrationTest` runs with `-Djdk.tracePinnedThreads=full`.
- Container images, one tag each for tests and compose (each verified with `docker manifest inspect`): `apache/kafka:4.3.1`, `redis:7.4-alpine`, `amazon/dynamodb-local:3.3.1`.
- Defaults the spec leaves open: `MAX_SEND_RATE` 1000 (per replica), `HEALTH_PORT` 8081.
- Load runs select DynamoDB Local's in-memory mode with `DYNAMO_STORAGE=-inMemory docker compose ... up -d`. Deliberate deviation from the spec §7.3 wording ("`-inMemory` under the `load` profile"): a compose profile selects services and cannot change another service's flags.
- `CircuitBreaker` (C0a): over the last `window` attempts a transient ratio above `threshold` opens it for `openFor`; while open, `send` returns `TRANSIENT_FAILURE` without calling the delegate; `isOpen()` is true until `openFor` has elapsed and while the single half-open probe is in flight, then false so paused consumers resume and supply that probe; a successful probe closes it and clears the window, a failed one reopens it. Dispatcher arguments: 100, 0.5, 30 s.
- Every commit ends with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- The frozen contracts below may not be changed by a task. A task that finds a contract wrong stops and reports `BLOCKED` with the reason; the controller rules and updates this file.

## Review Focus

Input classes the spec implies but its tests do not name, most likely to bite first. Each has a pinned test in its owning task.

1. A cart id containing `:` or `|` (both are delimiters in the ledger key and the Redis packed timer value): keys and timers round-trip exactly. Pinned in A1 (`LedgerKey`) and B0 (Lua packing).
2. A cart with no `firstName` and an empty item list, through JSON and DynamoDB (which accepts empty strings in non-key attributes; only a null must be omitted, never written as an attribute): stored and read back as absent / empty, message built without a name. Pinned in B1 and B3.
3. `sendBy` exactly equal to now, and a claim exactly at `leaseUntil`: not late, and takeover allowed (boundaries inclusive as the spec's `<=` conditions say). Pinned in A3 and B1.
4. A rebalance that revokes a partition while its groups are still in flight: the loop commits only completed prefixes and does not commit past an unfinished record. Pinned in C0b.
5. A malformed environment value (`WINDOW=30m` instead of `PT30M`, `SHARDS=abc`): the process exits with a one-line message naming the variable, not a stack trace. Pinned in C0a.

---

## 1. Frozen contracts

Thread A creates these exact types in A1; every other thread codes against them. Signatures are exact; Javadoc in A1 may add detail but not change behaviour.

### 1.1 Model (`com.quince.cartrecovery.model`)

```java
// Existing records gain trailing components; old constructors stay as delegating overloads.
record CartEvent.CartEdited(String cartId, String shopperKey, long version, Instant occurredAt,
                            List<CartItem> items, String firstName)          // firstName nullable
    // + overload CartEdited(cartId, shopperKey, version, occurredAt, items) → firstName null
record CartRecord(String cartId, String shopperKey, CartStatus status, long version,
                  Instant lastActivityAt, List<CartItem> items, Arm arm, List<Instant> sequenceStarts,
                  String firstName, int srcPartition)                         // srcPartition -1 = unknown
    // + overload with the original 8 components → firstName null, srcPartition -1
    // activity(...) caps items at 50; new method:
    // List<Instant> startsWith(Instant start, Instant now, Duration frequencyWindow)
    //   → sequenceStarts pruned to [now - frequencyWindow, now] plus start
record Timer(String cartId, TimerKind kind, long version, int offsetIndex, Instant dueAt, int srcPartition)
    // + overload with the original 5 components → srcPartition -1
    // static checkAbandon(cartId, version, dueAt, srcPartition), reminder(cartId, version, offsetIndex, dueAt, srcPartition)
    // + the original 3/4-arg factories → srcPartition -1
record ReminderIntent(String key, String cartId, long version, int offsetIndex, int srcPartition,
                      Instant scheduledFor, Instant sendBy)
record ReminderMessage(String key, String cartId, String shopperKey, String firstName, List<CartItem> items)
record LedgerKey(String cartId, long version, int offsetIndex)
    // String toString() → "cartId:version:offsetIndex"; static LedgerKey parse(String) parses from the right
enum OutcomeKind { ABANDONED, SENT, SKIPPED_LATE, CANCELLED, DEAD, SUPERSEDED }   // SUPERSEDED: review fixes 2026-09-28
record Outcome(String key, String cartId, long version, Arm arm, OutcomeKind kind, Instant at, int attempts)
    // key null for ABANDONED. arm is meaningful only for ABANDONED (the per-arm denominator); reminder
    // outcomes (SENT, SKIPPED_LATE, CANCELLED, DEAD) always carry Arm.TREATMENT, since only treatment carts get intents.
record DeadLetter(ReminderIntent intent, String reason, Instant at)          // REASON_POISON = "poison"
enum Lane { FAST, SLOW; static Lane of(int offsetIndex, int fastOffsets) }   // offsetIndex < fastOffsets → FAST
record DispatchConfig(Duration lease, Duration gatewayTimeout, Duration clockSkew, int fastOffsets)
    // static defaults() = (90 s, 30 s, 5 s, 2); constructor rejects lease < 3 × gatewayTimeout
final class Shards { static int of(String cartId, int shards) }             // floorMod(cartId.hashCode(), shards)
sealed interface ClaimResult {
    record Claimed(String token, int attempts, Instant sendBy, int srcPartition, Instant leaseUntil) implements ClaimResult {}
    record NotClaimed(String reason) implements ClaimResult {}                // "final" or "leased"
}
record DueRetry(String key, int srcPartition, Instant sendBy)   // sendBy: review fixes 2026-09-28; GSI projects it
sealed interface TimerDecision {
    record Ack() implements TimerDecision {}
    record Release(Duration delay) implements TimerDecision {}
}
enum HandleResult { DONE, HOLD }
```

`NotificationIntent` and `OutboxEntry` are deleted in A1.

### 1.2 Ports (`com.quince.cartrecovery.ports`)

```java
interface Clock { Instant now(); }
interface ArmAssigner { Arm assign(String shopperKey); }                                  // unchanged
interface CartStateStore {
    Optional<CartRecord> get(String cartId);                                             // consistent
    List<CartRecord> getAll(Collection<String> cartIds);                                 // consistent, input order, absent ids omitted
    Optional<CartRecord> applyEvent(CartEvent event, Arm arm, int srcPartition);         // empty when stale (version <= stored)
    boolean markAbandoned(CartRecord record, List<Instant> sequenceStarts, boolean eligible); // cond version = :v AND status = ACTIVE
    boolean endSequence(String cartId, long version);                                    // cond version = :v AND status = ABANDONED
    Stream<String> openCartIds(int shard, Instant now);                                  // carts with a next step, openUntil >= now
}
interface TimerStore {
    record Upsert(boolean written, Optional<Timer> displaced) {}   // displaced: the stored timer the write overwrote
    Upsert upsert(Timer timer);                           // only if (version, offsetIndex) greater; equal data is a no-op (displaces nothing)
    Optional<Timer> remove(String cartId, long version);  // only if stored version <= version; returns the removed timer (review fixes 2026-09-28)
    List<Timer> claimDue(int limit);                      // due by the store's time source; leases them
    void release(Timer timer, Duration delay);            // re-due after delay if unchanged
    void ack(Timer timer);                                // remove if unchanged
    Set<String> existing(int shard, Collection<String> cartIds);
}
interface Watermark {
    void publish(int partition, long generation, Instant eventTime);
    Instant current(int srcPartition);                    // Instant.EPOCH if unknown or stale; srcPartition -1 → min over all
    Instant now();                                        // the watermark's time source
}
interface IntentPublisher { void publish(ReminderIntent intent); }                       // blocks until acknowledged
interface SendLedger {
    ClaimResult claim(String key, Instant sendBy, int srcPartition, Instant now);
    boolean markRetry(String key, String token, Instant nextAt);
    boolean finish(String key, String token, OutcomeKind outcome, String reason);
    List<DueRetry> dueRetries(int shard, Instant now, int limit);
    boolean reopen(String key, Instant now);
    int highestOffsetIndex(String cartId, long version);  // -1 if none
}
interface NotificationSink { SendResult send(ReminderMessage message); }
interface OutcomeRecorder { void record(Outcome outcome); }
interface DeadLetterQueue { void add(DeadLetter letter); }
interface SendBudget { boolean tryAcquire(Lane lane); void release(Lane lane); }        // never blocks; release returns an unused token, capped at capacity (review fixes 2026-09-28)
```

`Outbox` is deleted.

### 1.3 Core APIs (`com.quince.cartrecovery.core`)

```java
Metrics                    // unchanged API: increment(String), get(String), snapshot(); thread-safe after A1
ReminderPolicy(RecoveryConfig)                          boolean eligible(CartRecord, Instant now)   // unchanged
AbandonmentDetector(RecoveryConfig, CartStateStore, TimerStore, ArmAssigner, OutcomeRecorder, Metrics)
                                                        void handle(CartEvent event, int srcPartition)
ReminderScheduler(RecoveryConfig, DispatchConfig, CartStateStore, TimerStore, Watermark,
                  IntentPublisher, OutcomeRecorder, Metrics)
                                                        TimerDecision onTimer(Timer timer)  // throws on transient errors
Dispatcher(RecoveryConfig, DispatchConfig, CartStateStore, SendLedger, Watermark, SendBudget,
           NotificationSink, OutcomeRecorder, DeadLetterQueue, Clock, Metrics)
                                                        HandleResult handle(ReminderIntent intent)
                                                        void retryDue(int shard, int limit)
                                                        void replay(List<DeadLetter> letters)
Reconciler(RecoveryConfig, CartStateStore, TimerStore, SendLedger, OutcomeRecorder, Clock, Metrics)
                                                        void reconcileShard(int shard)
```

### 1.4 In-memory adapters (`com.quince.cartrecovery.inmemory`)

```java
FakeClock(Instant)                                       // unchanged
InMemoryCartStateStore(RecoveryConfig config, int shards)
PriorityQueueTimerStore(Clock clock, Duration lease)     // + Optional<Instant> nextDueAt(), int size(), void clear()
InMemoryWatermark(Clock clock)                           // + void setLagging(int partition, Instant eventTime), void clearLag()
InMemoryIntentQueue implements IntentPublisher          // + List<ReminderIntent> drain(), int size()
InMemorySendLedger(Duration lease, int shards)           // + Optional<Instant> nextRetryAt(), int size(), Optional<String> status(String key)
InMemoryOutcomeRecorder implements OutcomeRecorder      // + List<Outcome> all()
RecordingNotificationSink(Clock)                         // records ReminderMessage; scriptOutcomes(...), sent(), attempts()
InMemoryDeadLetterQueue implements DeadLetterQueue       // + List<DeadLetter> drain(), int size()
UnlimitedSendBudget implements SendBudget                // always true
HashArmAssigner(String salt, int holdoutPercent)         // unchanged
```

Shared contract tests: A1 creates abstract classes in `src/test/java/com/quince/cartrecovery/contract/`: `CartStateStoreContract` (`newStore(RecoveryConfig config, int shards)`), `SendLedgerContract` (`newLedger(Duration lease, int shards)`), `TimerStoreContract` (`newStore(Duration lease)`, `now()`, `advance(Duration)`), `WatermarkContract` (`newWatermark()`, `advance(Duration)`), plus in-memory subclasses in `test`. Only the timer-store and watermark contracts have time hooks; a Redis subclass sleeps in `advance`. Millisecond-exact time boundaries are tested only in the in-memory subclasses, never in the abstract contracts or thread B's subclasses. Every factory returns an empty store (no other test's timers, watermarks or rows). Thread B subclasses them in `integrationTest`.

### 1.5 Runtime contracts (thread C, created in C0a/C0b)

```java
// com.quince.cartrecovery.app
record InfraConfig(RecoveryConfig recovery, DispatchConfig dispatch,
                   String kafkaBootstrap, String redisUrl, String dynamoEndpoint /* nullable */,
                   int shards, int partitions, int replicationFactor, int minInsyncReplicas,
                   double maxSendRate, double fastReserve, double sendFailureRate,
                   Duration reconcileInterval, Duration retryPoll, int maxInFlight, int healthPort) {
    static InfraConfig fromEnv(Map<String, String> env);   // throws ConfigException with a one-line message naming the variable
    String hash();                                          // stable hash of the effective config, logged at startup
}
final class ConfigException extends RuntimeException { ConfigException(String message); }
final class Health {
    void beat(String loop);                                 // loop made progress (including backoff or pause)
    void setReady(String key, String value);                // shown on /ready
    boolean healthy(Duration maxSilence);                   // every registered loop beat within maxSilence
}
final class HealthServer implements AutoCloseable {
    HealthServer(int port, Health health, Metrics metrics, Duration maxSilence);  // /health, /ready, /metrics
}
interface Role { String name(); void run(InfraConfig config, Health health, Metrics metrics) throws Exception; }
    // run returns when its thread is interrupted. Main's shutdown hook (SIGTERM) interrupts the role thread and
    // waits up to 30 s for run to return; RoleThread.close() in tests does the same.
final class TokenBucket implements SendBudget {
    TokenBucket(double ratePerSecond, double fastReserve, LongSupplier nanoTime);
    boolean tryAcquire(Lane lane);                          // FAST takes any token; SLOW only while tokens > fastReserve × capacity
    boolean slowAllowed(); boolean anyAvailable();          // used to pause lane consumers
}
final class CircuitBreaker implements NotificationSink {
    CircuitBreaker(NotificationSink delegate, int window, double threshold, Duration openFor, Clock clock);
    SendResult send(ReminderMessage m); boolean isOpen();   // opens when transient ratio over the last `window` > threshold; half-open probe
}
final class BatchConsumerLoop<V> implements AutoCloseable {
    enum Verdict { DONE, HOLD }                             // HOLD: pause the record's partition, seek back to it, commit below it
    interface Handler<V> { Verdict handle(ConsumerRecord<String, V> record) throws Exception; }
    interface Hooks<V> {
        default void beforePoll(Consumer<String, V> consumer) {}                          // poll thread only, every iteration
        default void afterCommit(Consumer<String, V> consumer, Map<TopicPartition, Long> committed, int generation) {}
            // after every commit (incl. on revoke), and once per iteration that committed nothing with an empty map
    }
    record Settings(String groupId, List<String> topics, int maxPollRecords, Duration pollTimeout,
                    int maxInFlight, String dlqTopic /* nullable */) {}
    BatchConsumerLoop(Map<String, Object> consumerProps, Settings settings, Deserializer<V> valueDeserializer,
                      Handler<V> handler, Hooks<V> hooks, Producer<String, byte[]> dlqProducer,
                      Health health, Metrics metrics);
    void pauseWhile(Predicate<TopicPartition> shouldPause); // evaluated every iteration; resumes when false
    void run();                                             // until close(); groups by key on virtual threads
    void close();
}
final class PoisonException extends RuntimeException { PoisonException(String message, Throwable cause); }
// Handler throws PoisonException → record to dlqTopic and commit; any other exception → retry the group
// in process (3 attempts, backoff), then seek back to the last committed offset with backoff capped at 30 s.
```

Roles in thread C1 use only these members.

### 1.6 Infra adapter APIs (thread B, created in B0 to B3)

Reconciled by controller rulings R6 and R7: threads C1 and D1 call exactly these members (all public).

```java
// com.quince.cartrecovery.infra.dynamo
final class DynamoTables {
    static final String CARTS = "carts", SEND_LEDGER = "send-ledger", RECOVERY_META = "recovery-meta",
                        OPEN_BY_SHARD = "open-by-shard", RETRYING_BY_SHARD = "retrying-by-shard";
    static DynamoDbClient client(String endpoint /* null or blank: real AWS */, int maxConnections);
    static void createAll(DynamoDbClient ddb);                            // carts, send-ledger, recovery-meta; idempotent
    static void createCarts(DynamoDbClient ddb, String table);
    static void createLedger(DynamoDbClient ddb, String table);
    static void createMeta(DynamoDbClient ddb, String table);
}
final class DynamoCartStateStore implements CartStateStore {
    DynamoCartStateStore(DynamoDbClient ddb, String table, RecoveryConfig config, int shards);
    static Instant openUntil(Instant lastActivityAt, RecoveryConfig config);   // rounded up to the next whole hour
}
final class DynamoSendLedger implements SendLedger {
    DynamoSendLedger(DynamoDbClient ddb, String table, Duration lease, int shards);
    static String sk(long version, int offsetIndex);
}
final class RecoveryMetaStore {
    record Meta(int shards, int partitions, boolean paused, String redisRunId, String redisRole, Instant redisChangeAt) {}
    RecoveryMetaStore(DynamoDbClient ddb);                                // table "recovery-meta"
    static void createTable(DynamoDbClient ddb);                          // idempotent
    void init(int shards, int partitions);                                // writes S and P only if absent, never overwrites
    Meta read();                                                          // consistent; IllegalStateException if init has not run
    void setPaused(boolean paused);
    void setRedisIdentity(String runId, String role);                     // also clears redisChangeAt
    boolean markRedisChange(Instant at, String staleRunId, String staleRole); // earliest unrepaired change; no-op once the identity was replaced
}
// com.quince.cartrecovery.infra.redis
final class RedisTimerStore implements TimerStore {
    RedisTimerStore(StatefulRedisConnection<String, String> connection, int shards, Duration lease);
    static String pack(Timer t);  static Timer unpack(String cartId, String packed);
}
final class RedisWatermark implements Watermark {
    RedisWatermark(StatefulRedisConnection<String, String> connection, int partitions);                     // 5 s staleness
    RedisWatermark(StatefulRedisConnection<String, String> connection, int partitions, Duration staleAfter);
}
final class RedisMeta {                                                   // the only Redis epoch / identity helper
    static final String EPOCH = "epoch";
    RedisMeta(StatefulRedisConnection<String, String> connection);
    boolean epochPresent();  void writeEpoch();  String runId();  String role();
}
// com.quince.cartrecovery.infra.kafka
final class Topics {
    static final String CART_EVENTS = "cart-events", CART_EVENTS_DLQ = "cart-events-dlq",
                        INTENTS_FAST = "reminder-intents-fast", INTENTS_SLOW = "reminder-intents-slow",
                        REMINDER_DLQ = "reminder-dlq", OUTCOMES = "reminder-outcomes", SINK_SENDS = "sink-sends";
    static final List<String> ALL;                                         // the 7 topics
    static String intents(Lane lane);  static Duration retention(String topic);
}
final class KafkaClients {
    static Map<String, Object> producerProps(String bootstrap);  static Producer<String, byte[]> producer(String bootstrap);
    static Admin admin(String bootstrap);
    static void sendAndWait(Producer<String, byte[]> producer, ProducerRecord<String, byte[]> record);
}
final class TopicAdmin {
    TopicAdmin(Admin admin);
    void createAll(int partitions, int replicationFactor, int minInsyncReplicas);   // creates missing, then verify
    void verify(int partitions);                                                     // IllegalStateException naming the topic
    Map<String, Integer> partitionCounts();                                          // existing topics only
}
final class JsonCodec {                                                   // epoch-millisecond times, schemaVersion 1
    static final int SCHEMA_VERSION = 1;
    record SinkSend(String key, String cartId, Instant at, boolean hasFirstName, int itemCount) {}
    static byte[] encode(CartEvent e);  static byte[] encode(ReminderIntent i);  static byte[] encode(Outcome o);
    static byte[] encode(DeadLetter d);  static byte[] encode(SinkSend s);
    static CartEvent decodeCartEvent(byte[] b);  static ReminderIntent decodeIntent(byte[] b);
    static Outcome decodeOutcome(byte[] b);  static DeadLetter decodeDeadLetter(byte[] b);
    static SinkSend decodeSinkSend(byte[] b);                              // decoders throw IllegalArgumentException
}
final class KafkaIntentPublisher implements IntentPublisher { KafkaIntentPublisher(Producer<String, byte[]> producer, int fastOffsets); }
final class KafkaOutcomeRecorder implements OutcomeRecorder { KafkaOutcomeRecorder(Producer<String, byte[]> producer); }
final class KafkaDeadLetterQueue implements DeadLetterQueue { KafkaDeadLetterQueue(Producer<String, byte[]> producer); }
final class KafkaRecordingSink implements NotificationSink {
    KafkaRecordingSink(Producer<String, byte[]> producer, Clock clock, double failureRate, Random random);
    // one sink-sends record per successful send only; an injected or produce failure returns TRANSIENT_FAILURE, records nothing
}
```

## 2. Threads, tasks, and dependencies

| Task | Scope | Depends on | Model | Why this model |
|---|---|---|---|---|
| **T0** | Gradle: pinned dependency versions, `integrationTest` source set (classpath includes `test` output), `check` wiring, test `Await` helper | none | sonnet | Build config plus a version lookup; no domain judgment |
| **A1** | Model and ports per §1, in-memory adapters with real leases and tokens, thread-safe `Metrics`, shared contract tests run on in-memory | T0 | opus | Defines every semantic the other 16 tasks build on; mistakes cascade |
| **A2** | `AbandonmentDetector` (timer first, `srcPartition`) and `ReminderScheduler` (gate, release delay, re-arm, `sendBy`, `endSequence`, poison, `ABANDONED` outcomes) | A1 | sonnet | Well-specified by spec §6.2; plan carries full code |
| **A3** | `Dispatcher.handle` steps 1 to 7, `retryDue` (token before claim), `replay`, lease check, produce-before-finish | A2 | opus | Ordering and fencing subtleties; the review rounds concentrated here |
| **A4** | `Reconciler` key diff, `Pipeline` (intent drain, retry stops, detector-lag and dispatch-delay options), verifier updates and 3 new scenarios, `Main` demo, `PipelineTest` | A3 | sonnet | Integration of A1 to A3 with complete test code in the plan |
| **B0** | Redis Lua scripts (`upsert`, `claim`, `release`, `ack`, `remove`, `wmSet`, `wmGet`) as resources, plus script-level tests with raw Lettuce | T0 | opus | Atomicity and ordering in Lua; packed-value escaping |
| **B1** | `DynamoCartStateStore`, `DynamoSendLedger`, table and index creation, `RecoveryMetaStore`, contract subclasses | A1 | opus | Conditional expressions and fencing are correctness-critical |
| **B2** | `RedisTimerStore`, `RedisWatermark`, `RedisMeta` over B0 scripts, contract subclasses | A1, B0 | sonnet | Thin adapter over reviewed scripts |
| **B3** | `JsonCodec`, `Topics`, `KafkaIntentPublisher` (lanes), `KafkaOutcomeRecorder`, `KafkaDeadLetterQueue`, `KafkaRecordingSink` (`sink-sends`), `TopicAdmin` | A1 | sonnet | Straightforward client wiring |
| **C0a** | `InfraConfig` (env parsing, validation, config hash), `Health` and `HealthServer` (`/health`, `/ready`, `/metrics`), `Role` interface, `TokenBucket`, `CircuitBreaker` | T0, A1 | sonnet | Small, well-specified units |
| **C0b** | `BatchConsumerLoop`: grouping by key on virtual threads, per-partition commit at lowest held or unfinished offset, pause/seek/resume, commit on revoke, poison routing hook, backoff, shutdown; Testcontainers Kafka tests | C0a | opus | The subtlest concurrency code in the project |
| **C0c** | Multi-stage `Dockerfile`, `docker-compose.yml` (services, replicas, profiles incl. the `loadgen` service, env block, grace periods), `demo.env` | C0a | haiku | Declarative files fully specified by spec §7.2 to §7.3 |
| **C1a** | Detector role (end-offset snapshots, `wmSet`, classification) and scheduler role | A2, B1, B2, B3, C0a, C0b | opus | Watermark snapshot logic is the spec's most delicate mechanism |
| **C1b** | Dispatcher role: two lane consumers, budget and reserve pausing, gate hold with seek, breaker and guardrail pause, retry-loop thread | A3, C1a | opus | Many interacting pause conditions |
| **C1c** | Reconciler role (per-shard parallel, epoch, failover replay from `recovery-meta` run id), replay role, init role, `RoleRegistry`, `Main` role dispatch | A4, C1b, D1 | sonnet | Mostly orchestration of existing pieces |
| **C2** | The 7 infra end-to-end tests with roles as threads, plus the Redis-restart contract test | C1a, C1b, C1c | opus | Real-infra debugging across roles |
| **D0** | Loadgen pure logic: workload scripts, expected sends, outcome precedence accounting, percentiles, markdown report | T0 | sonnet | Pure functions with full test code |
| **D1** | Loadgen role (no CLI args; `RATE`, `DURATION`, `RUN_PREFIX` env): rate-paced producer, `sink-sends` and outcome consumers, lag sampling, per-lane latency from its own script | B3, C0a, D0 | sonnet | Wiring over D0 and B3 |
| **E1** | `DESIGN.md` and `README.md` per spec §9 | C2, D1 | sonnet | Prose accuracy against the built system |
| **E2** | Run `docker compose` demo, drills, and a load test; commit the report | E1 | sonnet | Execution and honest reporting |

### 2.1 Waves

```
W0  T0
W1  A1   B0   D0                                  (3 in parallel)
W2  A2   B1   B2   B3   C0a                       (all need A1; B2 also B0)
W3  A3   C0b  C0c  D1                             (C0b, C0c once C0a is in; D1 once B3, C0a, D0 are in)
W4  A4   C1a                                      (C1a once A2, B1 to B3, C0a, C0b are in)
W5  C1b                                           (after A3 and C1a)
W6  C1c                                           (after A4, C1b, D1: RoleRegistry imports DispatcherRole and LoadgenRole)
W7  C2
W8  E1
W9  E2
```

A task starts as soon as its dependencies are merged; the waves are the earliest schedule, not barriers. The core chain is T0 → A1 → A2 → A3 → A4 → C1c → C2 → E1 → E2; since ruling R1 the runtime chain T0 → A1 → C0a → C0b → C1a → C1b → C1c → C2 → E1 → E2 is one task longer and sets the earliest finish (W9). No two tasks in one wave modify the same file.

### 2.2 Model selection rules

- **opus**: tasks that define semantics others depend on, or whose correctness is concurrency or ordering (A1, A3, B0, B1, C0b, C1a, C1b, C2).
- **sonnet**: well-specified implementation or integration where the thread file carries complete code (T0, A2, A4, B2, B3, C0a, C1c, D0, D1, E1, E2).
- **haiku**: fully declarative files with complete content in the plan (C0c).
- **Reviewers**: sonnet for every task review; opus for reviews of A1, A3, B0, B1, C0b, C1a, C1b; opus for the final whole-branch review.
- **Fix loops**: rounds 1 to 3 resume the implementer; rounds 4 to 5 move one tier up (haiku → sonnet → opus).

## 3. Execution protocol for parallel threads

1. Create an integration branch `infra` from `main` in a worktree (`superpowers:using-git-worktrees`).
2. Each task runs in its own worktree on branch `infra-<task>` created from the current `infra` head once its dependencies are merged there.
3. Tasks in the same wave touch disjoint files by construction (file ownership in each thread file). `build.gradle.kts` is owned by T0 only (it carries the pinned versions and the `-Djdk.tracePinnedThreads=full` flag C1 and C2 rely on). `Main.java`: A1 may edit only its import lines during the temporary `legacy` package move; otherwise A4, then C1c. `docker-compose.yml` is owned by C0c alone (it includes the `loadgen` service D1 runs).
4. After a task's review passes, merge `infra-<task>` into `infra` with `--no-ff`, run `./gradlew test` on `infra` (and `./gradlew integrationTest` once any B or C task is in), then start dependents.
5. A merge conflict or a red build on `infra` stops dependents until resolved by the task's implementer.
6. After E2, the final whole-branch review runs on `infra`, then `infra` merges to `main` via `superpowers:finishing-a-development-branch`.

## 4. Thread files

| File | Tasks |
|---|---|
| `2026-09-26-production-infra/thread-T0-D-E.md` | T0, D0, D1, E1, E2 |
| `2026-09-26-production-infra/thread-A.md` | A1 to A4 |
| `2026-09-26-production-infra/thread-B.md` | B0 to B3 |
| `2026-09-26-production-infra/thread-C0.md` | C0a to C0c |
| `2026-09-26-production-infra/thread-C1.md` | C1a to C1c, C2 |
