# Thread C1: Roles, Main dispatch, and end-to-end tests

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Read the master plan `docs/superpowers/plans/2026-09-26-production-infra.md` (§1 frozen contracts, §3 protocol) and the spec `docs/superpowers/specs/2026-09-25-production-infra-design.md` (§3, §5.4, §6.2, §6.3, §7, §8.3) before starting a task.

**Goal:** Turn the shared core classes and thread B's adapters into the separately runnable roles (`detector`, `scheduler`, `dispatcher`, `reconciler`, `replay`, `init`), dispatch them from `Main --role=<name>` with health, config validation and SIGTERM handling, and prove wiring and failure semantics with the spec §8.3 end-to-end tests on real containers.

**Tasks:**

| Task | Scope | Depends on | Model |
|---|---|---|---|
| **C1a** | `RoleContext` (clients, startup check, loop runner), `Failures`, watermark snapshot logic and detector hooks, `DetectorRole`, `SchedulerRole`; shared integration-test helpers | A2, B1, B2, B3, C0a, C0b | opus |
| **C1b** | `DispatcherRole`: two lane consumers, pause conditions, breaker-wrapped sink, retry/control loop, `/ready` state | A3, C1a | opus |
| **C1c** | `ReconcilerRole` (sweep + failover replay over B2's `RedisMeta`), `ReplayRole`, `InitRole`, `RoleRegistry`, `Main` role dispatch and `healthcheck` | A4, C1b, D1 | sonnet |
| **C2** | Pinning guard, the 7 end-to-end tests of spec §8.3, the Redis-restart contract test | C1a, C1b, C1c | opus |

C1b and C1c both need C1a merged (they use `RoleContext` and the integration-test helpers). C1c also needs C1b (its `RoleRegistry` imports `DispatcherRole`, controller ruling R1) and D1 (it imports `LoadgenRole`), so C1c runs after C1b.

**File ownership** (no other task edits these files):

- **C1a** creates `src/main/java/com/quince/cartrecovery/app/{RoleContext,Failures,WatermarkSnapshots,DetectorWatermarkHooks,DetectorRole,SchedulerRole}.java`, unit tests `src/test/java/com/quince/cartrecovery/app/{WatermarkSnapshotsTest,DetectorWatermarkHooksTest,FailuresTest,RoleContextTest}.java`, and integration-test helpers and tests `src/integrationTest/java/com/quince/cartrecovery/app/{RoleInfra,RoleThread,TopicTail,DetectorRoleIT,SchedulerRoleIT}.java`.
- **C1b** creates only `app/DispatcherRole.java`, `src/test/.../app/DispatcherRoleTest.java`, `src/integrationTest/.../app/DispatcherRoleIT.java`.
- **C1c** creates `app/{ReconcilerRole,ReplayRole,InitRole,RoleRegistry}.java`, tests `src/test/.../MainTest.java`, `src/integrationTest/.../app/{InitRoleIT,ReconcilerRoleIT,ReplayRoleIT}.java`, and **owns `Main.java`** (after A4) for role dispatch. C1a and C1b add role classes only; they never touch `Main.java` or `RoleRegistry`. Redis `run_id`, role and `epoch` helpers come from B2's `RedisMeta` (controller ruling R6); this thread has no Redis identity class of its own.
- **C2** creates `src/integrationTest/java/com/quince/cartrecovery/e2e/{PinningGuard,PinningGuardSelfIT,EndToEndIT,RedisRestartIT}.java` and `src/integrationTest/resources/{junit-platform.properties,META-INF/services/org.junit.jupiter.api.extension.Extension}`.

**Binding note:** every task codes only against master §1 (model, ports, core APIs, in-memory adapters, the §1.5 runtime members, and the §1.6 infra adapter APIs from thread B, controller rulings R6 and R7). A task that finds a §1 member unusable stops and reports `BLOCKED`; it never changes the contract.

## Global notes for this thread

- Commands always run with `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current`. Unit tests: `./gradlew test`; container tests: `./gradlew integrationTest` (Docker required; classes are annotated `@Testcontainers(disabledWithoutDocker = true)`).
- Roles log with `System.out`/`System.err`; no logging framework.
- A role's `run` returns when its thread is interrupted (master §1.5, controller ruling R8: `Main`'s shutdown hook interrupts the role thread and waits up to 30 s; `RoleThread.close()` in tests does the same). Loop threads stop through a flag and `BatchConsumerLoop.close()`, never through interrupts, so the Kafka consumer is never interrupted.
- Consumer group ids: `detector` (C1a), `dispatcher-fast` and `dispatcher-slow` (C1b), `replay` (C1c). D1's lag sampler reads the first three.
- Container images (master Global Constraints): `apache/kafka:4.3.1`, `redis:7.4-alpine`, `amazon/dynamodb-local:3.3.1`.
- Metric names (controller ruling R12). Core (thread A): `events.handled`, `events.ignored`, `carts.abandoned`, `timers.held`, `timers.conflict`, `timers.stale`, `timers.wrong_status`, `timers.poison`, `reminders.published`, `dispatch.sent`, `dispatch.cancelled`, `dispatch.duplicate`, `dispatch.retry`, `dispatch.dead_lettered`, `dispatch.skipped_late`, `dispatch.skipped_late_precheck`, `dispatch.held`, `dispatch.no_token`, `dispatch.retry_held`, `dispatch.retry_no_token`, `dispatch.lease_expiring`, `dispatch.lease_lost`, `dispatch.replayed`, `dispatch.replay_poison_skipped`, `reconcile.timers_rebuilt`, `reconcile.sequences_ended`. Role-level (this thread, not core): `dispatch.breaker_open` (C1b), the `watermark.lag_ms.p<n>` ready keys and `watermark.*` failure counters (C1a), `timers.poison` for deterministic store errors (C1a, same name as core's out-of-range offset case), `scheduler.*`, `dispatch.retry_error`, `dispatcher.meta_read_failed`, `reconciler.*`, `replay.undecodable`. `consumer.*` metrics come from C0b.
- The system clock port is the lambda `Clock clock = Instant::now;` (no extra class).
- Arm salt `"cart-recovery-v1"` everywhere (same as `Pipeline.withDefaults` and loadgen).
- Demo-scale timings in container tests (set by `RoleInfra.config`): window 2 s, offsets 3 s / 6 s / 9 s, lateness bounds 20 s each, `CLOCK_SKEW` 0.5 s, `LEASE` 3 s, `GATEWAY_TIMEOUT` 1 s, `RETRY_BASE` 0.2 s, `RETRY_POLL` 0.2 s, holdout 0, 8 shards, 8 partitions.

## Review Focus (this thread)

1. A detector whose snapshot call fails or whose partition is behind must write nothing for that partition, while an idle partition must read as current within one iteration. Pinned in C1a `DetectorWatermarkHooksTest`.
2. Redis time must be read before the end offsets in a snapshot (the reverse order breaks the safety argument of spec §5.4). Pinned in C1a `readsRedisTimeBeforeEndOffsets`.
3. A malformed environment value exits 2 with one stderr line naming the variable. Pinned in C1c `MainTest.badConfigExits2WithOneLineNamingTheVariable`.
4. A second Redis identity change during a failover replay must restart the replay from the earliest change, which survives a reconciler crash because it lives in `recovery-meta`. Implemented in C1c `ReconcilerRole.Cycle.next` (`markRedisChange` keeps the earliest); exercised end to end in C2 `RedisRestartIT`.
5. A dispatcher paused by the guardrail switch must not send and must resume by itself. Pinned in C1b `DispatcherRoleIT.guardrailPauseStopsAndResumesSending`.

---

### Task C1a: Role support, detector role, scheduler role

**Model:** opus (master §2: watermark snapshot logic is the spec's most delicate mechanism).

**Files:**
- Create: `src/main/java/com/quince/cartrecovery/app/WatermarkSnapshots.java`
- Create: `src/main/java/com/quince/cartrecovery/app/DetectorWatermarkHooks.java`
- Create: `src/main/java/com/quince/cartrecovery/app/Failures.java`
- Create: `src/main/java/com/quince/cartrecovery/app/RoleContext.java`
- Create: `src/main/java/com/quince/cartrecovery/app/DetectorRole.java`
- Create: `src/main/java/com/quince/cartrecovery/app/SchedulerRole.java`
- Test: `src/test/java/com/quince/cartrecovery/app/WatermarkSnapshotsTest.java`
- Test: `src/test/java/com/quince/cartrecovery/app/DetectorWatermarkHooksTest.java`
- Test: `src/test/java/com/quince/cartrecovery/app/FailuresTest.java`
- Test: `src/test/java/com/quince/cartrecovery/app/RoleContextTest.java`
- Create (integration helpers): `src/integrationTest/java/com/quince/cartrecovery/app/RoleInfra.java`, `RoleThread.java`, `TopicTail.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/app/DetectorRoleIT.java`, `SchedulerRoleIT.java`

**Interfaces:**
- Consumes (master §1): `AbandonmentDetector(RecoveryConfig, CartStateStore, TimerStore, ArmAssigner, Metrics).handle(CartEvent, int)`; `ReminderScheduler(RecoveryConfig, DispatchConfig, CartStateStore, TimerStore, Watermark, IntentPublisher, OutcomeRecorder, Metrics).onTimer(Timer) → TimerDecision`; `TimerStore.claimDue/ack/release/existing/remove`; `Watermark.publish(int, long, Instant)/current(int)/now()`; `TimerDecision.Ack/Release(Duration)`; `HashArmAssigner(String, int)`; `Shards.of(String, int)`; `InfraConfig` accessors; `Health.beat/setReady`; `Role`; `BatchConsumerLoop` (`Settings`, `Handler`, `Hooks`, `Verdict`, constructor, `run`, `close`); `PoisonException(String, Throwable)`. Thread B adapters exactly as master §1.6: `DynamoTables` (`client`, `createAll`, `CARTS`, `SEND_LEDGER`), `DynamoCartStateStore(DynamoDbClient, String table, RecoveryConfig, int shards)`, `RecoveryMetaStore` (`init`, `read`), `RedisTimerStore(StatefulRedisConnection, int shards, Duration lease)`, `RedisWatermark(StatefulRedisConnection, int partitions)`, `RedisMeta.writeEpoch()`, `KafkaIntentPublisher(Producer, int fastOffsets)`, `KafkaOutcomeRecorder(Producer)`, `TopicAdmin(Admin)` (`createAll`, `partitionCounts`), `Topics` constants, `JsonCodec.encode/decodeCartEvent/decodeIntent/decodeOutcome`. T0's `com.quince.cartrecovery.Await.until(BooleanSupplier, Duration)`.
- Produces (used by C1b, C1c, C2):
  - `public final class RoleContext implements AutoCloseable` with `public RoleContext(InfraConfig)`, `InfraConfig config()`, `DynamoDbClient dynamo()`, `StatefulRedisConnection<String,String> redis()`, `Producer<String,byte[]> producer()`, `Admin admin()`, `Map<String,Object> consumerProps(String groupId)`, `void verifyStartup()` (throws `IllegalStateException`), `static Optional<String> startupProblem(int shards, int partitions, Integer metaShards, Integer metaPartitions, Map<String,Integer> topicPartitions)`, `static void runLoops(Runnable stop, List<Runnable> loops)`, `static boolean sleep(Duration)`, `static final String ARM_SALT`.
  - `public final class Failures { static boolean isDeterministic(Throwable) }`.
  - `public final class DetectorRole implements Role` (name `"detector"`), `public final class SchedulerRole implements Role` (name `"scheduler"`), both with public no-arg constructors.
  - Integration helpers (public): `RoleInfra.start()`, `RoleInfra.config(Map<String,String> overrides) → InfraConfig`, `RoleInfra.ctx() → RoleContext`, `RoleInfra.bootstrap() → String`, `RoleInfra.prefix(String) → String`, `RoleInfra.produce(CartEvent) → RecordMetadata`, `RoleInfra.send(String topic, String key, byte[] value) → RecordMetadata`; `RoleThread(Role, InfraConfig)` with `health()`, `metrics()`, `failure()`, `boolean join(Duration)`, `close()`; `TopicTail(String bootstrap, String topic)` with `List<ConsumerRecord<String,byte[]>> records(String keyPrefix)`, `close()`.
  - Metric names: `watermark.snapshot_failed`, `watermark.committed_lookup_failed`, `watermark.publish_failed`, `scheduler.claim_failed`, `scheduler.timer_failed`, `timers.poison`. Ready keys: `watermark.lag_ms.p<n>` (role-level watermark lag, controller ruling R12).

- [ ] **Step 1: Write the failing snapshot-selection test**

`src/test/java/com/quince/cartrecovery/app/WatermarkSnapshotsTest.java`:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class WatermarkSnapshotsTest {
    static final TopicPartition P0 = new TopicPartition("cart-events", 0);
    static final TopicPartition P1 = new TopicPartition("cart-events", 1);
    static final Instant T1 = Instant.parse("2026-01-01T00:00:01Z");
    static final Instant T2 = Instant.parse("2026-01-01T00:00:02Z");
    static final Instant T3 = Instant.parse("2026-01-01T00:00:03Z");

    @Test
    void noSnapshotSatisfiesNothing() {
        assertEquals(Optional.empty(), new WatermarkSnapshots(4).satisfied(P0, 100));
    }

    @Test
    void committedBelowEndIsNotSatisfied() {
        WatermarkSnapshots s = new WatermarkSnapshots(4);
        s.add(T1, Map.of(P0, 10L));
        assertEquals(Optional.empty(), s.satisfied(P0, 9));
    }

    @Test
    void committedAtEndIsSatisfied() {
        WatermarkSnapshots s = new WatermarkSnapshots(4);
        s.add(T1, Map.of(P0, 10L));
        assertEquals(Optional.of(T1), s.satisfied(P0, 10));
    }

    @Test
    void emptyPartitionIsSatisfiedAtOnce() {
        WatermarkSnapshots s = new WatermarkSnapshots(4);
        s.add(T1, Map.of(P0, 0L));
        assertEquals(Optional.of(T1), s.satisfied(P0, 0));
    }

    @Test
    void newestSatisfiedSnapshotWins() {
        WatermarkSnapshots s = new WatermarkSnapshots(4);
        s.add(T1, Map.of(P0, 5L));
        s.add(T2, Map.of(P0, 10L));
        s.add(T3, Map.of(P0, 15L));
        assertEquals(Optional.of(T2), s.satisfied(P0, 12));
        assertEquals(Optional.of(T3), s.satisfied(P0, 15));
        assertEquals(Optional.empty(), s.satisfied(P0, 4));
    }

    @Test
    void snapshotWithoutThePartitionIsIgnored() {
        WatermarkSnapshots s = new WatermarkSnapshots(4);
        s.add(T1, Map.of(P0, 0L));
        assertEquals(Optional.empty(), s.satisfied(P1, 100));
    }

    @Test
    void keepsOnlyTheLatestSnapshots() {
        WatermarkSnapshots s = new WatermarkSnapshots(2);
        s.add(T1, Map.of(P0, 5L));
        s.add(T2, Map.of(P0, 10L));
        s.add(T3, Map.of(P0, 15L));
        assertEquals(Optional.empty(), s.satisfied(P0, 7));
        assertEquals(Optional.of(T2), s.satisfied(P0, 10));
    }

    @Test
    void newestIsTheLastAdded() {
        WatermarkSnapshots s = new WatermarkSnapshots(4);
        assertEquals(Optional.empty(), s.newest());
        s.add(T1, Map.of(P0, 5L));
        s.add(T2, Map.of(P0, 10L));
        assertEquals(Optional.of(T2), s.newest());
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.WatermarkSnapshotsTest' --console=plain`
Expected: FAIL, `compileTestJava` reports `cannot find symbol ... class WatermarkSnapshots`.

- [ ] **Step 3: Implement `WatermarkSnapshots`**

`src/main/java/com/quince/cartrecovery/app/WatermarkSnapshots.java`:

```java
package com.quince.cartrecovery.app;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;

/**
 * The detector's latest end-offset snapshots (spec §5.4). A snapshot (T, E) says every record appended
 * to partition p before Redis time T lies below offset E[p]; once the committed position reaches E[p],
 * the watermark for p may be set to T. Used by the poll thread only.
 */
final class WatermarkSnapshots {
    private record Snapshot(Instant time, Map<TopicPartition, Long> ends) {}

    private final int keep;
    private final Deque<Snapshot> newestFirst = new ArrayDeque<>();

    WatermarkSnapshots(int keep) {
        if (keep < 1) throw new IllegalArgumentException("keep must be >= 1");
        this.keep = keep;
    }

    void add(Instant time, Map<TopicPartition, Long> ends) {
        newestFirst.addFirst(new Snapshot(time, Map.copyOf(ends)));
        while (newestFirst.size() > keep) newestFirst.removeLast();
    }

    /** T of the newest snapshot whose end offset for p is at or below the committed position; empty if none. */
    Optional<Instant> satisfied(TopicPartition p, long committed) {
        for (Snapshot s : newestFirst) {
            Long end = s.ends().get(p);
            if (end != null && committed >= end) return Optional.of(s.time());
        }
        return Optional.empty();
    }

    Optional<Instant> newest() {
        Snapshot s = newestFirst.peekFirst();
        return s == null ? Optional.empty() : Optional.of(s.time());
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.WatermarkSnapshotsTest' --console=plain`
Expected: PASS, 8 tests, `BUILD SUCCESSFUL`.

- [ ] **Step 5: Write the failing detector-hooks test**

`src/test/java/com/quince/cartrecovery/app/DetectorWatermarkHooksTest.java` (the consumer is a `java.lang.reflect.Proxy` answering only `assignment`, `committed`, `endOffsets`, so the test is independent of the `MockConsumer` API of the pinned Kafka version):

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.ports.Watermark;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

class DetectorWatermarkHooksTest {
    static final TopicPartition P0 = new TopicPartition("cart-events", 0);
    static final TopicPartition P1 = new TopicPartition("cart-events", 1);
    static final Instant T1 = Instant.parse("2026-01-01T00:00:01Z");
    static final Instant T2 = Instant.parse("2026-01-01T00:00:02Z");

    final List<String> calls = new ArrayList<>();
    final RecordingWatermark watermark = new RecordingWatermark();
    final FakeConsumer broker = new FakeConsumer();
    final Metrics metrics = new Metrics();
    final long[] nanos = {0};
    final DetectorWatermarkHooks hooks = new DetectorWatermarkHooks(watermark, new Health(), metrics, () -> nanos[0]);

    @Test
    void idlePartitionPublishesAtOnceEvenWithoutANewCommit() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 5L);
        broker.committed.put(P0, 5L);
        hooks.beforePoll(broker.proxy());
        hooks.afterCommit(broker.proxy(), Map.of(), 3);   // an empty poll or a backoff iteration commits nothing
        assertEquals(List.of("0|3|" + T1), watermark.published);
    }

    @Test
    void behindPartitionWritesNothingUntilCommittedReachesTheSnapshot() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 10L);
        broker.committed.put(P0, 5L);
        hooks.beforePoll(broker.proxy());
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals(List.of(), watermark.published);
        hooks.afterCommit(broker.proxy(), Map.of(P0, 10L), 1);
        assertEquals(List.of("0|1|" + T1), watermark.published);
    }

    @Test
    void failedSnapshotTakesNothing() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 0L);
        broker.endOffsetsFailure = new TimeoutException("broker unreachable");
        hooks.beforePoll(broker.proxy());
        hooks.afterCommit(broker.proxy(), Map.of(P0, 0L), 1);
        assertEquals(List.of(), watermark.published);
        assertEquals(1, metrics.get("watermark.snapshot_failed"));
    }

    @Test
    void newestSatisfiedSnapshotWins() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 0L);
        broker.ends.put(P0, 5L);
        hooks.beforePoll(broker.proxy());                  // (T1, 5)
        nanos[0] += Duration.ofMillis(300).toNanos();
        watermark.now = T2;
        broker.ends.put(P0, 10L);
        hooks.beforePoll(broker.proxy());                  // (T2, 10)
        hooks.afterCommit(broker.proxy(), Map.of(P0, 7L), 1);
        hooks.afterCommit(broker.proxy(), Map.of(P0, 10L), 1);
        assertEquals(List.of("0|1|" + T1, "0|1|" + T2), watermark.published);
    }

    @Test
    void snapshotsAtMostEvery250ms() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 0L);
        broker.ends.put(P0, 0L);
        hooks.beforePoll(broker.proxy());
        nanos[0] += Duration.ofMillis(100).toNanos();
        hooks.beforePoll(broker.proxy());
        assertEquals(1, broker.endOffsetsCalls);
        nanos[0] += Duration.ofMillis(150).toNanos();
        hooks.beforePoll(broker.proxy());
        assertEquals(2, broker.endOffsetsCalls);
    }

    @Test
    void readsRedisTimeBeforeEndOffsets() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 0L);
        broker.ends.put(P0, 0L);
        hooks.beforePoll(broker.proxy());
        assertEquals(List.of("committed", "now", "endOffsets"), calls);
    }

    @Test
    void revokedPartitionIsNotPublished() {
        broker.assignment = Set.of(P0, P1);
        broker.ends.putAll(Map.of(P0, 0L, P1, 0L));
        broker.committed.putAll(Map.of(P0, 0L, P1, 0L));
        hooks.beforePoll(broker.proxy());
        broker.assignment = Set.of(P1);
        hooks.afterCommit(broker.proxy(), Map.of(P0, 0L), 2);
        assertEquals(List.of("1|2|" + T1), watermark.published);
    }

    final class RecordingWatermark implements Watermark {
        final List<String> published = new ArrayList<>();
        Instant now = T1;

        @Override public void publish(int partition, long generation, Instant eventTime) {
            published.add(partition + "|" + generation + "|" + eventTime);
        }
        @Override public Instant current(int srcPartition) { return Instant.EPOCH; }
        @Override public Instant now() { calls.add("now"); return now; }
    }

    final class FakeConsumer {
        Set<TopicPartition> assignment = Set.of();
        final Map<TopicPartition, Long> ends = new HashMap<>();
        final Map<TopicPartition, Long> committed = new HashMap<>();
        RuntimeException endOffsetsFailure;
        int endOffsetsCalls;

        @SuppressWarnings("unchecked")
        Consumer<String, byte[]> proxy() {
            return (Consumer<String, byte[]>) Proxy.newProxyInstance(Consumer.class.getClassLoader(),
                new Class<?>[] {Consumer.class}, (self, method, args) -> switch (method.getName()) {
                    case "assignment" -> Set.copyOf(assignment);
                    case "endOffsets" -> {
                        calls.add("endOffsets");
                        endOffsetsCalls++;
                        if (endOffsetsFailure != null) throw endOffsetsFailure;
                        yield Map.copyOf(ends);
                    }
                    case "committed" -> {
                        calls.add("committed");
                        Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
                        committed.forEach((tp, offset) -> out.put(tp, new OffsetAndMetadata(offset)));
                        yield out;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        }
    }
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.DetectorWatermarkHooksTest' --console=plain`
Expected: FAIL, `cannot find symbol ... class DetectorWatermarkHooks`.

- [ ] **Step 7: Implement `DetectorWatermarkHooks`**

`src/main/java/com/quince/cartrecovery/app/DetectorWatermarkHooks.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Spec §5.4 watermark writes. beforePoll: at most every 250 ms, read Redis TIME as T and then the end
 * offsets E of the assigned partitions (a failed call takes no snapshot). afterCommit, every iteration
 * including empty polls and backoff: for each assigned partition publish T of the newest snapshot whose
 * E[p] is at or below the committed position; write nothing when none is satisfied. Poll thread only.
 */
final class DetectorWatermarkHooks implements BatchConsumerLoop.Hooks<byte[]> {
    static final Duration SNAPSHOT_INTERVAL = Duration.ofMillis(250);
    static final Duration BROKER_TIMEOUT = Duration.ofSeconds(1);
    static final int KEEP = 8;

    private final Watermark watermark;
    private final Health health;
    private final Metrics metrics;
    private final LongSupplier nanoTime;
    private final WatermarkSnapshots snapshots = new WatermarkSnapshots(KEEP);
    private final Map<TopicPartition, Long> committed = new HashMap<>();
    private boolean attempted;
    private long lastAttemptNanos;

    DetectorWatermarkHooks(Watermark watermark, Health health, Metrics metrics, LongSupplier nanoTime) {
        this.watermark = watermark;
        this.health = health;
        this.metrics = metrics;
        this.nanoTime = nanoTime;
    }

    @Override
    public void beforePoll(Consumer<String, byte[]> consumer) {
        Set<TopicPartition> assigned = consumer.assignment();
        committed.keySet().retainAll(assigned);
        seedCommitted(consumer, assigned);
        long now = nanoTime.getAsLong();
        if (assigned.isEmpty() || (attempted && now - lastAttemptNanos < SNAPSHOT_INTERVAL.toNanos())) return;
        attempted = true;
        lastAttemptNanos = now;
        try {
            Instant t = watermark.now();   // T before E: every record appended before T lies below E
            snapshots.add(t, consumer.endOffsets(assigned, BROKER_TIMEOUT));
        } catch (RuntimeException e) {
            metrics.increment("watermark.snapshot_failed");   // no snapshot; older ones stay valid
        }
    }

    @Override
    public void afterCommit(Consumer<String, byte[]> consumer, Map<TopicPartition, Long> committedNow, int generation) {
        Set<TopicPartition> assigned = consumer.assignment();
        committedNow.forEach((p, offset) -> {
            if (assigned.contains(p)) committed.merge(p, offset, Math::max);
        });
        for (TopicPartition p : assigned) {
            Long position = committed.get(p);
            if (position == null) continue;
            Optional<Instant> t = snapshots.satisfied(p, position);
            if (t.isEmpty()) continue;   // nothing satisfied: write nothing, the entry goes stale after 5 s
            try {
                watermark.publish(p.partition(), generation, t.get());
                long lagMs = Duration.between(t.get(), snapshots.newest().orElse(t.get())).toMillis();
                health.setReady("watermark.lag_ms.p" + p.partition(), Long.toString(lagMs));
            } catch (RuntimeException e) {
                metrics.increment("watermark.publish_failed");
            }
        }
    }

    /** Idle partitions may never be committed by this member; their broker-committed offset is still processed work. */
    private void seedCommitted(Consumer<String, byte[]> consumer, Set<TopicPartition> assigned) {
        Set<TopicPartition> missing = new HashSet<>(assigned);
        missing.removeAll(committed.keySet());
        if (missing.isEmpty()) return;
        try {
            Map<TopicPartition, OffsetAndMetadata> found = consumer.committed(missing, BROKER_TIMEOUT);
            for (TopicPartition p : missing) {
                OffsetAndMetadata o = found.get(p);
                committed.put(p, o == null ? 0L : o.offset());
            }
        } catch (RuntimeException e) {
            metrics.increment("watermark.committed_lookup_failed");
        }
    }
}
```

- [ ] **Step 8: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.DetectorWatermarkHooksTest' --console=plain`
Expected: PASS, 7 tests.

- [ ] **Step 9: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/app/WatermarkSnapshots.java src/main/java/com/quince/cartrecovery/app/DetectorWatermarkHooks.java src/test/java/com/quince/cartrecovery/app/WatermarkSnapshotsTest.java src/test/java/com/quince/cartrecovery/app/DetectorWatermarkHooksTest.java
git commit -m "Add detector end-offset watermark snapshots and hooks

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 10: Write failing tests for failure classification and the startup check**

`src/test/java/com/quince/cartrecovery/app/FailuresTest.java`:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

class FailuresTest {
    static DynamoDbException aws(int status, String code) {
        return (DynamoDbException) DynamoDbException.builder()
            .statusCode(status)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build())
            .build();
    }

    @Test void validationErrorIsDeterministic() { assertTrue(Failures.isDeterministic(aws(400, "ValidationException"))); }
    @Test void throttlingIsTransient() { assertFalse(Failures.isDeterministic(aws(400, "ProvisionedThroughputExceededException"))); }
    @Test void serverErrorIsTransient() { assertFalse(Failures.isDeterministic(aws(500, "InternalServerError"))); }
    @Test void wrappedValidationErrorIsDeterministic() {
        assertTrue(Failures.isDeterministic(new CompletionException(aws(400, "ValidationException"))));
    }
    @Test void ioErrorIsTransient() { assertFalse(Failures.isDeterministic(new UncheckedIOException(new IOException("reset")))); }
}
```

`src/test/java/com/quince/cartrecovery/app/RoleContextTest.java`:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.infra.kafka.Topics;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RoleContextTest {
    static Map<String, Integer> topics(int partitions) {
        Map<String, Integer> m = new HashMap<>();
        for (String t : Topics.ALL) m.put(t, partitions);
        return m;
    }

    @Test void matchingSetupHasNoProblem() {
        assertEquals(Optional.empty(), RoleContext.startupProblem(8, 8, 8, 8, topics(8)));
    }
    @Test void missingMetaAsksForInit() {
        assertTrue(RoleContext.startupProblem(8, 8, null, null, topics(8)).orElseThrow().contains("--role=init"));
    }
    @Test void shardMismatchRefuses() {
        assertTrue(RoleContext.startupProblem(4, 8, 8, 8, topics(8)).orElseThrow().contains("SHARDS=4"));
    }
    @Test void partitionMismatchRefuses() {
        assertTrue(RoleContext.startupProblem(8, 4, 8, 8, topics(8)).orElseThrow().contains("PARTITIONS=4"));
    }
    @Test void topicPartitionMismatchRefuses() {
        Map<String, Integer> t = topics(8);
        t.put(Topics.INTENTS_SLOW, 4);
        assertTrue(RoleContext.startupProblem(8, 8, 8, 8, t).orElseThrow().contains(Topics.INTENTS_SLOW));
    }
    @Test void missingTopicRefuses() {
        Map<String, Integer> t = topics(8);
        t.remove(Topics.CART_EVENTS);
        assertTrue(RoleContext.startupProblem(8, 8, 8, 8, t).orElseThrow().contains("missing"));
    }
}
```

- [ ] **Step 11: Run them to verify they fail**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.FailuresTest' --tests 'com.quince.cartrecovery.app.RoleContextTest' --console=plain`
Expected: FAIL, `cannot find symbol ... class Failures` and `... class RoleContext`.

- [ ] **Step 12: Implement `Failures` and `RoleContext`**

`src/main/java/com/quince/cartrecovery/app/Failures.java`:

```java
package com.quince.cartrecovery.app;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

/** Spec §6.3 failure classification. */
public final class Failures {
    private Failures() {}

    /** True for errors that fail the same way on every retry: AWS 400s other than throttling and clock skew. */
    public static boolean isDeterministic(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof AwsServiceException e) {
                return e.statusCode() == 400 && !e.isThrottlingException() && !e.isClockSkewException();
            }
        }
        return false;
    }
}
```

`src/main/java/com/quince/cartrecovery/app/RoleContext.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.TopicAdmin;
import com.quince.cartrecovery.infra.kafka.Topics;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** The clients one role process shares, the startup check every role runs, and the loop runner. */
public final class RoleContext implements AutoCloseable {
    public static final String ARM_SALT = "cart-recovery-v1";

    private final InfraConfig config;
    private final DynamoDbClient dynamo;
    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> redis;
    private final KafkaProducer<String, byte[]> producer;
    private final Admin admin;

    public RoleContext(InfraConfig config) {
        this.config = config;
        this.dynamo = DynamoTables.client(config.dynamoEndpoint(), config.maxInFlight());   // spec §6.3 client sizing
        this.redisClient = RedisClient.create(config.redisUrl());
        this.redis = redisClient.connect();
        this.producer = new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap(),
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
            ProducerConfig.LINGER_MS_CONFIG, 5,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
        this.admin = Admin.create(Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap()));
    }

    public InfraConfig config() { return config; }
    public DynamoDbClient dynamo() { return dynamo; }
    public StatefulRedisConnection<String, String> redis() { return redis; }
    public Producer<String, byte[]> producer() { return producer; }
    public Admin admin() { return admin; }

    /** Classic group protocol (watermark fencing relies on classic generations), manual commits. */
    public Map<String, Object> consumerProps(String groupId) {
        return Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap(),
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    }

    /**
     * Refuses to start when S or P differ from recovery-meta or any topic's partition count differs from P
     * (spec §7.1), then loads producer metadata for every topic on this platform thread so virtual threads
     * never wait for metadata inside the producer's monitor.
     */
    public void verifyStartup() {
        RecoveryMetaStore.Meta meta;
        try {
            meta = new RecoveryMetaStore(dynamo).read();
        } catch (IllegalStateException missing) {   // init has not run
            meta = null;
        }
        Map<String, Integer> counts = new TopicAdmin(admin).partitionCounts();
        startupProblem(config.shards(), config.partitions(),
                meta == null ? null : meta.shards(), meta == null ? null : meta.partitions(), counts)
            .ifPresent(problem -> { throw new IllegalStateException(problem); });
        for (String topic : Topics.ALL) producer.partitionsFor(topic);
    }

    static Optional<String> startupProblem(int shards, int partitions, Integer metaShards, Integer metaPartitions,
                                           Map<String, Integer> topicPartitions) {
        if (metaShards == null || metaPartitions == null) return Optional.of("recovery-meta missing: run --role=init first");
        if (metaShards != shards) return Optional.of("SHARDS=" + shards + " differs from recovery-meta shards=" + metaShards);
        if (metaPartitions != partitions)
            return Optional.of("PARTITIONS=" + partitions + " differs from recovery-meta partitions=" + metaPartitions);
        for (String topic : Topics.ALL) {
            Integer n = topicPartitions.get(topic);
            if (n == null) return Optional.of("topic " + topic + " missing: run --role=init first");
            if (n != partitions) return Optional.of("topic " + topic + " has " + n + " partitions, PARTITIONS=" + partitions);
        }
        return Optional.empty();
    }

    /**
     * Runs each loop on its own platform thread. Returns after stopping all loops when the calling thread is
     * interrupted (SIGTERM); throws if a loop ends on its own, so the process exits and is restarted.
     */
    public static void runLoops(Runnable stop, List<Runnable> loops) {
        CountDownLatch anyEnded = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < loops.size(); i++) {
            Runnable loop = loops.get(i);
            threads.add(Thread.ofPlatform().name("loop-" + i).start(() -> {
                try {
                    loop.run();
                } catch (Throwable e) {
                    System.err.println(Thread.currentThread().getName() + " failed: " + e);
                } finally {
                    anyEnded.countDown();
                }
            }));
        }
        boolean interrupted = false;
        try {
            anyEnded.await();
        } catch (InterruptedException e) {
            interrupted = true;
        }
        stop.run();
        for (Thread t : threads) {
            try {
                t.join(Duration.ofSeconds(35));
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
            return;
        }
        throw new IllegalStateException("a role loop ended unexpectedly");
    }

    /** Sleeps; false if interrupted (the interrupt flag is restored). */
    public static boolean sleep(Duration d) {
        try {
            Thread.sleep(d);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(5));
        admin.close(Duration.ofSeconds(5));
        redis.close();
        redisClient.shutdown();
        dynamo.close();
    }
}
```

- [ ] **Step 13: Run them to verify they pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.FailuresTest' --tests 'com.quince.cartrecovery.app.RoleContextTest' --console=plain`
Expected: PASS, 11 tests.

- [ ] **Step 14: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/app/Failures.java src/main/java/com/quince/cartrecovery/app/RoleContext.java src/test/java/com/quince/cartrecovery/app/FailuresTest.java src/test/java/com/quince/cartrecovery/app/RoleContextTest.java
git commit -m "Add role context, startup check, and failure classification

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 15: Add the shared integration-test helpers**

These are test infrastructure, not behaviour; they are exercised by the role ITs below.

`src/integrationTest/java/com/quince/cartrecovery/app/RoleInfra.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.TopicAdmin;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.infra.redis.RedisMeta;
import com.quince.cartrecovery.model.CartEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/** Containers shared by every role and end-to-end test in the JVM, started once, plus demo-scale config. */
public final class RoleInfra {
    public static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"))
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
    public static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
        .withCommand("redis-server", "--appendonly", "yes")
        .withExposedPorts(6379);
    public static final GenericContainer<?> DYNAMO = new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local:3.3.1"))
        .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
        .withExposedPorts(8000);

    private static RoleContext ctx;

    private RoleInfra() {}

    /** Starts the containers and creates topics, tables, the meta item and the epoch, once per JVM. */
    public static synchronized void start() {
        if (ctx != null) return;
        Startables.deepStart(KAFKA, REDIS, DYNAMO).join();
        ctx = new RoleContext(config(Map.of()));
        new TopicAdmin(ctx.admin()).createAll(8, 1, 1);
        DynamoTables.createAll(ctx.dynamo());   // carts, send-ledger, recovery-meta
        new RecoveryMetaStore(ctx.dynamo()).init(8, 8);
        new RedisMeta(ctx.redis()).writeEpoch();
    }

    public static RoleContext ctx() { return ctx; }

    public static String bootstrap() { return KAFKA.getBootstrapServers(); }

    /** Demo-scale config against the shared containers; overrides replace single variables. */
    public static InfraConfig config(Map<String, String> overrides) {
        Map<String, String> env = new HashMap<>();
        env.put("KAFKA_BOOTSTRAP", KAFKA.getBootstrapServers());
        env.put("REDIS_URL", "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        env.put("DYNAMO_ENDPOINT", "http://" + DYNAMO.getHost() + ":" + DYNAMO.getMappedPort(8000));
        env.put("SHARDS", "8");
        env.put("PARTITIONS", "8");
        env.put("REPLICATION_FACTOR", "1");
        env.put("MIN_INSYNC_REPLICAS", "1");
        env.put("WINDOW", "PT2S");
        env.put("OFFSETS", "PT3S,PT6S,PT9S");
        env.put("LATENESS_BOUNDS", "PT20S,PT20S,PT20S");
        env.put("FREQUENCY_CAP", "3");
        env.put("FREQUENCY_WINDOW", "P7D");
        env.put("HOLDOUT_PERCENT", "0");
        env.put("MAX_SEND_ATTEMPTS", "5");
        env.put("RETRY_BASE", "PT0.2S");
        env.put("FAST_OFFSETS", "2");
        env.put("LEASE", "PT3S");
        env.put("GATEWAY_TIMEOUT", "PT1S");
        env.put("CLOCK_SKEW", "PT0.5S");
        env.put("MAX_SEND_RATE", "500");
        env.put("FAST_RESERVE", "0.3");
        env.put("SEND_FAILURE_RATE", "0");
        env.put("RECONCILE_INTERVAL", "PT10M");
        env.put("RETRY_POLL", "PT0.2S");
        env.put("MAX_IN_FLIGHT", "64");
        env.put("HEALTH_PORT", "18080");
        env.putAll(overrides);
        return InfraConfig.fromEnv(env);
    }

    /** A unique cart-id prefix per test, so shared topics and tables can be filtered. */
    public static String prefix(String test) {
        return test + "-" + UUID.randomUUID().toString().substring(0, 8) + "-";
    }

    public static RecordMetadata produce(CartEvent event) {
        return send(Topics.CART_EVENTS, event.cartId(), JsonCodec.encode(event));
    }

    public static RecordMetadata send(String topic, String key, byte[] value) {
        try {
            return ctx.producer().send(new ProducerRecord<>(topic, key, value)).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
```

`src/integrationTest/java/com/quince/cartrecovery/app/RoleThread.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import java.time.Duration;

/** Runs a role in-process on its own thread; close() interrupts it (the SIGTERM convention) and waits. */
public final class RoleThread implements AutoCloseable {
    private final Health health = new Health();
    private final Metrics metrics = new Metrics();
    private final Thread thread;
    private volatile Throwable failure;

    public RoleThread(Role role, InfraConfig config) {
        this.thread = Thread.ofPlatform().name("role-" + role.name()).start(() -> {
            try {
                role.run(config, health, metrics);
            } catch (Throwable e) {
                failure = e;
            }
        });
    }

    public Health health() { return health; }
    public Metrics metrics() { return metrics; }
    public Throwable failure() { return failure; }

    /** For one-off roles: true if the role returned within the timeout. */
    public boolean join(Duration timeout) throws InterruptedException {
        return thread.join(timeout);
    }

    @Override
    public void close() {
        thread.interrupt();
        try {
            thread.join(Duration.ofSeconds(40));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (thread.isAlive()) throw new AssertionError(thread.getName() + " did not stop within 40 s");
    }
}
```

`src/integrationTest/java/com/quince/cartrecovery/app/TopicTail.java`:

```java
package com.quince.cartrecovery.app;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Reads a whole topic from the beginning without a group and keeps every record seen, in partition order. */
public final class TopicTail implements AutoCloseable {
    private final KafkaConsumer<String, byte[]> consumer;
    private final List<ConsumerRecord<String, byte[]>> seen = new ArrayList<>();

    public TopicTail(String bootstrap, String topic) {
        consumer = new KafkaConsumer<>(Map.<String, Object>of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class));
        List<TopicPartition> parts = consumer.partitionsFor(topic).stream()
            .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
        consumer.assign(parts);
        consumer.seekToBeginning(parts);
    }

    /** Polls what is new, then returns every record so far whose key starts with keyPrefix. */
    public synchronized List<ConsumerRecord<String, byte[]>> records(String keyPrefix) {
        for (int i = 0; i < 5; i++) {
            ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(100));
            if (batch.isEmpty()) break;
            batch.forEach(seen::add);
        }
        return seen.stream().filter(r -> r.key() != null && r.key().startsWith(keyPrefix)).toList();
    }

    @Override
    public synchronized void close() {
        consumer.close();
    }
}
```

- [ ] **Step 16: Write the failing detector-role integration test**

`src/integrationTest/java/com/quince/cartrecovery/app/DetectorRoleIT.java`:

```java
package com.quince.cartrecovery.app;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.infra.dynamo.DynamoCartStateStore;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.TimerStore;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class DetectorRoleIT {
    static final Duration WAIT = Duration.ofSeconds(25);
    static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));

    @BeforeAll
    static void infra() { RoleInfra.start(); }

    @Test
    void writesTimerAndCartWithTheRecordedPartitionAndKeepsIdlePartitionsCurrent() {
        InfraConfig c = RoleInfra.config(Map.of("WINDOW", "PT60S", "OFFSETS", "PT60S,PT120S,PT180S"));
        String cart = RoleInfra.prefix("det") + "cart";
        DynamoCartStateStore carts = new DynamoCartStateStore(RoleInfra.ctx().dynamo(), DynamoTables.CARTS, c.recovery(), c.shards());
        TimerStore timers = new RedisTimerStore(RoleInfra.ctx().redis(), c.shards(), c.dispatch().lease());
        Watermark wm = new RedisWatermark(RoleInfra.ctx().redis(), c.partitions());
        try (RoleThread detector = new RoleThread(new DetectorRole(), c)) {
            RecordMetadata sent = RoleInfra.produce(new CartEvent.CartEdited(cart, "shopper", 1, Instant.now(), ITEMS, "Ada"));
            Await.until(() -> carts.get(cart).isPresent(), WAIT);
            CartRecord record = carts.get(cart).orElseThrow();
            assertEquals(CartStatus.ACTIVE, record.status());
            assertEquals(sent.partition(), record.srcPartition());
            assertTrue(timers.existing(Shards.of(cart, c.shards()), List.of(cart)).contains(cart));
            Await.until(() -> IntStream.range(0, c.partitions()).allMatch(p ->
                Duration.between(wm.current(p), wm.now()).compareTo(Duration.ofSeconds(3)) < 0), WAIT);
            assertNull(detector.failure());
        }
    }

    @Test
    void undecodableEventGoesToTheDeadLetterTopic() {
        String key = RoleInfra.prefix("det") + "poison";
        try (TopicTail dlq = new TopicTail(RoleInfra.bootstrap(), Topics.CART_EVENTS_DLQ);
             RoleThread detector = new RoleThread(new DetectorRole(), RoleInfra.config(Map.of()))) {
            RoleInfra.send(Topics.CART_EVENTS, key, "not json".getBytes(UTF_8));
            Await.until(() -> !dlq.records(key).isEmpty(), WAIT);
            assertNull(detector.failure());
        }
    }
}
```

- [ ] **Step 17: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.app.DetectorRoleIT' --console=plain`
Expected: FAIL, `compileIntegrationTestJava` reports `cannot find symbol ... class DetectorRole`.

- [ ] **Step 18: Implement `DetectorRole`**

`src/main/java/com/quince/cartrecovery/app/DetectorRole.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.AbandonmentDetector;
import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.inmemory.HashArmAssigner;
import com.quince.cartrecovery.infra.dynamo.DynamoCartStateStore;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.model.CartEvent;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Consumes cart-events: timer first, then the conditional cart update; publishes watermarks every loop. */
public final class DetectorRole implements Role {
    static final String GROUP = "detector";

    @Override
    public String name() { return "detector"; }

    @Override
    public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        try (RoleContext ctx = new RoleContext(config)) {
            ctx.verifyStartup();
            AbandonmentDetector detector = new AbandonmentDetector(config.recovery(),
                new DynamoCartStateStore(ctx.dynamo(), DynamoTables.CARTS, config.recovery(), config.shards()),
                new RedisTimerStore(ctx.redis(), config.shards(), config.dispatch().lease()),
                new HashArmAssigner(RoleContext.ARM_SALT, config.recovery().holdoutPercent()), metrics);
            DetectorWatermarkHooks hooks = new DetectorWatermarkHooks(
                new RedisWatermark(ctx.redis(), config.partitions()), health, metrics, System::nanoTime);
            BatchConsumerLoop<byte[]> loop = new BatchConsumerLoop<>(ctx.consumerProps(GROUP),
                new BatchConsumerLoop.Settings(GROUP, List.of(Topics.CART_EVENTS), 500, Duration.ofMillis(500),
                    config.maxInFlight(), Topics.CART_EVENTS_DLQ),
                new ByteArrayDeserializer(), record -> handle(detector, record), hooks, ctx.producer(), health, metrics);
            RoleContext.runLoops(loop::close, List.of(loop::run));
        }
    }

    /** Bad JSON, an unknown type, or a deterministic store error is poison (DLQ and commit); anything else is retried. */
    static BatchConsumerLoop.Verdict handle(AbandonmentDetector detector, ConsumerRecord<String, byte[]> record) {
        CartEvent event;
        try {
            event = JsonCodec.decodeCartEvent(record.value());
        } catch (RuntimeException e) {
            throw new PoisonException("undecodable cart event at " + record.topic() + "-" + record.partition()
                + "@" + record.offset(), e);
        }
        try {
            detector.handle(event, record.partition());
        } catch (RuntimeException e) {
            if (Failures.isDeterministic(e)) throw new PoisonException("deterministic failure: " + e.getMessage(), e);
            throw e;
        }
        return BatchConsumerLoop.Verdict.DONE;
    }
}
```

- [ ] **Step 19: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.app.DetectorRoleIT' --console=plain`
Expected: PASS, 2 tests.

- [ ] **Step 20: Write the failing scheduler-role integration test**

`src/integrationTest/java/com/quince/cartrecovery/app/SchedulerRoleIT.java`:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertNull;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class SchedulerRoleIT {
    static final Duration WAIT = Duration.ofSeconds(25);
    static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));

    @BeforeAll
    static void infra() { RoleInfra.start(); }

    @Test
    void abandonsTheCartAndPublishesTheFirstIntentOnTheFastLane() {
        InfraConfig c = RoleInfra.config(Map.of());
        String cart = RoleInfra.prefix("sched") + "cart";
        String firstKey = new LedgerKey(cart, 1, 0).toString();
        try (TopicTail outcomes = new TopicTail(RoleInfra.bootstrap(), Topics.OUTCOMES);
             TopicTail fast = new TopicTail(RoleInfra.bootstrap(), Topics.INTENTS_FAST);
             RoleThread detector = new RoleThread(new DetectorRole(), c);
             RoleThread scheduler = new RoleThread(new SchedulerRole(), c)) {
            RoleInfra.produce(new CartEvent.CartEdited(cart, "shopper", 1, Instant.now(), ITEMS, "Ada"));
            Await.until(() -> outcomes.records(cart).stream().map(r -> JsonCodec.decodeOutcome(r.value()))
                .anyMatch(o -> o.kind() == OutcomeKind.ABANDONED), WAIT);
            Await.until(() -> fast.records(cart).stream().map(r -> JsonCodec.decodeIntent(r.value()).key())
                .anyMatch(firstKey::equals), WAIT);
            assertNull(scheduler.failure());
            assertNull(detector.failure());
        }
    }
}
```

- [ ] **Step 21: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.app.SchedulerRoleIT' --console=plain`
Expected: FAIL, `cannot find symbol ... class SchedulerRole`.

- [ ] **Step 22: Implement `SchedulerRole`**

`src/main/java/com/quince/cartrecovery/app/SchedulerRole.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.core.ReminderScheduler;
import com.quince.cartrecovery.infra.dynamo.DynamoCartStateStore;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.kafka.KafkaIntentPublisher;
import com.quince.cartrecovery.infra.kafka.KafkaOutcomeRecorder;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerDecision;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/** Claims due timers from all shards, runs them concurrently on virtual threads, acks or releases each. */
public final class SchedulerRole implements Role {
    static final Duration IDLE = Duration.ofMillis(200);

    @Override
    public String name() { return "scheduler"; }

    @Override
    public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        try (RoleContext ctx = new RoleContext(config)) {
            ctx.verifyStartup();
            TimerStore timers = new RedisTimerStore(ctx.redis(), config.shards(), config.dispatch().lease());
            // The scheduler has no Clock: its time source is the Redis-timed watermark (controller ruling R13).
            ReminderScheduler scheduler = new ReminderScheduler(config.recovery(), config.dispatch(),
                new DynamoCartStateStore(ctx.dynamo(), DynamoTables.CARTS, config.recovery(), config.shards()), timers,
                new RedisWatermark(ctx.redis(), config.partitions()),
                new KafkaIntentPublisher(ctx.producer(), config.dispatch().fastOffsets()),
                new KafkaOutcomeRecorder(ctx.producer()), metrics);
            AtomicBoolean running = new AtomicBoolean(true);
            RoleContext.runLoops(() -> running.set(false),
                List.of(() -> claimLoop(timers, scheduler, config.maxInFlight(), running, health, metrics)));
        }
    }

    static void claimLoop(TimerStore timers, ReminderScheduler scheduler, int batch, AtomicBoolean running,
                          Health health, Metrics metrics) {
        while (running.get()) {
            health.beat("scheduler");
            List<Timer> due;
            try {
                due = timers.claimDue(batch);
            } catch (RuntimeException e) {
                metrics.increment("scheduler.claim_failed");   // Redis unreachable: keep beating, try again
                due = List.of();
            }
            if (due.isEmpty()) {
                if (!RoleContext.sleep(IDLE)) return;
                continue;
            }
            try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
                for (Timer t : due) exec.submit(() -> fire(timers, scheduler, t, metrics));
            }
        }
    }

    /**
     * Ack after processing; Release for the gate. Deterministic SDK errors (DynamoDB ValidationException and other
     * non-retryable 400s, see Failures) are poison: ack and count timers.poison (controller ruling R13). Others stay
     * leased for redelivery.
     */
    static void fire(TimerStore timers, ReminderScheduler scheduler, Timer timer, Metrics metrics) {
        try {
            switch (scheduler.onTimer(timer)) {
                case TimerDecision.Ack ack -> timers.ack(timer);
                case TimerDecision.Release release -> timers.release(timer, release.delay());
            }
        } catch (RuntimeException e) {
            if (Failures.isDeterministic(e)) {
                timers.ack(timer);
                metrics.increment("timers.poison");
            } else {
                metrics.increment("scheduler.timer_failed");
            }
        }
    }
}
```

- [ ] **Step 23: Run it and the full unit suite**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.app.SchedulerRoleIT' --console=plain`
Expected: PASS, 1 test.

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`, no failures.

- [ ] **Step 24: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/app/DetectorRole.java src/main/java/com/quince/cartrecovery/app/SchedulerRole.java src/integrationTest/java/com/quince/cartrecovery/app/
git commit -m "Add detector and scheduler roles with container tests

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task C1b: Dispatcher role

**Model:** opus (many interacting pause conditions).

**Files:**
- Create: `src/main/java/com/quince/cartrecovery/app/DispatcherRole.java`
- Test: `src/test/java/com/quince/cartrecovery/app/DispatcherRoleTest.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/app/DispatcherRoleIT.java`

**Interfaces:**
- Consumes: `Dispatcher(RecoveryConfig, DispatchConfig, CartStateStore, SendLedger, Watermark, SendBudget, NotificationSink, OutcomeRecorder, DeadLetterQueue, Clock, Metrics)` with `handle(ReminderIntent) → HandleResult`, `retryDue(int, int)`; `TokenBucket(double, double, LongSupplier)` with `tryAcquire`, `slowAllowed()`, `anyAvailable()`; `CircuitBreaker(NotificationSink, int, double, Duration, Clock)` with `isOpen()`; `BatchConsumerLoop.pauseWhile(Predicate<TopicPartition>)`; from C1a: `RoleContext`, `RoleInfra`, `RoleThread`, `TopicTail`; B (master §1.6): `RecoveryMetaStore.read()/setPaused(boolean)`, `KafkaRecordingSink(Producer, Clock, double failureRate, Random)` (records a `sink-sends` row only on a successful send, controller ruling R17), `KafkaOutcomeRecorder(Producer)`, `KafkaDeadLetterQueue(Producer)`, `DynamoCartStateStore(DynamoDbClient, DynamoTables.CARTS, RecoveryConfig, int)`, `DynamoSendLedger(DynamoDbClient, DynamoTables.SEND_LEDGER, Duration, int)`, `RedisWatermark(StatefulRedisConnection, int)`, `Topics`, `JsonCodec.decodeIntent/decodeSinkSend`.
- Produces: `public final class DispatcherRole implements Role` (name `"dispatcher"`, public no-arg constructor); `static Predicate<TopicPartition> pauseFast(TokenBucket, CircuitBreaker, BooleanSupplier)`, `static Predicate<TopicPartition> pauseSlow(TokenBucket, CircuitBreaker, BooleanSupplier)`, `static int effectiveAttempts(Duration bound, Duration retryBase, int maxAttempts)`. Consumer groups `dispatcher-fast`, `dispatcher-slow`. Ready keys `breaker` (`open`/`closed`) and `paused` (`true`/`false`). Metrics `dispatch.breaker_open`, `dispatch.retry_error`, `dispatcher.meta_read_failed`.

- [ ] **Step 1: Write the failing pause-condition and attempt-count tests**

`src/test/java/com/quince/cartrecovery/app/DispatcherRoleTest.java`:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Predicate;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class DispatcherRoleTest {
    static final TopicPartition TP = new TopicPartition("reminder-intents-fast", 0);
    static final ReminderMessage MESSAGE = new ReminderMessage("c:1:0", "c", "s", "Ada", List.of());

    final long[] nanos = {0};
    final TokenBucket bucket = new TokenBucket(10, 0.3, () -> nanos[0]);
    final CircuitBreaker breaker = new CircuitBreaker(m -> SendResult.TRANSIENT_FAILURE, 4, 0.5,
        Duration.ofSeconds(30), () -> Instant.parse("2026-01-01T00:00:00Z"));
    boolean metaPaused;

    Predicate<TopicPartition> fast() { return DispatcherRole.pauseFast(bucket, breaker, () -> metaPaused); }
    Predicate<TopicPartition> slow() { return DispatcherRole.pauseSlow(bucket, breaker, () -> metaPaused); }

    void drainAll() {
        for (int i = 0; i < 1000 && bucket.tryAcquire(Lane.FAST); i++) { }
    }

    @Test
    void fullBucketPausesNothing() {
        assertFalse(fast().test(TP));
        assertFalse(slow().test(TP));
    }

    @Test
    void bucketAtReservePausesOnlyTheSlowLane() {
        for (int i = 0; i < 1000 && bucket.slowAllowed(); i++) assertTrue(bucket.tryAcquire(Lane.FAST));
        assertTrue(bucket.anyAvailable());
        assertFalse(fast().test(TP));
        assertTrue(slow().test(TP));
    }

    @Test
    void emptyBucketPausesBothLanes() {
        drainAll();
        assertTrue(fast().test(TP));
        assertTrue(slow().test(TP));
    }

    @Test
    void refilledBucketResumes() {
        drainAll();
        nanos[0] += Duration.ofSeconds(1).toNanos();
        assertFalse(fast().test(TP));
        assertFalse(slow().test(TP));
    }

    @Test
    void openBreakerPausesBothLanes() {
        for (int i = 0; i < 4; i++) breaker.send(MESSAGE);
        assertTrue(breaker.isOpen());
        assertTrue(fast().test(TP));
        assertTrue(slow().test(TP));
    }

    @Test
    void guardrailSwitchPausesBothLanes() {
        metaPaused = true;
        assertTrue(fast().test(TP));
        assertTrue(slow().test(TP));
    }

    @Test
    void effectiveAttemptsFitTheLatenessBound() {
        assertEquals(3, DispatcherRole.effectiveAttempts(Duration.ofMinutes(5), Duration.ofMinutes(1), 5));
        assertEquals(5, DispatcherRole.effectiveAttempts(Duration.ofMinutes(30), Duration.ofMinutes(1), 5));
        assertEquals(1, DispatcherRole.effectiveAttempts(Duration.ZERO, Duration.ofMinutes(1), 5));
        assertEquals(2, DispatcherRole.effectiveAttempts(Duration.ofHours(1), Duration.ofMinutes(1), 2));
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.DispatcherRoleTest' --console=plain`
Expected: FAIL, `cannot find symbol ... DispatcherRole`.

- [ ] **Step 3: Implement `DispatcherRole`**

`src/main/java/com/quince/cartrecovery/app/DispatcherRole.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Dispatcher;
import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.infra.dynamo.DynamoCartStateStore;
import com.quince.cartrecovery.infra.dynamo.DynamoSendLedger;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.KafkaDeadLetterQueue;
import com.quince.cartrecovery.infra.kafka.KafkaOutcomeRecorder;
import com.quince.cartrecovery.infra.kafka.KafkaRecordingSink;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.model.HandleResult;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.ports.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Two lane consumers (fast, slow) sharing one token bucket, a breaker-wrapped recording sink, and a control
 * loop that polls the ledger retry index every RETRY_POLL and re-reads the guardrail switch every 5 s.
 */
public final class DispatcherRole implements Role {
    static final int MAX_POLL_RECORDS = 50;
    static final int RETRY_LIMIT = 100;
    static final Duration META_POLL = Duration.ofSeconds(5);

    @Override
    public String name() { return "dispatcher"; }

    @Override
    public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        try (RoleContext ctx = new RoleContext(config)) {
            ctx.verifyStartup();
            logAttempts(config);
            Clock clock = Instant::now;
            TokenBucket budget = new TokenBucket(config.maxSendRate(), config.fastReserve(), System::nanoTime);
            CircuitBreaker breaker = new CircuitBreaker(
                new KafkaRecordingSink(ctx.producer(), clock, config.sendFailureRate(), new Random()), 100, 0.5, Duration.ofSeconds(30), clock);
            Dispatcher dispatcher = new Dispatcher(config.recovery(), config.dispatch(),
                new DynamoCartStateStore(ctx.dynamo(), DynamoTables.CARTS, config.recovery(), config.shards()),
                new DynamoSendLedger(ctx.dynamo(), DynamoTables.SEND_LEDGER, config.dispatch().lease(), config.shards()),
                new RedisWatermark(ctx.redis(), config.partitions()), budget, breaker,
                new KafkaOutcomeRecorder(ctx.producer()), new KafkaDeadLetterQueue(ctx.producer()), clock, metrics);
            RecoveryMetaStore meta = new RecoveryMetaStore(ctx.dynamo());
            AtomicBoolean metaPaused = new AtomicBoolean(readPaused(meta, false, metrics));
            AtomicBoolean running = new AtomicBoolean(true);
            BatchConsumerLoop<byte[]> fast = lane(ctx, config, "dispatcher-fast", Topics.INTENTS_FAST, dispatcher, health, metrics);
            BatchConsumerLoop<byte[]> slow = lane(ctx, config, "dispatcher-slow", Topics.INTENTS_SLOW, dispatcher, health, metrics);
            fast.pauseWhile(pauseFast(budget, breaker, metaPaused::get));
            slow.pauseWhile(pauseSlow(budget, breaker, metaPaused::get));
            Runnable control = () -> controlLoop(dispatcher, breaker, meta, metaPaused, config, running, health, metrics);
            RoleContext.runLoops(() -> {
                running.set(false);
                fast.close();
                slow.close();
            }, List.of(fast::run, slow::run, control));
        }
    }

    /** Fast lane pauses when the bucket is empty; both pause while the breaker is open or the guardrail switch is set. */
    static Predicate<TopicPartition> pauseFast(TokenBucket budget, CircuitBreaker breaker, BooleanSupplier metaPaused) {
        return tp -> breaker.isOpen() || metaPaused.getAsBoolean() || !budget.anyAvailable();
    }

    /** Slow lane pauses while the bucket is at or below the fast reserve. */
    static Predicate<TopicPartition> pauseSlow(TokenBucket budget, CircuitBreaker breaker, BooleanSupplier metaPaused) {
        return tp -> breaker.isOpen() || metaPaused.getAsBoolean() || !budget.slowAllowed();
    }

    static BatchConsumerLoop<byte[]> lane(RoleContext ctx, InfraConfig config, String group, String topic,
                                          Dispatcher dispatcher, Health health, Metrics metrics) {
        return new BatchConsumerLoop<>(ctx.consumerProps(group),
            new BatchConsumerLoop.Settings(group, List.of(topic), MAX_POLL_RECORDS, Duration.ofMillis(500),
                config.maxInFlight(), Topics.REMINDER_DLQ),
            new ByteArrayDeserializer(), record -> handleIntent(dispatcher, record),
            new BatchConsumerLoop.Hooks<>() {}, ctx.producer(), health, metrics);
    }

    /** An undeserializable intent is poison (to reminder-dlq); HOLD pauses and seeks back the record's partition. */
    static BatchConsumerLoop.Verdict handleIntent(Dispatcher dispatcher, ConsumerRecord<String, byte[]> record) {
        ReminderIntent intent;
        try {
            intent = JsonCodec.decodeIntent(record.value());
        } catch (RuntimeException e) {
            throw new PoisonException("undecodable intent at " + record.topic() + "-" + record.partition()
                + "@" + record.offset(), e);
        }
        return dispatcher.handle(intent) == HandleResult.HOLD ? BatchConsumerLoop.Verdict.HOLD : BatchConsumerLoop.Verdict.DONE;
    }

    static void controlLoop(Dispatcher dispatcher, CircuitBreaker breaker, RecoveryMetaStore meta, AtomicBoolean metaPaused,
                            InfraConfig config, AtomicBoolean running, Health health, Metrics metrics) {
        long nextMetaRead = System.nanoTime() + META_POLL.toNanos();
        boolean wasOpen = false;
        while (running.get()) {
            health.beat("dispatcher-retry");
            if (System.nanoTime() >= nextMetaRead) {
                metaPaused.set(readPaused(meta, metaPaused.get(), metrics));
                nextMetaRead = System.nanoTime() + META_POLL.toNanos();
            }
            boolean open = breaker.isOpen();
            if (open && !wasOpen) metrics.increment("dispatch.breaker_open");
            wasOpen = open;
            health.setReady("breaker", open ? "open" : "closed");
            health.setReady("paused", Boolean.toString(metaPaused.get()));
            if (!open && !metaPaused.get()) {
                int start = ThreadLocalRandom.current().nextInt(config.shards());
                for (int i = 0; i < config.shards() && running.get(); i++) {
                    try {
                        dispatcher.retryDue((start + i) % config.shards(), RETRY_LIMIT);
                    } catch (RuntimeException e) {
                        metrics.increment("dispatch.retry_error");
                    }
                }
            }
            if (!RoleContext.sleep(config.retryPoll())) return;
        }
    }

    /** The guardrail switch; on a read failure (including a missing item) keep the last known value. */
    static boolean readPaused(RecoveryMetaStore meta, boolean last, Metrics metrics) {
        try {
            return meta.read().paused();
        } catch (RuntimeException e) {
            metrics.increment("dispatcher.meta_read_failed");
            return last;
        }
    }

    /** Attempts that fit in a lateness bound when each retry waits at most RETRY_BASE × 2^(n−1) (spec §6.3). */
    static int effectiveAttempts(Duration bound, Duration retryBase, int maxAttempts) {
        int attempts = 1;
        Duration waited = Duration.ZERO;
        Duration next = retryBase;
        while (attempts < maxAttempts && waited.plus(next).compareTo(bound) <= 0) {
            waited = waited.plus(next);
            next = next.multipliedBy(2);
            attempts++;
        }
        return attempts;
    }

    private static void logAttempts(InfraConfig config) {
        List<Duration> bounds = config.recovery().latenessBounds();
        for (int i = 0; i < bounds.size(); i++) {
            System.out.printf("dispatcher: offset %d allows about %d send attempts within its %s lateness bound%n", i,
                effectiveAttempts(bounds.get(i), config.recovery().retryBase(), config.recovery().maxSendAttempts()), bounds.get(i));
        }
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.app.DispatcherRoleTest' --console=plain`
Expected: PASS, 7 tests.

- [ ] **Step 5: Write the failing dispatcher-role integration test**

`src/integrationTest/java/com/quince/cartrecovery/app/DispatcherRoleIT.java`:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.LedgerKey;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class DispatcherRoleIT {
    static final Duration WAIT = Duration.ofSeconds(25);
    static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));

    @BeforeAll
    static void infra() { RoleInfra.start(); }

    static List<String> sentKeys(TopicTail sends, String prefix) {
        return sends.records(prefix).stream().map(r -> JsonCodec.decodeSinkSend(r.value()).key()).toList();
    }

    @Test
    void sendsTheFirstReminderAndShowsBreakerAndPauseOnReady() throws Exception {
        InfraConfig c = RoleInfra.config(Map.of());
        String cart = RoleInfra.prefix("disp") + "cart";
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        try (TopicTail sends = new TopicTail(RoleInfra.bootstrap(), Topics.SINK_SENDS);
             RoleThread detector = new RoleThread(new DetectorRole(), c);
             RoleThread scheduler = new RoleThread(new SchedulerRole(), c);
             RoleThread dispatcher = new RoleThread(new DispatcherRole(), c);
             HealthServer server = new HealthServer(port, dispatcher.health(), dispatcher.metrics(), Duration.ofSeconds(15))) {
            RoleInfra.produce(new CartEvent.CartEdited(cart, "shopper", 1, Instant.now(), ITEMS, "Ada"));
            Await.until(() -> sentKeys(sends, cart).contains(new LedgerKey(cart, 1, 0).toString()), WAIT);
            String ready = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/ready")).build(),
                HttpResponse.BodyHandlers.ofString()).body();
            assertTrue(ready.contains("breaker"), ready);
            assertTrue(ready.contains("paused"), ready);
            assertNull(dispatcher.failure());
        }
    }

    @Test
    void guardrailPauseStopsAndResumesSending() throws Exception {
        InfraConfig c = RoleInfra.config(Map.of());
        String cart = RoleInfra.prefix("pause") + "cart";
        RecoveryMetaStore meta = new RecoveryMetaStore(RoleInfra.ctx().dynamo());
        meta.setPaused(true);
        try (TopicTail sends = new TopicTail(RoleInfra.bootstrap(), Topics.SINK_SENDS);
             RoleThread detector = new RoleThread(new DetectorRole(), c);
             RoleThread scheduler = new RoleThread(new SchedulerRole(), c);
             RoleThread dispatcher = new RoleThread(new DispatcherRole(), c)) {
            Instant t0 = Instant.now();
            RoleInfra.produce(new CartEvent.CartEdited(cart, "shopper", 1, t0, ITEMS, "Ada"));
            Thread.sleep(Math.max(0, Duration.between(Instant.now(), t0.plusSeconds(7)).toMillis()));   // reminder 0 due at +3 s
            assertEquals(List.of(), sentKeys(sends, cart));
            meta.setPaused(false);
            Await.until(() -> sentKeys(sends, cart).contains(new LedgerKey(cart, 1, 0).toString()), WAIT);
            assertNull(dispatcher.failure());
        } finally {
            meta.setPaused(false);
        }
    }
}
```

- [ ] **Step 6: Run it to verify it passes**

`DispatcherRole` already exists, so this test is expected to pass on the first run; if it fails, fix the role, not the test.

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.app.DispatcherRoleIT' --console=plain`
Expected: PASS, 2 tests.

- [ ] **Step 7: Run the unit suite**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/app/DispatcherRole.java src/test/java/com/quince/cartrecovery/app/DispatcherRoleTest.java src/integrationTest/java/com/quince/cartrecovery/app/DispatcherRoleIT.java
git commit -m "Add dispatcher role with lane pausing, breaker, and retry loop

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task C1c: Reconciler, replay, init roles, and Main dispatch

**Model:** sonnet.

**Files:**
- Create: `src/main/java/com/quince/cartrecovery/app/ReconcilerRole.java`
- Create: `src/main/java/com/quince/cartrecovery/app/ReplayRole.java`
- Create: `src/main/java/com/quince/cartrecovery/app/InitRole.java`
- Create: `src/main/java/com/quince/cartrecovery/app/RoleRegistry.java`
- Modify: `src/main/java/com/quince/cartrecovery/Main.java` (rename A4's `main` to `runDemo`, add dispatch)
- Test: `src/test/java/com/quince/cartrecovery/MainTest.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/app/InitRoleIT.java`, `ReconcilerRoleIT.java`, `ReplayRoleIT.java`

**Interfaces:**
- Consumes: `Reconciler(RecoveryConfig, CartStateStore, TimerStore, SendLedger, Clock, Metrics).reconcileShard(int)`; `Dispatcher.replay(List<DeadLetter>)`; `Timer.checkAbandon(String, long, Instant, int)`; `TimerStore.upsert/remove/existing`; `SendLedger.claim/finish/dueRetries`; `DeadLetter.REASON_POISON`; `UnlimitedSendBudget`; `InfraConfig.fromEnv`, `ConfigException`, `Health`, `HealthServer`, `Role`; from C1a: `RoleContext`, `DetectorRole`, `SchedulerRole`, `RoleInfra`, `RoleThread`; from C1b: `DispatcherRole` (registry; C1c depends on C1b, controller ruling R1); B (master §1.6): `DynamoTables.createAll`, `RecoveryMetaStore` (`init`, `read`, `setRedisIdentity`, `markRedisChange`), `RedisMeta` (`epochPresent`, `writeEpoch`, `runId`, `role`, `EPOCH`), `TopicAdmin.createAll/partitionCounts`, `Topics`, `JsonCodec.decodeCartEvent/decodeDeadLetter/encode`, `KafkaDeadLetterQueue.add`, `KafkaRecordingSink`, `RedisTimerStore`; D1: `LoadgenRole()` (no-arg; reads `RATE`, `DURATION`, `RUN_PREFIX` from the environment, controller ruling R9).
- Produces: roles `ReconcilerRole` (`"reconciler"`, `public static final String EPOCH_KEY = RedisMeta.EPOCH`, i.e. `"epoch"`), `ReplayRole` (`"replay"`, consumer group `replay`), `InitRole` (`"init"`); `RoleRegistry.create(String name) → Optional<Role>`, `RoleRegistry.NAMES`; `Main.run(String[] args, Map<String,String> env) → int` (0 ok, 1 role failed or healthcheck unhealthy, 2 usage or config error); modes `--mode=inmemory` (default), `--role=<name>`, `--role=healthcheck`. Metrics `reconciler.sweeps`, `reconciler.replays`, `reconciler.errors`, `replay.undecodable`. Ready keys `reconciler.sweep_ms`, `reconciler.sweep_slow`.

- [ ] **Step 1: Write the failing init-role integration test**

`src/integrationTest/java/com/quince/cartrecovery/app/InitRoleIT.java`:

```java
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
```

- [ ] **Step 2: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.app.InitRoleIT' --console=plain`
Expected: FAIL, `cannot find symbol ... InitRole` / `ReconcilerRole`.

- [ ] **Step 3: Implement `InitRole` and `ReconcilerRole`**

`src/main/java/com/quince/cartrecovery/app/InitRole.java`:

```java
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
```

`src/main/java/com/quince/cartrecovery/app/ReconcilerRole.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.core.Reconciler;
import com.quince.cartrecovery.infra.dynamo.DynamoCartStateStore;
import com.quince.cartrecovery.infra.dynamo.DynamoSendLedger;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.infra.redis.RedisMeta;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Every second: on a Redis run_id or role change, replay the last 60 s of cart-events into timer upserts
 * (spec §6.2 failover replay); otherwise sweep all shards when the epoch is missing, at start, and every
 * RECONCILE_INTERVAL. One piece of work runs at a time on a platform worker thread.
 */
public final class ReconcilerRole implements Role {
    public static final String EPOCH_KEY = RedisMeta.EPOCH;
    static final Duration TICK = Duration.ofSeconds(1);
    static final Duration REPLAY_LOOKBACK = Duration.ofSeconds(60);

    @Override
    public String name() { return "reconciler"; }

    @Override
    public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        try (RoleContext ctx = new RoleContext(config)) {
            ctx.verifyStartup();
            TimerStore timers = new RedisTimerStore(ctx.redis(), config.shards(), config.dispatch().lease());
            Reconciler reconciler = new Reconciler(config.recovery(),
                new DynamoCartStateStore(ctx.dynamo(), DynamoTables.CARTS, config.recovery(), config.shards()), timers,
                new DynamoSendLedger(ctx.dynamo(), DynamoTables.SEND_LEDGER, config.dispatch().lease(), config.shards()), Instant::now, metrics);
            Cycle cycle = new Cycle(config, new RedisMeta(ctx.redis()), timers, reconciler, new RecoveryMetaStore(ctx.dynamo()), health, metrics);
            AtomicBoolean running = new AtomicBoolean(true);
            RoleContext.runLoops(() -> running.set(false), List.of(() -> cycle.loop(running)));
        }
    }

    private static final class Cycle {
        private final InfraConfig config;
        private final RedisMeta redis;
        private final TimerStore timers;
        private final Reconciler reconciler;
        private final RecoveryMetaStore meta;
        private final Health health;
        private final Metrics metrics;
        private final Duration smallestBound;
        private Future<?> current;
        private Instant lastSweep;

        Cycle(InfraConfig config, RedisMeta redis, TimerStore timers, Reconciler reconciler,
              RecoveryMetaStore meta, Health health, Metrics metrics) {
            this.config = config;
            this.redis = redis;
            this.timers = timers;
            this.reconciler = reconciler;
            this.meta = meta;
            this.health = health;
            this.metrics = metrics;
            this.smallestBound = config.recovery().latenessBounds().stream().min(Comparator.naturalOrder()).orElseThrow();
        }

        void loop(AtomicBoolean running) {
            // Platform worker: the replay drives a KafkaConsumer, which must not run on a virtual thread.
            ExecutorService work = Executors.newSingleThreadExecutor(Thread.ofPlatform().name("reconciler-work").factory());
            try {
                while (running.get()) {
                    health.beat("reconciler");
                    if (current == null || current.isDone()) {
                        report();
                        current = next(work);
                    }
                    if (!RoleContext.sleep(TICK)) return;
                }
            } finally {
                work.shutdownNow();
            }
        }

        private void report() {
            if (current == null) return;
            try {
                current.get();
            } catch (ExecutionException e) {
                metrics.increment("reconciler.errors");
                System.err.println("reconciler: " + e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            current = null;
        }

        private Future<?> next(ExecutorService work) {
            try {
                String runId = redis.runId();
                String role = redis.role();
                RecoveryMetaStore.Meta m = meta.read();   // throws when init has not run: counted below, retried next tick
                if (m.redisRunId() == null) {
                    meta.setRedisIdentity(runId, role);
                } else if (!runId.equals(m.redisRunId()) || !role.equals(m.redisRole())) {
                    meta.markRedisChange(Instant.now());   // keeps the earliest unrepaired change
                    Instant stored = meta.read().redisChangeAt();
                    Instant changeAt = stored != null ? stored : Instant.now();
                    return work.submit(() -> { replay(runId, role, changeAt); return null; });
                }
                boolean due = lastSweep == null || !Instant.now().isBefore(lastSweep.plus(config.reconcileInterval()));
                if (due || !redis.epochPresent()) {
                    lastSweep = Instant.now();
                    return work.submit(() -> { sweep(); return null; });
                }
            } catch (RuntimeException e) {
                metrics.increment("reconciler.errors");   // Redis or DynamoDB unreachable: try again next tick
            }
            return null;
        }

        private void sweep() throws Exception {
            long start = System.nanoTime();
            try (ExecutorService shards = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> futures = new ArrayList<>();
                for (int s = 0; s < config.shards(); s++) {
                    int shard = s;
                    futures.add(shards.submit(() -> reconciler.reconcileShard(shard)));
                }
                for (Future<?> f : futures) f.get();
            }
            redis.writeEpoch();
            Duration took = Duration.ofNanos(System.nanoTime() - start);
            boolean slow = took.compareTo(smallestBound) > 0;
            health.setReady("reconciler.sweep_ms", Long.toString(took.toMillis()));
            health.setReady("reconciler.sweep_slow", Boolean.toString(slow));
            metrics.increment("reconciler.sweeps");
            System.out.printf("reconciler: sweep of %d shards took %d ms%s%n", config.shards(), took.toMillis(),
                slow ? " (longer than the smallest lateness bound)" : "");
        }

        /**
         * Re-issues CHECK_ABANDON upserts for every edit or resume appended since changeAt − 60 s, up to the end
         * offsets read at the start. Aborts if Redis changes again; the next tick restarts from the earliest
         * change still stored in recovery-meta. Stores the new identity only after a complete replay.
         */
        private void replay(String targetRunId, String targetRole, Instant changeAt) {
            Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            long upserts = 0;
            try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
                List<TopicPartition> parts = consumer.partitionsFor(Topics.CART_EVENTS).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
                consumer.assign(parts);
                Map<TopicPartition, Long> ends = consumer.endOffsets(parts);
                long from = changeAt.minus(REPLAY_LOOKBACK).toEpochMilli();
                Map<TopicPartition, Long> query = new HashMap<>();
                for (TopicPartition p : parts) query.put(p, from);
                Map<TopicPartition, OffsetAndTimestamp> starts = consumer.offsetsForTimes(query);
                for (TopicPartition p : parts) {
                    OffsetAndTimestamp s = starts.get(p);
                    consumer.seek(p, s == null ? ends.get(p) : s.offset());
                }
                while (parts.stream().anyMatch(p -> consumer.position(p) < ends.get(p))) {
                    if (!targetRunId.equals(redis.runId()) || !targetRole.equals(redis.role())) {
                        throw new IllegalStateException("Redis changed again during failover replay; restarting from " + changeAt);
                    }
                    for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(500))) {
                        if (r.offset() >= ends.get(new TopicPartition(r.topic(), r.partition()))) continue;
                        CartEvent event;
                        try {
                            event = JsonCodec.decodeCartEvent(r.value());
                        } catch (RuntimeException e) {
                            continue;   // poison: the detector dead-letters it
                        }
                        if (event instanceof CartEvent.CartEdited || event instanceof CartEvent.CartResumed) {
                            timers.upsert(Timer.checkAbandon(event.cartId(), event.version(),
                                event.occurredAt().plus(config.recovery().window()), r.partition()));
                            upserts++;
                        }
                    }
                }
            }
            meta.setRedisIdentity(targetRunId, targetRole);
            metrics.increment("reconciler.replays");
            System.out.printf("reconciler: failover replay from %s re-issued %d timer upserts%n", changeAt.minus(REPLAY_LOOKBACK), upserts);
        }
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.app.InitRoleIT' --console=plain`
Expected: PASS, 3 tests.

- [ ] **Step 5: Write the reconciler-role integration test**

`src/integrationTest/java/com/quince/cartrecovery/app/ReconcilerRoleIT.java`:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class ReconcilerRoleIT {
    static final Duration WAIT = Duration.ofSeconds(25);
    static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));

    @BeforeAll
    static void infra() { RoleInfra.start(); }

    @Test
    void missingEpochTriggersAnImmediateSweepThatRebuildsTheTimer() {
        InfraConfig c = RoleInfra.config(Map.of("WINDOW", "PT60S", "OFFSETS", "PT60S,PT120S,PT180S"));
        TimerStore timers = new RedisTimerStore(RoleInfra.ctx().redis(), c.shards(), c.dispatch().lease());
        String cart = RoleInfra.prefix("recon") + "cart";
        int shard = Shards.of(cart, c.shards());
        try (RoleThread detector = new RoleThread(new DetectorRole(), c)) {
            RoleInfra.produce(new CartEvent.CartEdited(cart, "shopper", 1, Instant.now(), ITEMS, "Ada"));
            Await.until(() -> timers.existing(shard, List.of(cart)).contains(cart), WAIT);
        }
        try (RoleThread reconciler = new RoleThread(new ReconcilerRole(), c)) {
            Await.until(() -> reconciler.metrics().get("reconciler.sweeps") >= 1, WAIT);
            timers.remove(cart, 1);
            RoleInfra.ctx().redis().sync().del(ReconcilerRole.EPOCH_KEY);
            assertFalse(timers.existing(shard, List.of(cart)).contains(cart));
            Await.until(() -> timers.existing(shard, List.of(cart)).contains(cart), WAIT);
            assertTrue(reconciler.metrics().get("reconciler.sweeps") >= 2);
            assertNotNull(RoleInfra.ctx().redis().sync().get(ReconcilerRole.EPOCH_KEY));
            assertNull(reconciler.failure());
        }
    }
}
```

- [ ] **Step 6: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.app.ReconcilerRoleIT' --console=plain`
Expected: PASS, 1 test.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/app/InitRole.java src/main/java/com/quince/cartrecovery/app/ReconcilerRole.java src/integrationTest/java/com/quince/cartrecovery/app/InitRoleIT.java src/integrationTest/java/com/quince/cartrecovery/app/ReconcilerRoleIT.java
git commit -m "Add init and reconciler roles with failover replay

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 8: Write the failing replay-role integration test**

`src/integrationTest/java/com/quince/cartrecovery/app/ReplayRoleIT.java`:

```java
package com.quince.cartrecovery.app;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.infra.dynamo.DynamoSendLedger;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.kafka.KafkaDeadLetterQueue;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.Shards;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class ReplayRoleIT {
    @BeforeAll
    static void infra() { RoleInfra.start(); }

    static ReminderIntent intent(String cartId, Instant now) {
        return new ReminderIntent(new LedgerKey(cartId, 1, 0).toString(), cartId, 1, 0, 0, now, now.plus(Duration.ofMinutes(10)));
    }

    static Set<String> dueKeys(DynamoSendLedger ledger, String cartId, int shards) {
        return ledger.dueRetries(Shards.of(cartId, shards), Instant.now().plusSeconds(1), 1000).stream()
            .map(DueRetry::key).collect(Collectors.toSet());
    }

    @Test
    void reopensDeadRowsAndSkipsPoisonAndUndecodableRecords() throws Exception {
        InfraConfig c = RoleInfra.config(Map.of());
        DynamoSendLedger ledger = new DynamoSendLedger(RoleInfra.ctx().dynamo(), DynamoTables.SEND_LEDGER, c.dispatch().lease(), c.shards());
        KafkaDeadLetterQueue dlq = new KafkaDeadLetterQueue(RoleInfra.ctx().producer());
        String prefix = RoleInfra.prefix("replay");
        Instant now = Instant.now();
        ReminderIntent dead = intent(prefix + "dead", now);
        ReminderIntent poison = intent(prefix + "poison", now);
        for (ReminderIntent i : List.of(dead, poison)) {
            ClaimResult.Claimed claim = (ClaimResult.Claimed) ledger.claim(i.key(), i.sendBy(), i.srcPartition(), now);
            assertTrue(ledger.finish(i.key(), claim.token(), OutcomeKind.DEAD, "permanent"));
        }
        dlq.add(new DeadLetter(dead, "permanent", now));
        dlq.add(new DeadLetter(poison, DeadLetter.REASON_POISON, now));
        RoleInfra.send(Topics.REMINDER_DLQ, prefix + "garbage", "not json".getBytes(UTF_8));

        RoleThread replay = new RoleThread(new ReplayRole(), c);
        assertTrue(replay.join(Duration.ofSeconds(25)), "replay is a one-off and returns when caught up");
        assertNull(replay.failure());

        assertTrue(dueKeys(ledger, dead.cartId(), c.shards()).contains(dead.key()));
        assertFalse(dueKeys(ledger, poison.cartId(), c.shards()).contains(poison.key()));
    }
}
```

- [ ] **Step 9: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.app.ReplayRoleIT' --console=plain`
Expected: FAIL, `cannot find symbol ... ReplayRole`.

- [ ] **Step 10: Implement `ReplayRole`**

`src/main/java/com/quince/cartrecovery/app/ReplayRole.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Dispatcher;
import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.inmemory.UnlimitedSendBudget;
import com.quince.cartrecovery.infra.dynamo.DynamoCartStateStore;
import com.quince.cartrecovery.infra.dynamo.DynamoSendLedger;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.KafkaDeadLetterQueue;
import com.quince.cartrecovery.infra.kafka.KafkaOutcomeRecorder;
import com.quince.cartrecovery.infra.kafka.KafkaRecordingSink;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.ports.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** One-off: reads reminder-dlq with its own group up to the end offsets at start and reopens DEAD ledger rows. */
public final class ReplayRole implements Role {
    static final String GROUP = "replay";

    @Override
    public String name() { return "replay"; }

    @Override
    public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        try (RoleContext ctx = new RoleContext(config);
             KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(ctx.consumerProps(GROUP))) {
            ctx.verifyStartup();
            Clock clock = Instant::now;
            Dispatcher dispatcher = new Dispatcher(config.recovery(), config.dispatch(),
                new DynamoCartStateStore(ctx.dynamo(), DynamoTables.CARTS, config.recovery(), config.shards()),
                new DynamoSendLedger(ctx.dynamo(), DynamoTables.SEND_LEDGER, config.dispatch().lease(), config.shards()),
                new RedisWatermark(ctx.redis(), config.partitions()), new UnlimitedSendBudget(),
                new KafkaRecordingSink(ctx.producer(), clock, 0.0, new Random()),
                new KafkaOutcomeRecorder(ctx.producer()), new KafkaDeadLetterQueue(ctx.producer()), clock, metrics);
            List<TopicPartition> parts = consumer.partitionsFor(Topics.REMINDER_DLQ).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
            consumer.assign(parts);
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(parts));
            for (TopicPartition p : parts) {
                OffsetAndMetadata o = committed.get(p);
                if (o == null) consumer.seekToBeginning(List.of(p)); else consumer.seek(p, o.offset());
            }
            Map<TopicPartition, Long> ends = consumer.endOffsets(parts);
            int replayed = 0;
            while (parts.stream().anyMatch(p -> consumer.position(p) < ends.get(p))) {
                if (Thread.currentThread().isInterrupted()) return;
                health.beat("replay");
                List<DeadLetter> batch = new ArrayList<>();
                for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(500))) {
                    try {
                        batch.add(JsonCodec.decodeDeadLetter(r.value()));
                    } catch (RuntimeException e) {
                        metrics.increment("replay.undecodable");   // raw poison bytes routed by the consumer loop
                    }
                }
                dispatcher.replay(batch);   // skips reason "poison"; reopening twice is harmless
                replayed += batch.size();
                consumer.commitSync();
            }
            System.out.printf("replay: processed %d dead letters%n", replayed);
        }
    }
}
```

- [ ] **Step 11: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.app.ReplayRoleIT' --console=plain`
Expected: PASS, 1 test.

- [ ] **Step 12: Write the failing `Main` test**

`src/test/java/com/quince/cartrecovery/MainTest.java`:

```java
package com.quince.cartrecovery;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.app.Health;
import com.quince.cartrecovery.app.HealthServer;
import com.quince.cartrecovery.app.RoleRegistry;
import com.quince.cartrecovery.core.Metrics;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MainTest {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();
    private PrintStream oldOut;
    private PrintStream oldErr;

    @BeforeEach
    void capture() {
        oldOut = System.out;
        oldErr = System.err;
        System.setOut(new PrintStream(out, true, UTF_8));
        System.setErr(new PrintStream(err, true, UTF_8));
    }

    @AfterEach
    void restore() {
        System.setOut(oldOut);
        System.setErr(oldErr);
    }

    @Test
    void defaultModeRunsTheInMemoryDemo() {
        assertEquals(0, Main.run(new String[0], Map.of()));
        assertFalse(out.toString(UTF_8).isBlank());
    }

    @Test
    void explicitInMemoryModeRunsTheDemo() {
        assertEquals(0, Main.run(new String[] {"--mode=inmemory"}, Map.of()));
    }

    @Test
    void unknownModeExits2() {
        assertEquals(2, Main.run(new String[] {"--mode=cloud"}, Map.of()));
        assertTrue(err.toString(UTF_8).contains("unknown mode"));
    }

    @Test
    void unknownRoleExits2WithoutReadingConfig() {
        assertEquals(2, Main.run(new String[] {"--role=nope"}, Map.of()));
        assertTrue(err.toString(UTF_8).contains("unknown role: nope"));
    }

    @Test
    void badConfigExits2WithOneLineNamingTheVariable() {
        Map<String, String> env = Map.of("KAFKA_BOOTSTRAP", "localhost:9092", "REDIS_URL", "redis://localhost:6379", "SHARDS", "abc");
        assertEquals(2, Main.run(new String[] {"--role=detector"}, env));
        List<String> lines = err.toString(UTF_8).lines().toList();
        assertEquals(1, lines.size(), err.toString(UTF_8));
        assertTrue(lines.get(0).contains("SHARDS"), lines.get(0));
    }

    @Test
    void healthcheckReportsTheServerState() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        Map<String, String> env = Map.of("HEALTH_PORT", Integer.toString(port));
        try (HealthServer server = new HealthServer(port, new Health(), new Metrics(), Duration.ofSeconds(15))) {
            assertEquals(0, Main.run(new String[] {"--role=healthcheck"}, env));
        }
        assertEquals(1, Main.run(new String[] {"--role=healthcheck"}, env));
    }

    @Test
    void registryBuildsEveryRoleByName() {
        for (String name : RoleRegistry.NAMES) {
            assertEquals(name, RoleRegistry.create(name).orElseThrow().name());
        }
    }
}
```

- [ ] **Step 13: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.MainTest' --console=plain`
Expected: FAIL, `cannot find symbol ... RoleRegistry` and `method run(String[], Map<String,String>)`.

- [ ] **Step 14: Implement `RoleRegistry` and the `Main` dispatch**

`src/main/java/com/quince/cartrecovery/app/RoleRegistry.java`:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.loadgen.LoadgenRole;
import java.util.List;
import java.util.Optional;

/** Maps --role names to roles. */
public final class RoleRegistry {
    public static final List<String> NAMES =
        List.of("init", "detector", "scheduler", "dispatcher", "reconciler", "replay", "loadgen");

    private RoleRegistry() {}

    /** Every role has a no-arg constructor; loadgen reads RATE, DURATION and RUN_PREFIX itself (controller ruling R9). */
    public static Optional<Role> create(String name) {
        Role role = switch (name) {
            case "init" -> new InitRole();
            case "detector" -> new DetectorRole();
            case "scheduler" -> new SchedulerRole();
            case "dispatcher" -> new DispatcherRole();
            case "reconciler" -> new ReconcilerRole();
            case "replay" -> new ReplayRole();
            case "loadgen" -> new LoadgenRole();
            default -> null;
        };
        return Optional.ofNullable(role);
    }
}
```

Modify `src/main/java/com/quince/cartrecovery/Main.java`: rename A4's `public static void main(String[] args)` to `static void runDemo()` without changing its body (the demo body does not read `args`), then add the members below and these imports:

```java
import com.quince.cartrecovery.app.ConfigException;
import com.quince.cartrecovery.app.Health;
import com.quince.cartrecovery.app.HealthServer;
import com.quince.cartrecovery.app.InfraConfig;
import com.quince.cartrecovery.app.Role;
import com.quince.cartrecovery.app.RoleRegistry;
import com.quince.cartrecovery.core.Metrics;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
```

(Keep A4's existing imports; `Duration`, `Map`, `List` are already imported by the demo — add any that are missing.)

```java
    static final Duration MAX_SILENCE = Duration.ofSeconds(15);

    /**
     * --mode=inmemory (default): the fake-clock demo. --role=<name>: one infra role until SIGTERM.
     * --role=healthcheck: GET localhost /health, exit 0 when healthy (compose healthcheck without curl).
     */
    public static void main(String[] args) {
        int code = run(args, System.getenv());
        if (code != 0 || option(args, "--role") != null) System.exit(code);
    }

    static int run(String[] args, Map<String, String> env) {
        String role = option(args, "--role");
        String mode = option(args, "--mode");
        if (role == null) {
            if (mode == null || mode.equals("inmemory")) {
                runDemo();
                return 0;
            }
            System.err.println("unknown mode: " + mode + " (expected --mode=inmemory or --role=<name>)");
            return 2;
        }
        if (role.equals("healthcheck")) return healthcheck(env);
        Optional<Role> found = RoleRegistry.create(role);
        if (found.isEmpty()) {
            System.err.println("unknown role: " + role + " (expected one of " + RoleRegistry.NAMES + " or healthcheck)");
            return 2;
        }
        InfraConfig config;
        try {
            config = InfraConfig.fromEnv(env);
        } catch (ConfigException e) {
            System.err.println(e.getMessage());
            return 2;
        }
        return runRole(found.get(), config);
    }

    /** SIGTERM runs the shutdown hook, which interrupts this thread and waits up to 30 s for the role to return (master §1.5). */
    private static int runRole(Role role, InfraConfig config) {
        System.out.printf("role=%s config=%s%n", role.name(), config.hash());
        Health health = new Health();
        Metrics metrics = new Metrics();
        Thread roleThread = Thread.currentThread();
        CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            roleThread.interrupt();
            try {
                done.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));
        try (HealthServer server = new HealthServer(config.healthPort(), health, metrics, MAX_SILENCE)) {
            role.run(config, health, metrics);
            return 0;
        } catch (InterruptedException e) {
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        } finally {
            done.countDown();
        }
    }

    private static int healthcheck(Map<String, String> env) {
        try {
            int port = Integer.parseInt(env.getOrDefault("HEALTH_PORT", "8081"));   // InfraConfig's default
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
            HttpResponse<Void> response = client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/health")).timeout(Duration.ofSeconds(2)).build(),
                HttpResponse.BodyHandlers.discarding());
            return response.statusCode() == 200 ? 0 : 1;
        } catch (Exception e) {
            return 1;
        }
    }

    private static String option(String[] args, String name) {
        for (String a : args) {
            if (a.startsWith(name + "=")) return a.substring(name.length() + 1);
        }
        return null;
    }
```

- [ ] **Step 15: Run it and the full unit suite**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.MainTest' --console=plain`
Expected: PASS, 7 tests.

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL` (the in-memory demo, verifier, and `PipelineTest` unchanged).

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew run --console=plain`
Expected: the in-memory demo output as before C1c, `BUILD SUCCESSFUL`.

- [ ] **Step 16: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/app/ReplayRole.java src/main/java/com/quince/cartrecovery/app/RoleRegistry.java src/main/java/com/quince/cartrecovery/Main.java src/test/java/com/quince/cartrecovery/MainTest.java src/integrationTest/java/com/quince/cartrecovery/app/ReplayRoleIT.java
git commit -m "Add replay role and Main role dispatch with healthcheck

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task C2: End-to-end tests and the Redis-restart contract test

**Model:** opus (real-infra debugging across roles).

**Files:**
- Create: `src/integrationTest/java/com/quince/cartrecovery/e2e/PinningGuard.java`
- Create: `src/integrationTest/resources/junit-platform.properties` (if T0 already created it, add the line below to it instead)
- Create: `src/integrationTest/resources/META-INF/services/org.junit.jupiter.api.extension.Extension`
- Test: `src/integrationTest/java/com/quince/cartrecovery/e2e/PinningGuardSelfIT.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/e2e/EndToEndIT.java`
- Test: `src/integrationTest/java/com/quince/cartrecovery/e2e/RedisRestartIT.java`

**Interfaces:**
- Consumes: all roles (`DetectorRole`, `SchedulerRole`, `DispatcherRole`, `ReconcilerRole`, `ReplayRole`), `ReconcilerRole.EPOCH_KEY`, `RoleContext`, `RoleInfra`, `RoleThread`, `TopicTail`, metrics `reconciler.sweeps`/`reconciler.replays`; B (master §1.6): `JsonCodec.decodeSinkSend/decodeOutcome`, `Topics.SINK_SENDS/OUTCOMES`, `RedisTimerStore`, `RedisMeta.runId`, `RecoveryMetaStore.read`; model `CartEvent`, `CartItem`, `LedgerKey`, `Outcome`, `OutcomeKind`, `Shards`; T0 `Await.until`.
- Produces: `PinningGuard` (JUnit 5 extension, auto-registered for every integration test; `static String drain()`), and the tests. No production code.

- [ ] **Step 1: Write the pinning self-test**

`src/integrationTest/java/com/quince/cartrecovery/e2e/PinningGuardSelfIT.java`:

```java
package com.quince.cartrecovery.e2e;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Proves the JVM flag is on and the guard sees pinning reports. */
class PinningGuardSelfIT {
    @Test
    void detectsAVirtualThreadPinnedByAMonitor() throws Exception {
        Object lock = new Object();
        Thread t = Thread.ofVirtual().start(() -> {
            synchronized (lock) {
                try {
                    Thread.sleep(20);   // parks while holding a monitor: pinned on JDK 21
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        t.join();
        assertTrue(PinningGuard.drain().contains("onPinned"), "no pinning report captured; is -Djdk.tracePinnedThreads=full set?");
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.e2e.PinningGuardSelfIT' --console=plain`
Expected: FAIL, `cannot find symbol ... PinningGuard`.

- [ ] **Step 3: Implement and register `PinningGuard`**

`src/integrationTest/java/com/quince/cartrecovery/e2e/PinningGuard.java`:

```java
package com.quince.cartrecovery.e2e;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.OutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Spec §6.3: integration tests fail on any virtual-thread pinning report. With -Djdk.tracePinnedThreads=full
 * the JDK prints a stack containing "onPinned" to System.out; this tees System.out and fails the test
 * during which such a report appeared. Auto-registered for every integration test class.
 */
public final class PinningGuard implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback {
    private static final Object LOCK = new Object();
    private static StringBuilder captured;

    @Override
    public void beforeAll(ExtensionContext context) {
        assertEquals("full", System.getProperty("jdk.tracePinnedThreads"),
            "integrationTest must run with -Djdk.tracePinnedThreads=full");
        synchronized (LOCK) {
            if (captured != null) return;
            captured = new StringBuilder();
            PrintStream original = System.out;
            System.setOut(new PrintStream(new OutputStream() {
                @Override public void write(int b) {
                    original.write(b);
                    synchronized (LOCK) { captured.append((char) b); }
                }
                @Override public void write(byte[] b, int off, int len) {
                    original.write(b, off, len);
                    synchronized (LOCK) { captured.append(new String(b, off, len, UTF_8)); }
                }
                @Override public void flush() { original.flush(); }
            }, true, UTF_8));
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        drain();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        String out = drain();
        int at = out.indexOf("onPinned");
        if (at >= 0) fail("virtual thread pinned:\n" + out.substring(Math.max(0, at - 300), Math.min(out.length(), at + 3000)));
    }

    /** Returns and clears everything printed since the last drain. */
    static String drain() {
        synchronized (LOCK) {
            if (captured == null) return "";
            String s = captured.toString();
            captured.setLength(0);
            return s;
        }
    }
}
```

`src/integrationTest/resources/junit-platform.properties`:

```properties
junit.jupiter.extensions.autodetection.enabled=true
```

`src/integrationTest/resources/META-INF/services/org.junit.jupiter.api.extension.Extension`:

```
com.quince.cartrecovery.e2e.PinningGuard
```

- [ ] **Step 4: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.e2e.PinningGuardSelfIT' --console=plain`
Expected: PASS, 1 test. T0's `integrationTest` task sets `-Djdk.tracePinnedThreads=full` (controller ruling R10); if the guard still reports the flag missing, stop and report `BLOCKED`, do not edit `build.gradle.kts`.

- [ ] **Step 5: Commit**

```bash
git add src/integrationTest/java/com/quince/cartrecovery/e2e/PinningGuard.java src/integrationTest/java/com/quince/cartrecovery/e2e/PinningGuardSelfIT.java src/integrationTest/resources/junit-platform.properties src/integrationTest/resources/META-INF/services/org.junit.jupiter.api.extension.Extension
git commit -m "Fail integration tests on virtual-thread pinning reports

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: Write the seven end-to-end tests**

The roles already exist, so these tests are expected to pass; a failure is a wiring bug in a role or adapter to be fixed with `superpowers:systematic-debugging`, never by loosening an assertion. Assertions use order and counts only, never timestamps; sleeps only bound negative windows ("nothing was sent before X").

`src/integrationTest/java/com/quince/cartrecovery/e2e/EndToEndIT.java`:

```java
package com.quince.cartrecovery.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.app.DetectorRole;
import com.quince.cartrecovery.app.DispatcherRole;
import com.quince.cartrecovery.app.InfraConfig;
import com.quince.cartrecovery.app.ReconcilerRole;
import com.quince.cartrecovery.app.ReplayRole;
import com.quince.cartrecovery.app.Role;
import com.quince.cartrecovery.app.RoleInfra;
import com.quince.cartrecovery.app.RoleThread;
import com.quince.cartrecovery.app.SchedulerRole;
import com.quince.cartrecovery.app.TopicTail;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Spec §8.3: roles as in-process threads on shared containers, demo-scale timings (offsets +3 s, +6 s, +9 s). */
@Testcontainers(disabledWithoutDocker = true)
class EndToEndIT {
    static final Duration WAIT = Duration.ofSeconds(25);
    static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));
    static final List<OutcomeKind> PRECEDENCE =
        List.of(OutcomeKind.SENT, OutcomeKind.DEAD, OutcomeKind.CANCELLED, OutcomeKind.SKIPPED_LATE);

    static TopicTail sends;
    static TopicTail outcomes;
    private final List<RoleThread> roles = new ArrayList<>();

    @BeforeAll
    static void infra() {
        RoleInfra.start();
        sends = new TopicTail(RoleInfra.bootstrap(), Topics.SINK_SENDS);
        outcomes = new TopicTail(RoleInfra.bootstrap(), Topics.OUTCOMES);
    }

    @AfterAll
    static void closeTails() {
        sends.close();
        outcomes.close();
    }

    @AfterEach
    void stopRoles() {
        List<Throwable> failures = new ArrayList<>();
        for (int i = roles.size() - 1; i >= 0; i--) {
            if (roles.get(i).failure() != null) failures.add(roles.get(i).failure());
            roles.get(i).close();
        }
        roles.clear();
        assertEquals(List.of(), failures, "roles failed while running");
    }

    // 1. Happy path, one cart over 8 partitions: three sends in order; idle partitions do not pause the gate.
    @Test
    void happyPathSendsThreeRemindersInOrder() {
        String cart = RoleInfra.prefix("e2e1") + "cart";
        pipeline(RoleInfra.config(Map.of()));
        edit(cart, 1, Instant.now());
        Await.until(() -> sentKeys(cart).size() >= 3, WAIT);
        assertEquals(List.of(key(cart, 1, 0), key(cart, 1, 1), key(cart, 1, 2)), sentKeys(cart));
        Await.until(() -> resolved(cart).size() == 3, WAIT);
        assertTrue(resolved(cart).values().stream().allMatch(o -> o.kind() == OutcomeKind.SENT));
    }

    // 2. Purchase mid-sequence: no sends after the purchase.
    @Test
    void purchaseMidSequenceStopsTheRemainingReminders() throws Exception {
        String cart = RoleInfra.prefix("e2e2") + "cart";
        pipeline(RoleInfra.config(Map.of()));
        Instant t0 = Instant.now();
        edit(cart, 1, t0);
        Await.until(() -> sentKeys(cart).contains(key(cart, 1, 0)), WAIT);
        RoleInfra.produce(new CartEvent.CartPurchased(cart, "shopper-" + cart, 2, Instant.now()));
        sleepUntil(t0.plusSeconds(12));   // past reminders 1 and 2 (due +6 s, +9 s)
        assertEquals(List.of(key(cart, 1, 0)), sentKeys(cart));
    }

    // 3. Duplicate and out-of-order events: exactly one send per key, all for the newest version.
    @Test
    void duplicateAndOutOfOrderEventsSendEachKeyOnce() throws Exception {
        String cart = RoleInfra.prefix("e2e3") + "cart";
        pipeline(RoleInfra.config(Map.of()));
        Instant t0 = Instant.now();
        edit(cart, 2, t0);
        edit(cart, 1, t0.minusSeconds(1));   // older version arriving later
        edit(cart, 2, t0);                   // duplicate delivery
        Await.until(() -> sentKeys(cart).size() >= 3, WAIT);
        sleepUntil(Instant.now().plusSeconds(2));
        assertEquals(List.of(key(cart, 2, 0), key(cart, 2, 1), key(cart, 2, 2)), sentKeys(cart));
    }

    // 4. Two schedulers and two dispatchers on shared shards: no duplicate sends across 200 carts.
    @Test
    void twoSchedulersAndTwoDispatchersNeverSendAKeyTwice() throws Exception {
        String prefix = RoleInfra.prefix("e2e4");
        InfraConfig c = RoleInfra.config(Map.of());
        start(new DetectorRole(), c);
        start(new SchedulerRole(), c);
        start(new SchedulerRole(), c);
        start(new DispatcherRole(), c);
        start(new DispatcherRole(), c);
        Set<String> expected = new HashSet<>();
        Instant t0 = Instant.now();
        for (int i = 0; i < 200; i++) {
            edit(prefix + i, 1, t0);
            for (int offset = 0; offset < 3; offset++) expected.add(key(prefix + i, 1, offset));
        }
        Await.until(() -> sentKeys(prefix).size() >= 600, WAIT);
        sleepUntil(Instant.now().plusSeconds(2));
        List<String> keys = sentKeys(prefix);
        assertEquals(600, keys.size(), "one send per key");
        assertEquals(expected, new HashSet<>(keys));
    }

    // 5. SEND_FAILURE_RATE=0.3 with 2 attempts: some sends after a retry, some dead-lettered; replay sends each dead key once.
    @Test
    void transientFailuresRetryThenDeadLetterAndReplaySendsEachDeadKeyOnce() throws Exception {
        String prefix = RoleInfra.prefix("e2e5");
        InfraConfig failing = RoleInfra.config(Map.of("SEND_FAILURE_RATE", "0.3", "MAX_SEND_ATTEMPTS", "2"));
        start(new DetectorRole(), failing);
        start(new SchedulerRole(), failing);
        RoleThread failingDispatcher = start(new DispatcherRole(), failing);
        Set<String> expected = new HashSet<>();
        Instant t0 = Instant.now();
        for (int i = 0; i < 40; i++) {
            edit(prefix + i, 1, t0);
            for (int offset = 0; offset < 3; offset++) expected.add(key(prefix + i, 1, offset));
        }
        Await.until(() -> resolved(prefix).keySet().containsAll(expected), WAIT);
        Map<String, Outcome> first = resolved(prefix);
        List<String> dead = first.values().stream().filter(o -> o.kind() == OutcomeKind.DEAD).map(Outcome::key).toList();
        assertFalse(dead.isEmpty(), "some keys are dead-lettered at 30% failure with 2 attempts");
        assertTrue(first.values().stream().anyMatch(o -> o.kind() == OutcomeKind.SENT && o.attempts() >= 2),
            "some sends succeed after a retry");

        stop(failingDispatcher);
        InfraConfig healthy = RoleInfra.config(Map.of("MAX_SEND_ATTEMPTS", "2"));
        start(new DispatcherRole(), healthy);
        RoleThread replay = new RoleThread(new ReplayRole(), healthy);
        assertTrue(replay.join(WAIT), "replay returns when caught up");
        assertNull(replay.failure());

        Await.until(() -> {
            Map<String, Outcome> now = resolved(prefix);
            return dead.stream().allMatch(k -> now.get(k).kind() == OutcomeKind.SENT);
        }, WAIT);
        List<String> keys = sentKeys(prefix);
        for (String k : dead) assertEquals(1, Collections.frequency(keys, k), k);
        assertEquals(keys.size(), new HashSet<>(keys).size(), "no key sent twice");
        assertEquals(expected, new HashSet<>(keys));
    }

    // 6. Redis FLUSHALL mid-sequence: brief pause, the reconciler rebuilds, the remaining sends happen once.
    @Test
    void flushAllMidSequenceIsRebuiltWithoutDuplicates() throws Exception {
        String prefix = RoleInfra.prefix("e2e6");
        InfraConfig c = RoleInfra.config(Map.of());
        pipeline(c);
        start(new ReconcilerRole(), c);
        Set<String> expected = new HashSet<>();
        Instant t0 = Instant.now();
        for (int i = 0; i < 5; i++) {
            edit(prefix + i, 1, t0);
            for (int offset = 0; offset < 3; offset++) expected.add(key(prefix + i, 1, offset));
        }
        Await.until(() -> {
            List<String> keys = sentKeys(prefix);
            for (int i = 0; i < 5; i++) if (!keys.contains(key(prefix + i, 1, 0))) return false;
            return true;
        }, WAIT);
        RoleInfra.ctx().redis().sync().flushall();
        Await.until(() -> sentKeys(prefix).size() >= 15, WAIT);
        sleepUntil(Instant.now().plusSeconds(2));
        List<String> keys = sentKeys(prefix);
        assertEquals(15, keys.size(), "no duplicates after the rebuild");
        assertEquals(expected, new HashSet<>(keys));
    }

    // 7. Detector stopped, purchase produced, reminder due: no send; detector restarted: the reminder is cancelled.
    @Test
    void stoppedDetectorHoldsTheReminderUntilThePurchaseCancelsIt() throws Exception {
        String cart = RoleInfra.prefix("e2e7") + "cart";
        InfraConfig c = RoleInfra.config(Map.of("OFFSETS", "PT6S,PT9S,PT12S"));
        RoleThread detector = start(new DetectorRole(), c);
        start(new SchedulerRole(), c);
        start(new DispatcherRole(), c);
        Instant t0 = Instant.now();
        edit(cart, 1, t0);
        Await.until(() -> abandoned(cart), WAIT);
        stop(detector);
        RoleInfra.produce(new CartEvent.CartPurchased(cart, "shopper-" + cart, 2, Instant.now()));
        sleepUntil(t0.plusSeconds(10));   // reminder 0 due at +6 s: held by the stale watermark
        assertEquals(List.of(), sentKeys(cart));
        start(new DetectorRole(), c);
        Await.until(() -> {
            Outcome o = resolved(cart).get(key(cart, 1, 0));
            return o != null && o.kind() == OutcomeKind.CANCELLED;
        }, WAIT);
        assertEquals(List.of(), sentKeys(cart));
    }

    private RoleThread start(Role role, InfraConfig config) {
        RoleThread t = new RoleThread(role, config);
        roles.add(t);
        return t;
    }

    private void stop(RoleThread role) {
        role.close();
        roles.remove(role);
    }

    private void pipeline(InfraConfig c) {
        start(new DetectorRole(), c);
        start(new SchedulerRole(), c);
        start(new DispatcherRole(), c);
    }

    private static void edit(String cartId, long version, Instant at) {
        RoleInfra.produce(new CartEvent.CartEdited(cartId, "shopper-" + cartId, version, at, ITEMS, "Ada"));
    }

    private static String key(String cartId, long version, int offset) {
        return new LedgerKey(cartId, version, offset).toString();
    }

    /** Keys at the sink in the order sink-sends holds them (per cart, one partition, so in send order). */
    private static List<String> sentKeys(String prefix) {
        return sends.records(prefix).stream().map(r -> JsonCodec.decodeSinkSend(r.value()).key()).toList();
    }

    /** One outcome per key by precedence SENT > DEAD > CANCELLED > SKIPPED_LATE; ABANDONED has no key. */
    private static Map<String, Outcome> resolved(String prefix) {
        Map<String, Outcome> best = new HashMap<>();
        for (var r : outcomes.records(prefix)) {
            Outcome o = JsonCodec.decodeOutcome(r.value());
            if (o.key() == null) continue;
            best.merge(o.key(), o, (a, b) -> PRECEDENCE.indexOf(b.kind()) < PRECEDENCE.indexOf(a.kind()) ? b : a);
        }
        return best;
    }

    private static boolean abandoned(String cartId) {
        return outcomes.records(cartId).stream().map(r -> JsonCodec.decodeOutcome(r.value()))
            .anyMatch(o -> o.kind() == OutcomeKind.ABANDONED && o.cartId().equals(cartId));
    }

    private static void sleepUntil(Instant t) throws InterruptedException {
        long ms = Duration.between(Instant.now(), t).toMillis();
        if (ms > 0) Thread.sleep(ms);
    }
}
```

- [ ] **Step 7: Run the end-to-end tests**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.e2e.EndToEndIT' --console=plain`
Expected: PASS, 7 tests, each under 30 s; no `virtual thread pinned` failure.

- [ ] **Step 8: Commit**

```bash
git add src/integrationTest/java/com/quince/cartrecovery/e2e/EndToEndIT.java
git commit -m "Add the seven infra end-to-end tests

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 9: Write the Redis-restart contract test**

The test uses its own Redis container with a fixed host-port binding, so `docker restart` keeps the address and Lettuce reconnects. It lets the reconciler finish its start-up sweep and store this Redis's identity, drops the detector's timer upsert (the write a crash would lose), restarts Redis with AOF (new `run_id`, epoch preserved), and asserts the failover replay, not a sweep, restored the timer.

`src/integrationTest/java/com/quince/cartrecovery/e2e/RedisRestartIT.java`:

```java
package com.quince.cartrecovery.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.app.DetectorRole;
import com.quince.cartrecovery.app.InfraConfig;
import com.quince.cartrecovery.app.ReconcilerRole;
import com.quince.cartrecovery.app.RoleContext;
import com.quince.cartrecovery.app.RoleInfra;
import com.quince.cartrecovery.app.RoleThread;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.redis.RedisMeta;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.TimerStore;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class RedisRestartIT {
    static final Duration WAIT = Duration.ofSeconds(25);
    static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));
    static int port;
    static GenericContainer<?> redis;

    @BeforeAll
    static void infra() throws IOException {
        RoleInfra.start();
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        redis = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withCommand("redis-server", "--appendonly", "yes")
            .withExposedPorts(6379)
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(6379))));
        redis.start();
    }

    @AfterAll
    static void stopRedis() {
        redis.stop();
    }

    @Test
    void failoverReplayRestoresADroppedDetectorUpsert() {
        InfraConfig c = RoleInfra.config(Map.of(
            "REDIS_URL", "redis://" + redis.getHost() + ":" + port,
            "WINDOW", "PT60S", "OFFSETS", "PT60S,PT120S,PT180S", "RECONCILE_INTERVAL", "PT10M"));
        RecoveryMetaStore meta = new RecoveryMetaStore(RoleInfra.ctx().dynamo());
        try (RoleContext own = new RoleContext(c);
             RoleThread reconciler = new RoleThread(new ReconcilerRole(), c)) {
            RedisCommands<String, String> cmd = own.redis().sync();
            RedisMeta redisMeta = new RedisMeta(own.redis());
            String runIdBefore = redisMeta.runId();
            Await.until(() -> reconciler.metrics().get("reconciler.sweeps") >= 1
                && runIdBefore.equals(meta.read().redisRunId()), WAIT);

            TimerStore timers = new RedisTimerStore(own.redis(), c.shards(), c.dispatch().lease());
            String cart = RoleInfra.prefix("restart") + "cart";
            int shard = Shards.of(cart, c.shards());
            try (RoleThread detector = new RoleThread(new DetectorRole(), c)) {
                RoleInfra.produce(new CartEvent.CartEdited(cart, "shopper", 1, Instant.now(), ITEMS, "Ada"));
                Await.until(() -> timers.existing(shard, List.of(cart)).contains(cart), WAIT);
            }
            timers.remove(cart, 1);   // the detector upsert that the restart "dropped"
            assertFalse(timers.existing(shard, List.of(cart)).contains(cart));
            long sweeps = reconciler.metrics().get("reconciler.sweeps");
            long replays = reconciler.metrics().get("reconciler.replays");

            DockerClientFactory.instance().client().restartContainerCmd(redis.getContainerId()).exec();

            Await.until(() -> reconciler.metrics().get("reconciler.replays") > replays, WAIT);
            assertTrue(timers.existing(shard, List.of(cart)).contains(cart), "failover replay restores the timer");
            assertEquals(sweeps, reconciler.metrics().get("reconciler.sweeps"), "no key sweep ran: the epoch survived via AOF");
            assertNotNull(cmd.get(ReconcilerRole.EPOCH_KEY));
            assertFalse(runIdBefore.equals(redisMeta.runId()), "run_id changed on restart");
            assertNull(reconciler.failure());
        }
    }
}
```

- [ ] **Step 10: Run it**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew integrationTest --tests 'com.quince.cartrecovery.e2e.RedisRestartIT' --console=plain`
Expected: PASS, 1 test.

- [ ] **Step 11: Run everything**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew check --console=plain`
Expected: `BUILD SUCCESSFUL`; unit and integration suites green, no pinning failures.

- [ ] **Step 12: Commit**

```bash
git add src/integrationTest/java/com/quince/cartrecovery/e2e/RedisRestartIT.java
git commit -m "Add Redis-restart failover replay contract test

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

## Adapter APIs and contract issues

All resolved by controller rulings (master plan):

- Thread B adapter APIs (constructors, `JsonCodec.encode/decode*`, `Topics`, `TopicAdmin.createAll/partitionCounts`, `DynamoTables`, `RecoveryMetaStore`, `RedisMeta`): resolved by controller rulings R6 and R7; the reconciled list is master §1.6 and this thread calls exactly those members.
- Stop signal (`Role.run` returns when its thread is interrupted; `Main` waits up to 30 s): resolved by controller ruling R8 (master §1.5).
- `Hooks.afterCommit` coverage: C0b calls it with the committed offsets after every commit (including the revoke commit) and once per iteration that committed nothing, with an empty map; `DetectorWatermarkHooks` merges the map and seeds idle partitions from `consumer.committed(...)`.
- `loadgen` arguments: resolved by controller ruling R9 (`new LoadgenRole()`, env `RATE`, `DURATION`, `RUN_PREFIX`).
- `-Djdk.tracePinnedThreads=full`: resolved by controller ruling R10 (T0's `build.gradle.kts`). `PinningGuard` assumes JDK 21 (from JDK 24 `synchronized` no longer pins).
- `Health` has no reader for ready values, so tests read `/ready` over HTTP (`DispatcherRoleIT`) and use role metrics (`reconciler.sweeps`, `reconciler.replays`) as completion signals. Accepted as written.
- `recovery-meta` store API: resolved by controller ruling R6 (`RecoveryMetaStore` in B1; `read()` throws `IllegalStateException` when init has not run, which `RoleContext.verifyStartup` turns into "run --role=init first").
- `HEALTH_PORT` default 8081 and `MAX_SEND_RATE` default 1000: controller ruling R14.

## Self-review

- **Spec coverage:** §5.4 write rules → C1a hooks and tests. §5.4 dispatcher gate and pausing, §6.3 lanes/budget/breaker/guardrail → C1b. §6.2 retry loop from a random shard → C1b `controlLoop`. §6.2 reconciler sweep, epoch, sweep duration on `/ready`, failover replay with an earliest-change restart → C1c. §6.2 replay → C1c `ReplayRole`. §4 init, §7.1 startup refusal and config hash → C1c `InitRole`, C1a `verifyStartup`, `Main.runRole`. §6.3 failure classification → C1a `Failures` and the handlers. §6.3 client sizing → `RoleContext.dynamo`. §6.3 attempt-count log → C1b `logAttempts`. §7.4 scheduler idles 200 ms, detector poll 500 ms, `/health` beats → C1a. §8.3 tests 1 to 7 and the §8.4 Redis-restart contract test → C2. The §7.4 stuck-partition signal and the 10 s metrics log belong to C0b and C0a.
- **Placeholder scan:** every code step has complete code. The one conditional instruction (C2 `junit-platform.properties` "if T0 already created it") names the exact line to add.
- **Type consistency:** `RoleContext.startupProblem(int, int, Integer, Integer, Map)` is used the same way in C1a and C1c. `ReconcilerRole.EPOCH_KEY` is B2's `RedisMeta.EPOCH`; C1a's `RoleInfra` writes it with `RedisMeta.writeEpoch()`. Every thread-B member used here is listed in master §1.6. Metric and ready-key names match between the roles and the tests that read them.
