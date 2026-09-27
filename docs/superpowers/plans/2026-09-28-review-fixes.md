# Review Fixes 1–3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the three defects the principal-architect review found: the watermark collapses into a halt under detector lag, superseded and reconciler-skipped reminders leave no outcome, and the dispatcher burns send tokens it never uses and cannot know a retry is late before taking one.

**Architecture:** The detector keeps a dense plus sparse snapshot history and always publishes its satisfied time, and the scheduler caps its hold from configuration. The timer store returns the timer each write displaced, so the detector can record `SUPERSEDED`, and the reconciler records `SKIPPED_LATE` for offsets it skips. The dispatcher returns every token it did not send with, the retry index carries `sendBy` so a late retry is settled without a token, and the retry pass runs all shards concurrently. The loadgen resolves its accounting from outcomes and keeps the old script inference as a cross-check.

**Tech Stack:** Java 21 (virtual threads), Gradle wrapper, JUnit 5, Lettuce (Redis Lua), AWS SDK v2 DynamoDB, kafka-clients, Testcontainers 1.21.4, Docker Compose.

**Spec:** docs/superpowers/specs/2026-09-28-review-fixes-design.md

## Global Constraints

- Java 21. Run every Gradle command with `export JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current` from `/Users/vikas/Work/Interviews/quince`.
- `./gradlew test` needs only a JDK and stays green after every task.
- Infra tests live in `src/integrationTest` with `@Testcontainers(disabledWithoutDocker = true)` (Testcontainers BOM 1.21.4), and `./gradlew integrationTest` needs Docker.
- Every commit ends with the trailer `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- `core` depends only on `model` and `ports`, and nothing in `core` imports a client library.
- The in-memory mode keeps working: `./gradlew run`, `Pipeline`, `FakeClockVerifierTest`, `PipelineTest`.
- These frozen-contract changes are allowed, and only these. Each task that makes one also updates `docs/superpowers/plans/2026-09-26-production-infra.md` §1.1 to §1.3 in the same commit.
  - `TimerStore.upsert` returns `TimerStore.Upsert`, and `record Upsert(boolean written, Optional<Timer> displaced)` is nested in `TimerStore`.
  - `TimerStore.remove` returns `Optional<Timer>`.
  - `OutcomeKind` gains `SUPERSEDED`.
  - `SendBudget` gains `void release(Lane lane)`.
  - `DueRetry(String key, int srcPartition, Instant sendBy)`.
  - `AbandonmentDetector(RecoveryConfig, CartStateStore, TimerStore, ArmAssigner, OutcomeRecorder, Metrics)`.
  - `Reconciler(RecoveryConfig, CartStateStore, TimerStore, SendLedger, OutcomeRecorder, Clock, Metrics)`.
- Snapshot history: `history = max(latenessBounds) + CLOCK_SKEW`. That is 30 min 5 s at the defaults and 35 s with `demo.env`.
  - The dense ring holds the latest 8 snapshots, one per 250 ms attempt.
  - The sparse ring holds the first snapshot of each Redis-time second.
  - Both rings evict snapshots older than `newest − history`.
  - Nothing ages out while no new snapshot arrives.
- Scheduler hold cap: `maxHold = max(1 s, min(60 s, smallest lateness bound ÷ 4))`. `EPOCH` also holds `maxHold`. That is 60 s at the defaults and 5 s with `demo.env`.
- Outcome precedence: `SENT > DEAD > CANCELLED > SKIPPED_LATE > SUPERSEDED`.
- Detector `SUPERSEDED` outcome: `Outcome(cartId:v:j, cartId, v, TREATMENT, SUPERSEDED, at = event.occurredAt, attempts = 0)` for offsets with `dueAt_j ≤ occurredAt` (inclusive).
  - For a displaced `REMINDER(v, i)`: `j ≥ i` and `dueAt_j = dueAt_i − offset_i + offset_j`.
  - For a displaced `CHECK_ABANDON(v)`: `j ≥ 0` and `dueAt_j = checkDue − window + offset_j`. It records only if the cart, read before `applyEvent`, is `ACTIVE` at version `v` and `ReminderPolicy.eligible(cart.abandoned(), checkDue)` holds.
- Reconciler `SKIPPED_LATE`: `Outcome(cartId:v:j, cartId, v, TREATMENT, SKIPPED_LATE, at = clock.now(), 0)` for each skipped offset. It records only when the rebuild's upsert wrote or its `endSequence` succeeded.
- Dispatcher order in `handle` is unchanged: `sendBy` check, token, gate, claim.
  - The token is returned on the gate hold, on `NotClaimed`, on a cancel or late skip after the cart re-read, and on a lease too short for the gateway timeout.
  - An exception keeps the token, so a refund never follows a send.
- Retry: a row with `now > sendBy` is claimed and finished `SKIPPED_LATE` with no token and no gate. An on-time row goes gate, then token, then claim, with the same refunds. The `GONE` sentinel is deleted.
- The retry pass runs `retryDue` for every shard at once, one virtual thread per shard, and waits for all of them before the next `RETRY_POLL`.
- No migration code is written. A GSI projection change needs the local tables recreated with `docker compose --profile load down -v`.
- Load re-run settings: `RATE=250`, `DURATION=PT5M`, `COMPOSE_ENV_FILES=demo.env`, `DYNAMO_STORAGE=-inMemory`.

## Review Focus

1. Redis's "nothing displaced" reply may come back from Lettuce as an empty string or as null, and a displaced packed value can belong to a cart id containing `:` or `|`. Both must round-trip to `Optional.empty()` and to the exact timer. Pinned in Task 3: `TimerStoreContract.upsertReportsTheTimerItDisplaced` (cart `x:y|z`, both adapters) and `LuaScriptsTest.upsertReturnsThePreviousPackedValueOnlyWhenItOverwrites`.
2. Inclusive time boundaries. A reminder due exactly at the superseding event is recorded, and a `SUPERSEDED` at exactly `sendBy + shift` counts as before sendBy. Pinned in Task 4 (`aReminderDueExactlyAtTheEventIsSuperseded`) and Task 6 (`supersededExactlyAtTheShiftedSendByCountsAsBefore`).
3. Redelivered events must not record twice: a duplicate resume is a no-op upsert, and a duplicate purchase removes a timer that is already gone. Pinned in Task 4: `aRedeliveredEventRecordsNothingNew` and `aRedeliveredPurchaseRecordsNothingNew`.
4. A reconciler rebuild whose upsert does not write, because another writer rebuilt the timer first, must record no `SKIPPED_LATE`. Pinned in Task 5: `aRebuildWhoseUpsertDoesNotWriteRecordsNothing`.
5. Token edges: a refund into a full bucket stays at capacity, and a failure after the sink call keeps the token. Pinned in Task 7: `TokenBucketTest.releaseNeverRaisesTheBucketAboveCapacity`, `DispatcherTest.aFailedSendKeepsItsToken`, and the added assertion in `aCrashWhileProducingTheSentOutcomeLeavesTheRowForAResendUnderTheSameKey`.

## Dependencies and parallelism

Each task runs in its own git worktree, and waves merge in order.

| Wave | Tasks (concurrent) | Why this order |
|---|---|---|
| 1 | 1, 2, 3, 6, 7 | Disjoint files. Tasks 3 and 7 both edit master plan §1.2, but at non-adjacent lines (the `TimerStore` block and the `SendBudget` line). |
| 2 | 4, 8 | Task 4 needs 3 (the `Upsert` API) and 1 (it edits `DetectorRole` after 1 does), and it follows 7 because both edit `Pipeline` lines 77 to 81, which are adjacent. Task 8 needs 7 (`Dispatcher` and `DispatcherTest`). Tasks 4 and 8 touch §1.1 at non-adjacent lines. |
| 3 | 5, 9 | Task 5 needs 3 (`upsert(...).written()`) and follows 4 (both edit `Pipeline`). Task 9 needs 7 (the `dispatch.token_*` metrics, `DispatcherRole`) and 8 (`sendBy` in the retry index). |
| 4 | 10 | Needs everything: full verification, load re-run, docs. |

Run serially, tasks 1 to 10 in order also work: every edit below is written as an exact old-to-new replacement that holds whichever of its neighbours has already landed.

---

## Task 1: Watermark snapshot history and always-publish

**Files:**
- Modify: `src/main/java/com/quince/cartrecovery/app/WatermarkSnapshots.java` (whole file, 1–51)
- Modify: `src/main/java/com/quince/cartrecovery/app/DetectorWatermarkHooks.java` (whole file, 1–103)
- Modify: `src/main/java/com/quince/cartrecovery/app/DetectorRole.java:33-34`
- Test: `src/test/java/com/quince/cartrecovery/app/WatermarkSnapshotsTest.java` (whole file)
- Test: `src/test/java/com/quince/cartrecovery/app/DetectorWatermarkHooksTest.java` (whole file)

**Interfaces:**
- Consumes: `RecoveryConfig.latenessBounds()`, `DispatchConfig.clockSkew()`, `Watermark.now()`, `Watermark.publish(int, long, Instant)`.
- Produces:
  - `WatermarkSnapshots(int dense, Duration history)`
  - `void add(Instant time, Map<TopicPartition, Long> ends)`
  - `Optional<Instant> satisfied(TopicPartition p, long committed)`
  - `Optional<Instant> newest()`
  - `DetectorWatermarkHooks(Watermark, Health, Metrics, LongSupplier nanoTime, Duration history)`
  - `static Duration DetectorWatermarkHooks.history(RecoveryConfig recovery, Duration clockSkew)`

- [ ] **Step 1: Write the failing snapshot tests.** Replace `WatermarkSnapshotsTest.java` with:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class WatermarkSnapshotsTest {
    static final TopicPartition P0 = new TopicPartition("cart-events", 0);
    static final TopicPartition P1 = new TopicPartition("cart-events", 1);
    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    static final Duration HISTORY = Duration.ofSeconds(35);

    static Instant at(long millis) { return T0.plusMillis(millis); }

    static WatermarkSnapshots snapshots() { return new WatermarkSnapshots(8, HISTORY); }

    /** Twenty attempts 250 ms apart, end offset k at attempt k: dense keeps k = 12..19, sparse keeps k = 0, 4, 8, 12, 16. */
    static WatermarkSnapshots fiveSecondsOfAttempts() {
        WatermarkSnapshots s = snapshots();
        for (int k = 0; k < 20; k++) s.add(at(k * 250L), Map.of(P0, (long) k));
        return s;
    }

    @Test
    void noSnapshotSatisfiesNothing() {
        assertEquals(Optional.empty(), snapshots().satisfied(P0, 100));
    }

    @Test
    void committedBelowEndIsNotSatisfied() {
        WatermarkSnapshots s = snapshots();
        s.add(at(0), Map.of(P0, 10L));
        assertEquals(Optional.empty(), s.satisfied(P0, 9));
    }

    @Test
    void committedAtEndIsSatisfied() {
        WatermarkSnapshots s = snapshots();
        s.add(at(0), Map.of(P0, 10L));
        assertEquals(Optional.of(at(0)), s.satisfied(P0, 10));
    }

    @Test
    void emptyPartitionIsSatisfiedAtOnce() {
        WatermarkSnapshots s = snapshots();
        s.add(at(0), Map.of(P0, 0L));
        assertEquals(Optional.of(at(0)), s.satisfied(P0, 0));
    }

    @Test
    void newestSatisfiedSnapshotWins() {
        WatermarkSnapshots s = snapshots();
        s.add(at(0), Map.of(P0, 5L));
        s.add(at(250), Map.of(P0, 10L));
        s.add(at(500), Map.of(P0, 15L));
        assertEquals(Optional.of(at(250)), s.satisfied(P0, 12));
        assertEquals(Optional.of(at(500)), s.satisfied(P0, 15));
        assertEquals(Optional.empty(), s.satisfied(P0, 4));
    }

    @Test
    void snapshotWithoutThePartitionIsIgnored() {
        WatermarkSnapshots s = snapshots();
        s.add(at(0), Map.of(P0, 0L));
        assertEquals(Optional.empty(), s.satisfied(P1, 100));
    }

    @Test
    void newestIsTheLastAdded() {
        WatermarkSnapshots s = snapshots();
        assertEquals(Optional.empty(), s.newest());
        s.add(at(0), Map.of(P0, 5L));
        s.add(at(250), Map.of(P0, 10L));
        assertEquals(Optional.of(at(250)), s.newest());
    }

    @Test
    void aBehindPartitionIsSatisfiedByASparseSnapshotOlderThan2s() {
        assertEquals(Optional.of(at(0)), fiveSecondsOfAttempts().satisfied(P0, 0));
    }

    @Test
    void theSparseRingKeepsTheFirstSnapshotOfEachSecond() {
        WatermarkSnapshots s = fiveSecondsOfAttempts();
        assertEquals(Optional.of(at(0)), s.satisfied(P0, 3));       // k = 3 is not first in second 0; k = 0 is
        assertEquals(Optional.of(at(1000)), s.satisfied(P0, 5));    // second 1 starts at k = 4
        assertEquals(Optional.of(at(2000)), s.satisfied(P0, 11));   // second 2 starts at k = 8; dense starts at 12
    }

    @Test
    void theDenseRingKeepsTheLatestEightAttempts() {
        WatermarkSnapshots s = fiveSecondsOfAttempts();
        assertEquals(Optional.of(at(19 * 250)), s.satisfied(P0, 19));
        assertEquals(Optional.of(at(13 * 250)), s.satisfied(P0, 13));   // not a sparse entry: only dense has it
    }

    @Test
    void snapshotsOlderThanTheHistoryAreEvictedFromBothRings() {
        WatermarkSnapshots s = snapshots();
        s.add(at(0), Map.of(P0, 0L));
        s.add(at(35_000), Map.of(P0, 10L));
        assertEquals(Optional.of(at(0)), s.satisfied(P0, 0), "exactly the history back: kept");
        s.add(at(35_001), Map.of(P0, 10L));
        assertEquals(Optional.empty(), s.satisfied(P0, 0), "older than the history: evicted");
    }

    @Test
    void rejectsAnEmptyDenseRing() {
        assertThrows(IllegalArgumentException.class, () -> new WatermarkSnapshots(0, HISTORY));
    }
}
```

- [ ] **Step 2: Write the failing hooks tests.** Replace `DetectorWatermarkHooksTest.java` with:

```java
package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.model.RecoveryConfig;
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
    static final Duration HISTORY = Duration.ofSeconds(35);

    final List<String> calls = new ArrayList<>();
    final RecordingWatermark watermark = new RecordingWatermark();
    final FakeConsumer broker = new FakeConsumer();
    final Metrics metrics = new Metrics();
    final long[] nanos = {0};
    final Health health = new Health();
    final DetectorWatermarkHooks hooks = new DetectorWatermarkHooks(watermark, health, metrics, () -> nanos[0], HISTORY);

    /** Moves nanoTime and Redis TIME forward together, then runs one iteration's beforePoll. */
    void tick(Duration d) {
        nanos[0] += d.toNanos();
        watermark.now = watermark.now.plus(d);
        hooks.beforePoll(broker.proxy());
    }

    String lag() { return health.readiness().get("watermark.lag_ms.p0"); }

    @Test
    void historyIsTheLargestLatenessBoundPlusClockSkew() {
        assertEquals(Duration.ofMinutes(30).plusSeconds(5),
            DetectorWatermarkHooks.history(RecoveryConfig.defaults(), Duration.ofSeconds(5)));
        RecoveryConfig demo = RecoveryConfig.defaults()
            .withLatenessBounds(List.of(Duration.ofSeconds(20), Duration.ofSeconds(20), Duration.ofSeconds(30)));
        assertEquals(Duration.ofSeconds(35), DetectorWatermarkHooks.history(demo, Duration.ofSeconds(5)));
    }

    @Test
    void readyShowsStaleUntilASnapshotIsSatisfiedThenTheLagBehindRedisTime() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 5L);
        broker.committed.put(P0, 3L);
        hooks.beforePoll(broker.proxy());                  // (T1, 5), committed 3
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals("stale", lag());

        hooks.afterCommit(broker.proxy(), Map.of(P0, 5L), 1);
        assertEquals("0", lag());

        broker.ends.put(P0, 10L);
        tick(Duration.ofSeconds(1));                       // (T2, 10): one second behind
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals("1000", lag());
        assertEquals(List.of("0|1|" + T1, "0|1|" + T1), watermark.published);
    }

    @Test
    void aPartitionBehindTheWholeHistoryGoesStale() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 5L);
        broker.committed.put(P0, 5L);
        hooks.beforePoll(broker.proxy());
        hooks.afterCommit(broker.proxy(), Map.of(), 1);   // publishes T1
        broker.ends.put(P0, 10L);
        for (int i = 0; i < 30; i++) tick(Duration.ofSeconds(1));
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals("30000", lag(), "30 s behind: still inside the 35 s history");

        for (int i = 0; i < 10; i++) tick(Duration.ofSeconds(1));
        hooks.afterCommit(broker.proxy(), Map.of(), 1);
        assertEquals("stale", lag(), "40 s behind: T1 evicted");
        assertEquals(2, watermark.published.size());
    }

    @Test
    void idlePartitionPublishesAtOnceEvenWithoutANewCommit() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 5L);
        broker.committed.put(P0, 5L);
        hooks.beforePoll(broker.proxy());
        hooks.afterCommit(broker.proxy(), Map.of(), 3);
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
    void aCutOffDetectorKeepsRepublishingItsLastSatisfiedTimeAndItsLagGrows() {
        broker.assignment = Set.of(P0);
        broker.ends.put(P0, 5L);
        broker.committed.put(P0, 5L);
        hooks.beforePoll(broker.proxy());                  // (T1, 5)
        broker.endOffsetsFailure = new TimeoutException("broker unreachable");
        for (int i = 0; i < 12; i++) {                     // 3 s of failing snapshots, 250 ms apart
            tick(Duration.ofMillis(250));
            hooks.afterCommit(broker.proxy(), Map.of(), 1);
        }
        assertEquals(12, watermark.published.size());
        assertEquals(List.of("0|1|" + T1), watermark.published.stream().distinct().toList());
        assertEquals("3000", lag());
        assertEquals(12, metrics.get("watermark.snapshot_failed"));
    }

    @Test
    void aBehindPartitionPublishesAnOldTimeRatherThanNothing() {
        broker.assignment = Set.of(P0);
        broker.committed.put(P0, 0L);
        broker.ends.put(P0, 5L);
        hooks.beforePoll(broker.proxy());                  // (T1, 5)
        broker.ends.put(P0, 10L);
        tick(Duration.ofSeconds(10));                      // (T1 + 10 s, 10)
        hooks.afterCommit(broker.proxy(), Map.of(P0, 7L), 1);
        assertEquals(List.of("0|1|" + T1), watermark.published);
        assertEquals("10000", lag());
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

- [ ] **Step 3: Run the tests and watch them fail.** Run `./gradlew test --tests 'com.quince.cartrecovery.app.WatermarkSnapshotsTest' --tests 'com.quince.cartrecovery.app.DetectorWatermarkHooksTest'`. Expected: FAIL at `compileTestJava`. The constructors `WatermarkSnapshots(int,Duration)` and `DetectorWatermarkHooks(...,Duration)`, the method `satisfied(TopicPartition,long)` and the method `history(...)` do not match the current code.

- [ ] **Step 4: Implement `WatermarkSnapshots`.** Replace the file with:

```java
package com.quince.cartrecovery.app;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;

/**
 * The detector's end-offset snapshots (spec §5.4, review fix 1). A snapshot (T, E) says every record appended to
 * partition p before Redis time T lies below offset E[p]; once the committed position reaches E[p], the watermark
 * for p may be set to T. Two rings, newest first: dense holds the latest {@code dense} snapshots (one per 250 ms
 * attempt), and sparse holds the first snapshot of each Redis-time second. Both drop snapshots older than
 * {@code history} before the newest. Nothing ages out while no snapshot arrives, so a detector cut off from the
 * broker keeps satisfying from what it has. Poll thread only.
 */
final class WatermarkSnapshots {
    private record Snapshot(Instant time, Map<TopicPartition, Long> ends) {}

    private final int dense;
    private final Duration history;
    private final Deque<Snapshot> denseRing = new ArrayDeque<>();
    private final Deque<Snapshot> sparseRing = new ArrayDeque<>();

    WatermarkSnapshots(int dense, Duration history) {
        if (dense < 1) throw new IllegalArgumentException("dense must be >= 1");
        this.dense = dense;
        this.history = history;
    }

    void add(Instant time, Map<TopicPartition, Long> ends) {
        Snapshot s = new Snapshot(time, Map.copyOf(ends));
        denseRing.addFirst(s);
        while (denseRing.size() > dense) denseRing.removeLast();
        Snapshot newestSparse = sparseRing.peekFirst();
        if (newestSparse == null || time.getEpochSecond() > newestSparse.time().getEpochSecond()) sparseRing.addFirst(s);
        Instant horizon = time.minus(history);
        for (Deque<Snapshot> ring : List.of(denseRing, sparseRing)) {
            while (!ring.isEmpty() && ring.peekLast().time().isBefore(horizon)) ring.removeLast();
        }
    }

    /** T of the newest retained snapshot, dense first then sparse, whose end offset for p is at or below committed. */
    Optional<Instant> satisfied(TopicPartition p, long committed) {
        for (Deque<Snapshot> ring : List.of(denseRing, sparseRing)) {
            for (Snapshot s : ring) {
                Long end = s.ends().get(p);
                if (end != null && committed >= end) return Optional.of(s.time());
            }
        }
        return Optional.empty();
    }

    Optional<Instant> newest() {
        Snapshot s = denseRing.peekFirst();
        return s == null ? Optional.empty() : Optional.of(s.time());
    }
}
```

- [ ] **Step 5: Implement `DetectorWatermarkHooks`.** Replace the file with:

```java
package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
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
 * Spec §5.4 watermark writes, with review fix 1. beforePoll: at most every 250 ms, read Redis TIME as T and then the
 * end offsets E of the assigned partitions (a failed call takes no snapshot). afterCommit, every iteration including
 * empty polls and backoff: for each assigned partition publish T of the newest retained snapshot whose E[p] is at or
 * below the committed position, however old, so a lagging detector reports "behind by X" instead of going stale.
 * A detector cut off from the broker keeps republishing its last satisfied time, frozen: both gates hold as they would
 * on a stale entry, and the lag stays visible (a deliberate deviation from spec §5.4's "goes stale"). Only a partition
 * behind every retained snapshot, by then past every lateness bound, writes nothing; /ready then shows
 * {@code watermark.lag_ms.p<n>: stale}. The reported lag is the latest Redis TIME read minus the published time.
 * Poll thread only.
 */
final class DetectorWatermarkHooks implements BatchConsumerLoop.Hooks<byte[]> {
    static final Duration SNAPSHOT_INTERVAL = Duration.ofMillis(250);
    static final Duration BROKER_TIMEOUT = Duration.ofSeconds(1);
    static final int KEEP = 8;

    private final Watermark watermark;
    private final Health health;
    private final Metrics metrics;
    private final LongSupplier nanoTime;
    private final WatermarkSnapshots snapshots;
    private final Map<TopicPartition, Long> committed = new HashMap<>();
    private boolean attempted;
    private long lastAttemptNanos;
    private Instant redisNow;

    DetectorWatermarkHooks(Watermark watermark, Health health, Metrics metrics, LongSupplier nanoTime, Duration history) {
        this.watermark = watermark;
        this.health = health;
        this.metrics = metrics;
        this.nanoTime = nanoTime;
        this.snapshots = new WatermarkSnapshots(KEEP, history);
    }

    /** How far back snapshots are kept: max(latenessBounds) + CLOCK_SKEW. A partition further behind is past every bound. */
    static Duration history(RecoveryConfig recovery, Duration clockSkew) {
        return recovery.latenessBounds().stream().max(Comparator.naturalOrder()).orElseThrow().plus(clockSkew);
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
            redisNow = t;
            snapshots.add(t, consumer.endOffsets(assigned, BROKER_TIMEOUT));
        } catch (RuntimeException e) {
            metrics.increment("watermark.snapshot_failed");   // no snapshot; the retained ones stay valid
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
            Optional<Instant> t = position == null ? Optional.empty() : snapshots.satisfied(p, position);
            if (t.isEmpty()) {   // behind every retained snapshot: write nothing, the entry goes stale after 5 s
                health.setReady("watermark.lag_ms.p" + p.partition(), "stale");
                continue;
            }
            try {
                watermark.publish(p.partition(), generation, t.get());
                long lagMs = Math.max(0, Duration.between(t.get(), redisNow).toMillis());
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

- [ ] **Step 6: Wire the history in `DetectorRole`.** In `DetectorRole.java` replace:

```java
            DetectorWatermarkHooks hooks = new DetectorWatermarkHooks(
                new RedisWatermark(ctx.redis(), config.partitions()), health, metrics, System::nanoTime);
```

with:

```java
            DetectorWatermarkHooks hooks = new DetectorWatermarkHooks(
                new RedisWatermark(ctx.redis(), config.partitions()), health, metrics, System::nanoTime,
                DetectorWatermarkHooks.history(config.recovery(), config.dispatch().clockSkew()));
```

- [ ] **Step 7: Run the tests and watch them pass.** Run the command from Step 3. Expected: PASS. Then run `./gradlew test`. Expected: BUILD SUCCESSFUL.

- [ ] **Step 8: Commit.**

```bash
git add src/main/java/com/quince/cartrecovery/app/WatermarkSnapshots.java src/main/java/com/quince/cartrecovery/app/DetectorWatermarkHooks.java src/main/java/com/quince/cartrecovery/app/DetectorRole.java src/test/java/com/quince/cartrecovery/app/WatermarkSnapshotsTest.java src/test/java/com/quince/cartrecovery/app/DetectorWatermarkHooksTest.java
git commit -m "$(cat <<'EOF'
Keep a dense and sparse watermark history and always publish the satisfied time

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Scheduler hold cap from configuration

**Files:**
- Modify: `src/main/java/com/quince/cartrecovery/core/ReminderScheduler.java:19-22` (imports), `:31-32` (constants), `:35-56` (field and constructor), `:70` (call site), `:147-153` (`holdFor`)
- Test: `src/test/java/com/quince/cartrecovery/core/ReminderSchedulerTest.java` (add two tests)

**Interfaces:**
- Consumes: `RecoveryConfig.latenessBounds()`.
- Produces: `static Duration ReminderScheduler.maxHold(RecoveryConfig)` and `static Duration ReminderScheduler.holdFor(Instant w, Instant needed, Duration maxHold)` (package-private). The constructor signature is unchanged.

- [ ] **Step 1: Write the failing tests.** In `ReminderSchedulerTest.java`, add these members before the closing brace:

```java
    private static RecoveryConfig demoBounds() {
        return RecoveryConfig.defaults()
            .withLatenessBounds(List.of(Duration.ofSeconds(20), Duration.ofSeconds(20), Duration.ofSeconds(30)));
    }

    @Test
    void theHoldCapIsAQuarterOfTheSmallestLatenessBoundBetweenOneAndSixtySeconds() {
        assertEquals(Duration.ofSeconds(60), ReminderScheduler.maxHold(RecoveryConfig.defaults()));
        assertEquals(Duration.ofSeconds(5), ReminderScheduler.maxHold(demoBounds()));
        assertEquals(Duration.ofSeconds(1), ReminderScheduler.maxHold(RecoveryConfig.defaults()
            .withLatenessBounds(List.of(Duration.ZERO, Duration.ZERO, Duration.ZERO))));
    }

    @Test
    void atDemoBoundsAStaleOrFarBehindWatermarkHoldsFiveSeconds() {
        ReminderScheduler demo = scheduler(demoBounds(), store);
        active(1, T0, Arm.TREATMENT, 0);
        Instant needed = at(min(30)).plusSeconds(5);
        clock.set(needed);

        assertEquals(new TimerDecision.Release(Duration.ofSeconds(5)), demo.onTimer(check(1)));   // EPOCH
        watermark.publish(0, 1, needed.minusSeconds(20));
        assertEquals(new TimerDecision.Release(Duration.ofSeconds(5)), demo.onTimer(check(1)));
        watermark.publish(0, 1, needed.minusSeconds(3));
        assertEquals(new TimerDecision.Release(Duration.ofSeconds(3)), demo.onTimer(check(1)));
    }
```

- [ ] **Step 2: Run the tests and watch them fail.** Run `./gradlew test --tests 'com.quince.cartrecovery.core.ReminderSchedulerTest'`. Expected: FAIL at `compileTestJava` with "cannot find symbol: method maxHold(RecoveryConfig)".

- [ ] **Step 3: Implement the cap.** In `ReminderScheduler.java`:

  1. Add `import java.util.Comparator;` after `import java.time.Instant;`.
  2. Replace `    static final Duration MAX_HOLD = Duration.ofSeconds(60);` with `    static final Duration HOLD_CAP = Duration.ofSeconds(60);`.
  3. After `    private final Metrics metrics;` (the field), add `    private final Duration maxHold;`.
  4. In the constructor, after `        this.metrics = metrics;`, add `        this.maxHold = maxHold(config);`.
  5. Replace `            return new TimerDecision.Release(holdFor(w, needed));` with `            return new TimerDecision.Release(holdFor(w, needed, maxHold));`.
  6. Replace the whole `holdFor` method and its Javadoc (lines 147–153) with:

```java
    /**
     * max(1 s, min(60 s, smallest lateness bound ÷ 4)), review fix 1: one hold can never outlast a lateness bound.
     * 60 s at production bounds (5 min), 5 s with demo.env (20 s).
     */
    static Duration maxHold(RecoveryConfig config) {
        Duration quarter = config.latenessBounds().stream().min(Comparator.naturalOrder()).orElseThrow().dividedBy(4);
        Duration capped = quarter.compareTo(HOLD_CAP) > 0 ? HOLD_CAP : quarter;
        return capped.compareTo(MIN_HOLD) < 0 ? MIN_HOLD : capped;
    }

    /** clamp(needed - w, 1 s, maxHold); maxHold when the watermark is unknown or stale. */
    static Duration holdFor(Instant w, Instant needed, Duration maxHold) {
        if (w.equals(Instant.EPOCH)) return maxHold;
        Duration gap = Duration.between(w, needed);
        if (gap.compareTo(MIN_HOLD) < 0) return MIN_HOLD;
        return gap.compareTo(maxHold) > 0 ? maxHold : gap;
    }
```

- [ ] **Step 4: Run the tests and watch them pass.** Run the command from Step 2, then `./gradlew test`. Expected: PASS, BUILD SUCCESSFUL. The existing `theReleaseDelayGrowsWithTheLagBetweenOneAndSixtySeconds` still passes, since the defaults give 60 s. `FakeClockVerifierTest` and `PipelineTest` use the defaults and are unaffected.

- [ ] **Step 5: Commit.**

```bash
git add src/main/java/com/quince/cartrecovery/core/ReminderScheduler.java src/test/java/com/quince/cartrecovery/core/ReminderSchedulerTest.java
git commit -m "$(cat <<'EOF'
Derive the scheduler's hold cap from the smallest lateness bound

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: TimerStore returns what it displaced

**Files:**
- Modify: `src/main/java/com/quince/cartrecovery/ports/TimerStore.java` (whole file)
- Modify: `src/main/resources/redis/upsert.lua` (whole file), `src/main/resources/redis/remove.lua` (whole file)
- Modify: `src/main/java/com/quince/cartrecovery/infra/redis/RedisTimerStore.java:69-80` (`upsert`, `remove`), plus imports
- Modify: `src/main/java/com/quince/cartrecovery/inmemory/PriorityQueueTimerStore.java:39-49`
- Modify: `src/main/java/com/quince/cartrecovery/core/Reconciler.java:94`
- Modify: `docs/superpowers/plans/2026-09-26-production-infra.md` §1.2 (`TimerStore` block)
- Test: `src/test/java/com/quince/cartrecovery/contract/TimerStoreContract.java:142-157,166,191` plus two new tests
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/redis/LuaScriptsTest.java:46-50,66-69,187-199` plus one new test
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisTimerStoreTest.java:52,62-65,142`

**Interfaces:**
- Consumes: `Timer`, `RedisTimerStore.unpack(String, String)`.
- Produces:
  - `record TimerStore.Upsert(boolean written, Optional<Timer> displaced)`
  - `TimerStore.Upsert TimerStore.upsert(Timer)`
  - `Optional<Timer> TimerStore.remove(String cartId, long version)`
  - `upsert.lua` returns `{written, previous-packed-or-''}`, and `remove.lua` returns the removed packed value or `''`.

- [ ] **Step 1: Write the failing contract tests.** In `TimerStoreContract.java`:

  1. Add `import java.util.Optional;`.
  2. Replace the body of `upsertOnlyMovesForwardByVersionThenOffset` with:

```java
        String id = cart("a");
        Instant due = at(Duration.ofHours(1));
        assertTrue(store.upsert(Timer.checkAbandon(id, 2, due, 0)).written());
        assertFalse(store.upsert(Timer.checkAbandon(id, 1, due, 0)).written());
        assertFalse(store.upsert(Timer.reminder(id, 1, 2, due, 0)).written());
        assertTrue(store.upsert(Timer.reminder(id, 2, 0, due, 0)).written());
        assertFalse(store.upsert(Timer.checkAbandon(id, 2, due, 0)).written());
        assertTrue(store.upsert(Timer.reminder(id, 2, 1, due, 0)).written());
        assertTrue(store.upsert(Timer.checkAbandon(id, 3, due, 0)).written());
```

  3. Replace `        assertFalse(store.upsert(a));` with `        assertFalse(store.upsert(a).written());`.
  4. Replace `        assertFalse(store.upsert(Timer.reminder(cart("a"), 1, 0, at(Duration.ofSeconds(-1)), 7)));` with `        assertFalse(store.upsert(Timer.reminder(cart("a"), 1, 0, at(Duration.ofSeconds(-1)), 7)).written());`.
  5. Replace `        assertTrue(store.upsert(Timer.checkAbandon(id, 1, at(Duration.ofHours(1)), 0)));` with `        assertTrue(store.upsert(Timer.checkAbandon(id, 1, at(Duration.ofHours(1)), 0)).written());`.
  6. Add before the closing brace:

```java
    @Test
    void upsertReportsTheTimerItDisplaced() {
        String id = cart("x:y|z");
        Instant due = at(Duration.ofHours(1));
        Timer check = Timer.checkAbandon(id, 1, due, 2);
        Timer reminder = Timer.reminder(id, 1, 0, due, 2);

        assertEquals(new TimerStore.Upsert(true, Optional.empty()), store.upsert(check), "first write displaces nothing");
        assertEquals(new TimerStore.Upsert(true, Optional.of(check)), store.upsert(reminder), "overwrite");
        assertEquals(new TimerStore.Upsert(false, Optional.empty()), store.upsert(reminder), "equal no-op");
        assertEquals(new TimerStore.Upsert(false, Optional.empty()), store.upsert(Timer.checkAbandon(id, 1, due, 2)),
            "lower no-op");
    }

    @Test
    void removeReturnsTheTimerItDeleted() {
        String id = cart("a");
        Timer t = Timer.reminder(id, 3, 1, at(Duration.ofHours(1)), 5);
        store.upsert(t);

        assertEquals(Optional.empty(), store.remove(id, 2), "stored version is newer");
        assertEquals(Optional.of(t), store.remove(id, 3));
        assertEquals(Optional.empty(), store.remove(id, 3), "nothing left");
    }
```

- [ ] **Step 2: Run the tests and watch them fail.** Run `./gradlew test --tests 'com.quince.cartrecovery.inmemory.PriorityQueueTimerStoreContractTest'`. Expected: FAIL at `compileTestJava` with "cannot find symbol: class Upsert" and "boolean cannot be dereferenced".

- [ ] **Step 3: Change the port.** Replace `TimerStore.java` with:

```java
package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.Timer;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Derived due-time index, one timer per cart, rebuildable from durable state. Production: Redis scripts
 * on per-shard sorted sets, timed by Redis TIME. Timers compare by (version, offsetIndex), CHECK_ABANDON
 * having offsetIndex -1.
 */
public interface TimerStore {
    /** What an upsert did: whether it wrote, and the stored timer it overwrote (empty on a no-op or a first write). */
    record Upsert(boolean written, Optional<Timer> displaced) {}

    /** Writes only if (version, offsetIndex) is greater than the stored timer's; equal data is a no-op that keeps any lease. */
    Upsert upsert(Timer timer);

    /** Removes the cart's timer only if its version is <= version; returns the removed timer, empty if none was removed. */
    Optional<Timer> remove(String cartId, long version);

    /** Up to limit timers due by the store's time source; each is leased (re-due after the lease) until acked or released. */
    List<Timer> claimDue(int limit);

    /** If the stored timer still equals timer, makes it due again after delay. */
    void release(Timer timer, Duration delay);

    /** Removes the stored timer only if it still equals timer. */
    void ack(Timer timer);

    /** The subset of cartIds (all of this shard) that have a timer. */
    Set<String> existing(int shard, Collection<String> cartIds);
}
```

- [ ] **Step 4: Change the Lua scripts.** Replace `upsert.lua` with:

```lua
-- KEYS[1] timers:{s}   KEYS[2] timerdata:{s}
-- ARGV[1] cartId  ARGV[2] packed kind|version|offsetIndex|srcPartition|dueAtMillis
-- ARGV[3] version (non-negative decimal, no leading zeros)  ARGV[4] offsetIndex (-1 for CHECK_ABANDON)
-- ARGV[5] dueAtMillis
-- Writes only if (version, offsetIndex) is greater than the stored pair. Equal is a no-op that keeps
-- the score, so a claimed timer keeps its lease. Returns {1, previous packed value or ''} if written,
-- {0, ''} otherwise.
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
  if c < 0 or (c == 0 and tonumber(ARGV[4]) <= tonumber(o)) then return {0, ''} end
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[5], ARGV[1])
return {1, cur or ''}
```

  Replace `remove.lua` with:

```lua
-- KEYS[1] timers:{s}   KEYS[2] timerdata:{s}
-- ARGV[1] cartId  ARGV[2] version (non-negative decimal)
-- Removes the timer only if its stored version is <= ARGV[2]. Returns the removed packed value, or '' if none.
local function cmpver(a, b)
  if #a ~= #b then return (#a < #b) and -1 or 1 end
  if a == b then return 0 end
  return (a < b) and -1 or 1
end

local cur = redis.call('HGET', KEYS[2], ARGV[1])
if not cur then return '' end
local v = string.match(cur, '^[^|]*|(%d+)|')
if cmpver(v, ARGV[2]) > 0 then return '' end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
return cur
```

- [ ] **Step 5: Implement the adapters.**

  In `RedisTimerStore.java`, add `import java.util.Optional;`. Then replace the `upsert` and `remove` methods (lines 69–80) with:

```java
    @Override
    public Upsert upsert(Timer t) {
        if (t.version() < 0) throw new IllegalArgumentException("timer version must not be negative: " + t.version());
        List<Object> r = scripts.run("upsert", ScriptOutputType.MULTI, keys(t.cartId()), t.cartId(), pack(t),
                Long.toString(t.version()), Integer.toString(t.offsetIndex()), Long.toString(t.dueAt().toEpochMilli()));
        return new Upsert((Long) r.get(0) == 1L, displaced(t.cartId(), (String) r.get(1)));
    }

    @Override
    public Optional<Timer> remove(String cartId, long version) {
        String removed = scripts.run("remove", ScriptOutputType.VALUE, keys(cartId), cartId, Long.toString(version));
        return displaced(cartId, removed);
    }

    /** Lua's '' means nothing was displaced; Lettuce may decode it as "" or null. */
    private static Optional<Timer> displaced(String cartId, String packed) {
        return packed == null || packed.isEmpty() ? Optional.empty() : Optional.of(unpack(cartId, packed));
    }
```

  In `PriorityQueueTimerStore.java`, replace the `upsert` and `remove` methods (lines 39–49) with:

```java
    @Override public synchronized Upsert upsert(Timer timer) {
        Slot current = live.get(timer.cartId());
        if (current != null && !greater(timer, current.timer())) return new Upsert(false, Optional.empty());
        put(new Slot(timer, timer.dueAt()));
        return new Upsert(true, Optional.ofNullable(current).map(Slot::timer));
    }

    @Override public synchronized Optional<Timer> remove(String cartId, long version) {
        Slot current = live.get(cartId);
        if (current == null || current.timer().version() > version) return Optional.empty();
        live.remove(cartId);
        return Optional.of(current.timer());
    }
```

  `Optional` is already imported there.

  In `Reconciler.java`, replace `        if (timers.upsert(timer)) metrics.increment("reconcile.timers_rebuilt");` with `        if (timers.upsert(timer).written()) metrics.increment("reconcile.timers_rebuilt");`.

- [ ] **Step 6: Update the infra tests.**

  In `LuaScriptsTest.java`, replace the `upsert` helper (lines 46–50) with:

```java
    /** [written (Long), previous packed value or ""]; Lettuce may decode Lua's '' as null, which the store treats alike. */
    private List<Object> upsertRaw(String id, String kind, long version, int offset, int src, long dueAt) {
        List<Object> r = redis.eval(lua("upsert"), ScriptOutputType.MULTI, new String[] {Z, H},
                id, packed(kind, version, offset, src, dueAt), Long.toString(version), Integer.toString(offset), Long.toString(dueAt));
        return List.of(r.get(0), r.get(1) == null ? "" : r.get(1));
    }

    private long upsert(String id, String kind, long version, int offset, int src, long dueAt) {
        return (Long) upsertRaw(id, kind, version, offset, src, dueAt).get(0);
    }
```

  Replace the `remove` helper (lines 66–69) with:

```java
    private String remove(String id, long version) {
        String r = redis.eval(lua("remove"), ScriptOutputType.VALUE, new String[] {Z, H}, id, Long.toString(version));
        return r == null ? "" : r;
    }
```

  Replace the body of `removeOnlyIfStoredVersionIsNotGreater` with:

```java
        upsert("c1", "REMINDER", 5, 1, 0, 1_000);
        assertEquals("", remove("c1", 4));
        assertEquals("REMINDER|5|1|0|1000", redis.hget(H, "c1"));
        assertEquals("REMINDER|5|1|0|1000", remove("c1", 5));
        assertNull(redis.hget(H, "c1"));
        assertNull(redis.zscore(Z, "c1"));

        upsert("c2", "CHECK_ABANDON", 5, -1, 0, 1_000);
        assertEquals("CHECK_ABANDON|5|-1|0|1000", remove("c2", 9));
        assertEquals("", remove("missing", 1));
```

  Add this test:

```java
    @Test
    void upsertReturnsThePreviousPackedValueOnlyWhenItOverwrites() {
        assertEquals(List.of(1L, ""), upsertRaw("c1", "CHECK_ABANDON", 1, -1, 0, 1_000), "first write");
        assertEquals(List.of(1L, "CHECK_ABANDON|1|-1|0|1000"), upsertRaw("c1", "REMINDER", 1, 0, 0, 2_000), "overwrite");
        assertEquals(List.of(0L, ""), upsertRaw("c1", "REMINDER", 1, 0, 0, 2_000), "equal no-op");
        assertEquals(List.of(0L, ""), upsertRaw("c1", "CHECK_ABANDON", 1, -1, 0, 3_000), "lower no-op");
    }
```

  In `RedisTimerStoreTest.java`, make these replacements:

  | Old | New |
  |---|---|
  | `assertTrue(store.upsert(t));` | `assertTrue(store.upsert(t).written());` |
  | `assertTrue(store.upsert(r0));` | `assertTrue(store.upsert(r0).written());` |
  | `assertFalse(store.upsert(r0));` | `assertFalse(store.upsert(r0).written());` |
  | `assertFalse(store.upsert(Timer.checkAbandon("c", 2, LONG_AGO, 1)));` | `assertFalse(store.upsert(Timer.checkAbandon("c", 2, LONG_AGO, 1)).written());` |
  | `assertTrue(store.upsert(Timer.checkAbandon("c", 3, LONG_AGO, 1)));` | `assertTrue(store.upsert(Timer.checkAbandon("c", 3, LONG_AGO, 1)).written());` |
  | `assertTrue(store.upsert(Timer.checkAbandon("c", 1, LONG_AGO, 0)));` | `assertTrue(store.upsert(Timer.checkAbandon("c", 1, LONG_AGO, 0)).written());` |

- [ ] **Step 7: Update the frozen contract.** In `docs/superpowers/plans/2026-09-26-production-infra.md` §1.2, replace:

```
    boolean upsert(Timer timer);                          // only if (version, offsetIndex) greater; equal data is a no-op
    void remove(String cartId, long version);             // only if stored version <= version
```

with:

```
    record Upsert(boolean written, Optional<Timer> displaced) {}   // displaced: the stored timer the write overwrote
    Upsert upsert(Timer timer);                           // only if (version, offsetIndex) greater; equal data is a no-op (displaces nothing)
    Optional<Timer> remove(String cartId, long version);  // only if stored version <= version; returns the removed timer (review fixes 2026-09-28)
```

- [ ] **Step 8: Run the tests and watch them pass.** Run `./gradlew test`. Expected: BUILD SUCCESSFUL. With Docker running, run `./gradlew integrationTest --tests '*LuaScriptsTest' --tests '*RedisTimerStoreContractTest' --tests '*RedisTimerStoreTest'`. Expected: PASS, none skipped.

- [ ] **Step 9: Commit.**

```bash
git add src/main/java/com/quince/cartrecovery/ports/TimerStore.java src/main/resources/redis/upsert.lua src/main/resources/redis/remove.lua src/main/java/com/quince/cartrecovery/infra/redis/RedisTimerStore.java src/main/java/com/quince/cartrecovery/inmemory/PriorityQueueTimerStore.java src/main/java/com/quince/cartrecovery/core/Reconciler.java src/test/java/com/quince/cartrecovery/contract/TimerStoreContract.java src/integrationTest/java/com/quince/cartrecovery/infra/redis/LuaScriptsTest.java src/integrationTest/java/com/quince/cartrecovery/infra/redis/RedisTimerStoreTest.java docs/superpowers/plans/2026-09-26-production-infra.md
git commit -m "$(cat <<'EOF'
Return the displaced timer from TimerStore upsert and remove

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: SUPERSEDED outcome from the detector

**Files:**
- Modify: `src/main/java/com/quince/cartrecovery/model/OutcomeKind.java`
- Modify: `src/main/java/com/quince/cartrecovery/core/AbandonmentDetector.java` (whole file)
- Modify: `src/main/java/com/quince/cartrecovery/app/DetectorRole.java:29-32` plus an import
- Modify: `src/main/java/com/quince/cartrecovery/Pipeline.java:81`
- Modify: `docs/superpowers/plans/2026-09-26-production-infra.md` §1.1 (`OutcomeKind`) and §1.3 (`AbandonmentDetector`)
- Test: `src/test/java/com/quince/cartrecovery/core/AbandonmentDetectorTest.java` (fixture, 4 constructor sites, new tests)
- Test: `src/integrationTest/java/com/quince/cartrecovery/e2e/EndToEndIT.java:47-48,207-231,263`

**Interfaces:**
- Consumes (Task 3): `TimerStore.Upsert upsert(Timer)`, `.displaced()`, and `Optional<Timer> remove(String, long)`.
- Produces:
  - `OutcomeKind.SUPERSEDED`
  - `AbandonmentDetector(RecoveryConfig, CartStateStore, TimerStore, ArmAssigner, OutcomeRecorder, Metrics)`
  - metric `reminders.superseded`

- [ ] **Step 1: Write the failing tests.** In `AbandonmentDetectorTest.java`:

  1. Add these imports:
     - `import com.quince.cartrecovery.inmemory.InMemoryOutcomeRecorder;`
     - `import com.quince.cartrecovery.model.LedgerKey;`
     - `import com.quince.cartrecovery.model.Outcome;`
     - `import com.quince.cartrecovery.model.OutcomeKind;`
  2. Add the field `    private InMemoryOutcomeRecorder outcomes;`.
  3. In `setUp`, replace `        metrics = new Metrics();\n        detector = new AbandonmentDetector(RecoveryConfig.defaults(), store, timers, key -> Arm.TREATMENT, metrics);` with:

```java
        metrics = new Metrics();
        outcomes = new InMemoryOutcomeRecorder();
        detector = new AbandonmentDetector(RecoveryConfig.defaults(), store, timers, key -> Arm.TREATMENT, outcomes, metrics);
```

  4. Replace `            RecoveryConfig.defaults(), store, timers, key -> Arm.HOLDOUT, metrics);` with `            RecoveryConfig.defaults(), store, timers, key -> Arm.HOLDOUT, outcomes, metrics);`.
  5. Replace both occurrences of `            RecoveryConfig.defaults(), failingApply(), timers, key -> Arm.TREATMENT, metrics);` with `            RecoveryConfig.defaults(), failingApply(), timers, key -> Arm.TREATMENT, outcomes, metrics);`.
  6. Add before the closing brace:

```java
    private static Outcome supersededOutcome(long version, int offset, Instant at) {
        return new Outcome(new LedgerKey(CART, version, offset).toString(), CART, version, Arm.TREATMENT,
            OutcomeKind.SUPERSEDED, at, 0);
    }

    /** The cart abandoned at v1 with the given reminder pending, as the scheduler leaves it. */
    private void abandonedWith(Timer reminder) {
        detector.handle(edited(1, min(0)), 0);
        store.markAbandoned(store.get(CART).orElseThrow(), List.of(T0), true);
        timers.upsert(reminder);
    }

    @Test
    void aResumeOverADueReminderRecordsSupersededForEveryOffsetDueByThen() {
        abandonedWith(Timer.reminder(CART, 1, 0, at(min(30)), 0));
        detector.handle(resumed(2, min(65)), 0);
        assertEquals(List.of(supersededOutcome(1, 0, at(min(65))), supersededOutcome(1, 1, at(min(65)))), outcomes.all());
        assertEquals(2, metrics.get("reminders.superseded"));
    }

    @Test
    void aPurchaseOverADueReminderRecordsSuperseded() {
        abandonedWith(Timer.reminder(CART, 1, 1, at(hrs(1)), 0));
        detector.handle(purchased(2, min(62)), 0);
        assertEquals(List.of(supersededOutcome(1, 1, at(min(62)))), outcomes.all());
    }

    @Test
    void aReminderDueExactlyAtTheEventIsSuperseded() {
        abandonedWith(Timer.reminder(CART, 1, 0, at(min(30)), 0));
        detector.handle(resumed(2, min(30)), 0);
        assertEquals(List.of(supersededOutcome(1, 0, at(min(30)))), outcomes.all());
    }

    @Test
    void aReminderNotYetDueRecordsNothing() {
        abandonedWith(Timer.reminder(CART, 1, 1, at(hrs(1)), 0));
        detector.handle(resumed(2, min(59)), 0);
        assertEquals(List.of(), outcomes.all());
    }

    @Test
    void anOverdueCheckOnATreatmentCartRecordsSupersededForTheRemindersItOwed() {
        detector.handle(edited(1, min(0)), 0);
        detector.handle(resumed(2, min(61)), 0);
        assertEquals(List.of(supersededOutcome(1, 0, at(min(61))), supersededOutcome(1, 1, at(min(61)))), outcomes.all());
    }

    @Test
    void anOverdueCheckOnAHoldoutCartRecordsNothing() {
        AbandonmentDetector holdout = new AbandonmentDetector(
            RecoveryConfig.defaults(), store, timers, key -> Arm.HOLDOUT, outcomes, metrics);
        holdout.handle(edited(1, min(0)), 0);
        holdout.handle(resumed(2, min(31)), 0);
        assertEquals(List.of(), outcomes.all());
    }

    @Test
    void anOverdueCheckOnACartAtItsFrequencyCapRecordsNothing() {
        AbandonmentDetector capped = new AbandonmentDetector(
            RecoveryConfig.defaults().withFrequencyCap(1), store, timers, key -> Arm.TREATMENT, outcomes, metrics);
        CartRecord first = store.applyEvent(edited(1, min(0)), Arm.TREATMENT, 0).orElseThrow();
        store.markAbandoned(first, List.of(T0), true);
        store.applyEvent(edited(2, min(40)), Arm.TREATMENT, 0);
        timers.upsert(Timer.checkAbandon(CART, 2, at(min(70)), 0));

        capped.handle(resumed(3, min(71)), 0);

        assertEquals(List.of(), outcomes.all());
    }

    @Test
    void aRedeliveredEventRecordsNothingNew() {
        detector.handle(edited(1, min(0)), 0);
        detector.handle(resumed(2, min(31)), 0);
        detector.handle(resumed(2, min(31)), 0);
        assertEquals(List.of(supersededOutcome(1, 0, at(min(31)))), outcomes.all());
    }

    @Test
    void aRedeliveredPurchaseRecordsNothingNew() {
        abandonedWith(Timer.reminder(CART, 1, 0, at(min(30)), 0));
        detector.handle(purchased(2, min(40)), 0);
        detector.handle(purchased(2, min(40)), 0);
        assertEquals(List.of(supersededOutcome(1, 0, at(min(40)))), outcomes.all());
    }
```

- [ ] **Step 2: Run the tests and watch them fail.** Run `./gradlew test --tests 'com.quince.cartrecovery.core.AbandonmentDetectorTest'`. Expected: FAIL at `compileTestJava`. The six-argument constructor does not exist, and `SUPERSEDED` is an unknown symbol.

- [ ] **Step 3: Add the outcome kind.** Replace the `OutcomeKind.java` body line with `public enum OutcomeKind { ABANDONED, SENT, SKIPPED_LATE, CANCELLED, DEAD, SUPERSEDED }`.

- [ ] **Step 4: Implement the detector.** Replace `AbandonmentDetector.java` with:

```java
package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerKind;
import com.quince.cartrecovery.ports.ArmAssigner;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Instant;
import java.util.Optional;

/**
 * Consumes cart events. Writes the timer first (monotonic upsert or conditional remove), then the conditional
 * cart update, so a crash between the two leaves at most a missing or extra timer: a stale timer is rejected by
 * the monotonic upsert or dropped on fire, and a redelivered event repeats both writes. Review fix 2: a timer write
 * that displaces reminders already owed records SUPERSEDED for them. A redelivered event's write is a no-op and
 * displaces nothing, so it records nothing twice; a crash between the timer write and the outcome loses that outcome.
 */
public final class AbandonmentDetector {
    private final RecoveryConfig config;
    private final ReminderPolicy policy;
    private final CartStateStore store;
    private final TimerStore timers;
    private final ArmAssigner arms;
    private final OutcomeRecorder outcomes;
    private final Metrics metrics;

    public AbandonmentDetector(RecoveryConfig config, CartStateStore store, TimerStore timers,
                               ArmAssigner arms, OutcomeRecorder outcomes, Metrics metrics) {
        this.config = config;
        this.policy = new ReminderPolicy(config);
        this.store = store;
        this.timers = timers;
        this.arms = arms;
        this.outcomes = outcomes;
        this.metrics = metrics;
    }

    /** srcPartition is the cart-events partition the event was actually consumed from. */
    public void handle(CartEvent event, int srcPartition) {
        Optional<Timer> displaced = switch (event) {
            case CartEvent.CartEdited e -> timers.upsert(checkAbandon(e, srcPartition)).displaced();
            case CartEvent.CartResumed e -> timers.upsert(checkAbandon(e, srcPartition)).displaced();
            case CartEvent.CartCleared e -> timers.remove(e.cartId(), e.version());
            case CartEvent.CartPurchased e -> timers.remove(e.cartId(), e.version());
        };
        displaced.ifPresent(t -> superseded(t, event.occurredAt()));
        if (store.applyEvent(event, arms.assign(event.shopperKey()), srcPartition).isEmpty()) {
            metrics.increment("events.ignored");
            return;
        }
        metrics.increment("events.handled");
    }

    /**
     * SUPERSEDED for each reminder of the displaced timer's cycle due at or before occurredAt, offset j being due at
     * base + offset_j. A REMINDER(v, i) owes offsets j >= i (base = its dueAt - offset_i). An overdue CHECK_ABANDON(v)
     * owes offsets from 0 (base = its dueAt - window), but only for a cart the check would have given a sequence; that
     * cart is read here, before the cart update, and only on this rare path. A timer not yet due owes nothing.
     */
    private void superseded(Timer displaced, Instant occurredAt) {
        boolean reminder = displaced.kind() == TimerKind.REMINDER;
        int from = reminder ? displaced.offsetIndex() : 0;
        if (from < 0 || from >= config.offsets().size()) return;
        Instant base = displaced.dueAt().minus(reminder ? config.offsets().get(from) : config.window());
        if (base.plus(config.offsets().get(from)).isAfter(occurredAt)) return;
        if (!reminder && !sequenceOwed(displaced)) return;
        for (int j = from; j < config.offsets().size(); j++) {
            if (base.plus(config.offsets().get(j)).isAfter(occurredAt)) break;   // offsets increase: later ones are later still
            String key = new LedgerKey(displaced.cartId(), displaced.version(), j).toString();
            outcomes.record(new Outcome(key, displaced.cartId(), displaced.version(), Arm.TREATMENT,
                OutcomeKind.SUPERSEDED, occurredAt, 0));
            metrics.increment("reminders.superseded");
        }
    }

    /** As the overdue check would have found the cart: still ACTIVE at its version, treatment, within the cap at its due time. */
    private boolean sequenceOwed(Timer check) {
        Optional<CartRecord> cart = store.get(check.cartId());
        return cart.isPresent() && cart.get().version() == check.version() && cart.get().status() == CartStatus.ACTIVE
            && policy.eligible(cart.get().abandoned(), check.dueAt());
    }

    private Timer checkAbandon(CartEvent e, int srcPartition) {
        return Timer.checkAbandon(e.cartId(), e.version(), e.occurredAt().plus(config.window()), srcPartition);
    }
}
```

- [ ] **Step 5: Wire the recorder.**

  In `DetectorRole.java`, add `import com.quince.cartrecovery.infra.kafka.KafkaOutcomeRecorder;`. Then replace:

```java
                new HashArmAssigner(HashArmAssigner.SALT, config.recovery().holdoutPercent()), metrics);
```

  with:

```java
                new HashArmAssigner(HashArmAssigner.SALT, config.recovery().holdoutPercent()),
                new KafkaOutcomeRecorder(ctx.producer()), metrics);
```

  In `Pipeline.java`, replace `        this.detector = new AbandonmentDetector(config, store, timers, arms, metrics);` with `        this.detector = new AbandonmentDetector(config, store, timers, arms, outcomes, metrics);`.

- [ ] **Step 6: Update `EndToEndIT`.** The stopped-detector case must resolve to `SUPERSEDED` or `CANCELLED`, never to no outcome, and the precedence must know `SUPERSEDED`.

  1. Replace:

```java
    static final List<OutcomeKind> PRECEDENCE =
        List.of(OutcomeKind.SENT, OutcomeKind.DEAD, OutcomeKind.CANCELLED, OutcomeKind.SKIPPED_LATE);
```

  with:

```java
    static final List<OutcomeKind> PRECEDENCE = List.of(OutcomeKind.SENT, OutcomeKind.DEAD, OutcomeKind.CANCELLED,
        OutcomeKind.SKIPPED_LATE, OutcomeKind.SUPERSEDED);
```

  2. Replace `    /** One outcome per key by precedence SENT > DEAD > CANCELLED > SKIPPED_LATE; ABANDONED has no key. */` with `    /** One outcome per key by precedence SENT > DEAD > CANCELLED > SKIPPED_LATE > SUPERSEDED; ABANDONED has no key. */`.
  3. In `stoppedDetectorHoldsTheReminderUntilThePurchaseCancelsIt`, replace:

```java
        Await.until(() -> {
            Outcome o = resolved(cart).get(key(cart, 1, 0));
            return o != null && o.kind() == OutcomeKind.CANCELLED;
        }, WAIT);
        assertEquals(List.of(), sentKeys(cart));
```

  with:

```java
        // Review fix 2: the held reminder resolves to CANCELLED (its intent was published) or SUPERSEDED (its timer was
        // still pending when the purchase landed), never to no outcome.
        Await.until(() -> {
            Outcome o = resolved(cart).get(key(cart, 1, 0));
            return o != null && (o.kind() == OutcomeKind.CANCELLED || o.kind() == OutcomeKind.SUPERSEDED);
        }, WAIT);
        assertEquals(List.of(), sentKeys(cart));
        assertTrue(resolved(cart).values().stream()
            .allMatch(o -> o.kind() == OutcomeKind.CANCELLED || o.kind() == OutcomeKind.SUPERSEDED), resolved(cart).toString());
```

- [ ] **Step 7: Update the frozen contract.** In the master plan §1.1, replace `enum OutcomeKind { ABANDONED, SENT, SKIPPED_LATE, CANCELLED, DEAD }` with `enum OutcomeKind { ABANDONED, SENT, SKIPPED_LATE, CANCELLED, DEAD, SUPERSEDED }   // SUPERSEDED: review fixes 2026-09-28`. In §1.3, replace `AbandonmentDetector(RecoveryConfig, CartStateStore, TimerStore, ArmAssigner, Metrics)` with `AbandonmentDetector(RecoveryConfig, CartStateStore, TimerStore, ArmAssigner, OutcomeRecorder, Metrics)`.

- [ ] **Step 8: Run the tests and watch them pass.** Run `./gradlew test`. Expected: BUILD SUCCESSFUL, with `AbandonmentDetectorTest`, `FakeClockVerifierTest` and `PipelineTest` green. With Docker, run `./gradlew integrationTest --tests '*EndToEndIT' --tests '*DetectorRoleIT'`. Expected: PASS.

- [ ] **Step 9: Commit.**

```bash
git add src/main/java/com/quince/cartrecovery/model/OutcomeKind.java src/main/java/com/quince/cartrecovery/core/AbandonmentDetector.java src/main/java/com/quince/cartrecovery/app/DetectorRole.java src/main/java/com/quince/cartrecovery/Pipeline.java src/test/java/com/quince/cartrecovery/core/AbandonmentDetectorTest.java src/integrationTest/java/com/quince/cartrecovery/e2e/EndToEndIT.java docs/superpowers/plans/2026-09-26-production-infra.md
git commit -m "$(cat <<'EOF'
Record SUPERSEDED when the detector displaces a reminder already owed

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Reconciler records SKIPPED_LATE once

**Files:**
- Modify: `src/main/java/com/quince/cartrecovery/core/Reconciler.java` (whole file)
- Modify: `src/main/java/com/quince/cartrecovery/app/ReconcilerRole.java:55-57` plus an import
- Modify: `src/main/java/com/quince/cartrecovery/Pipeline.java:85`
- Modify: `docs/superpowers/plans/2026-09-26-production-infra.md` §1.3 (`Reconciler`)
- Test: `src/test/java/com/quince/cartrecovery/core/ReconcilerTest.java` (fixture plus three tests)
- Test: `src/integrationTest/java/com/quince/cartrecovery/app/ReconcilerRoleIT.java:95-96` plus an import

**Interfaces:**
- Consumes (Task 3): `TimerStore.upsert(Timer).written()`.
- Produces: `Reconciler(RecoveryConfig, CartStateStore, TimerStore, SendLedger, OutcomeRecorder, Clock, Metrics)` and the metric `reconcile.skipped_late`.

- [ ] **Step 1: Write the failing tests.** In `ReconcilerTest.java`:

  1. Add these imports:
     - `import com.quince.cartrecovery.inmemory.InMemoryOutcomeRecorder;`
     - `import com.quince.cartrecovery.model.Outcome;`
     - `import com.quince.cartrecovery.ports.TimerStore;`
     - `import java.util.Set;`
  2. Add the field `    private InMemoryOutcomeRecorder outcomes;`.
  3. In `setUp`, replace `        metrics = new Metrics();` with `        metrics = new Metrics();\n        outcomes = new InMemoryOutcomeRecorder();`.
  4. Replace `        reconciler = new Reconciler(RecoveryConfig.defaults(), recording, timers, ledger, clock, metrics);` with `        reconciler = new Reconciler(RecoveryConfig.defaults(), recording, timers, ledger, outcomes, clock, metrics);`.
  5. Add before the closing brace:

```java
    private static Outcome skipped(String cartId, int offset, Instant at) {
        return new Outcome(new LedgerKey(cartId, 1, offset).toString(), cartId, 1, Arm.TREATMENT, OutcomeKind.SKIPPED_LATE, at, 0);
    }

    @Test
    void aRebuildThatSkipsLateOffsetsRecordsEachOnceAcrossSweeps() {
        CartRecord r = edit("a", 1);
        store.markAbandoned(r, List.of(T0), true);
        clock.set(at(hrs(3)));

        reconcileAll();
        reconcileAll();

        assertEquals(List.of(skipped("a", 0, at(hrs(3))), skipped("a", 1, at(hrs(3)))), outcomes.all());
        assertEquals(2, metrics.get("reconcile.skipped_late"));
    }

    @Test
    void anEndedSequenceRecordsItsSkipsOnce() {
        CartRecord r = edit("a", 0);
        store.markAbandoned(r, List.of(T0), true);
        sendRow("a", 0);
        clock.set(at(hrs(24)).plus(min(31)));

        reconcileAll();
        reconcileAll();

        assertEquals(List.of(skipped("a", 1, clock.now()), skipped("a", 2, clock.now())), outcomes.all());
    }

    @Test
    void aRebuildWhoseUpsertDoesNotWriteRecordsNothing() {
        CartRecord r = edit("a", 1);
        store.markAbandoned(r, List.of(T0), true);
        timers.upsert(Timer.reminder("a", 1, 2, at(hrs(24)), 1));   // another writer rebuilt it first
        TimerStore blind = (TimerStore) Proxy.newProxyInstance(TimerStore.class.getClassLoader(),
            new Class<?>[] {TimerStore.class},
            (proxy, method, args) -> method.getName().equals("existing") ? Set.of() : method.invoke(timers, args));
        Reconciler racing = new Reconciler(RecoveryConfig.defaults(), store, blind, ledger, outcomes, clock, metrics);
        clock.set(at(hrs(3)));

        for (int s = 0; s < SHARDS; s++) racing.reconcileShard(s);

        assertEquals(List.of(), outcomes.all());
    }
```

- [ ] **Step 2: Run the tests and watch them fail.** Run `./gradlew test --tests 'com.quince.cartrecovery.core.ReconcilerTest'`. Expected: FAIL at `compileTestJava`, because no seven-argument `Reconciler` constructor exists.

- [ ] **Step 3: Implement.** Replace `Reconciler.java` with:

```java
package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import com.quince.cartrecovery.ports.SendLedger;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Rebuilds missing timers for one shard from durable state. Compares keys only: open cart ids against the timer
 * index, in batches, and reloads just the carts whose timer is missing. An ACTIVE cart needs its abandonment
 * check; an eligible ABANDONED cart needs the first offset after the ledger's highest that is still within its
 * lateness bound, or its sequence ended when none remains. Offsets skipped as already late are recorded
 * SKIPPED_LATE (review fix 2), only when the rebuild's upsert wrote or its endSequence succeeded, so once, not on every
 * sweep. Idempotent, so it needs no lock.
 */
public final class Reconciler {
    static final int BATCH = 100;

    private final RecoveryConfig config;
    private final ReminderPolicy policy;
    private final CartStateStore store;
    private final TimerStore timers;
    private final SendLedger ledger;
    private final OutcomeRecorder outcomes;
    private final Clock clock;
    private final Metrics metrics;

    public Reconciler(RecoveryConfig config, CartStateStore store, TimerStore timers,
                      SendLedger ledger, OutcomeRecorder outcomes, Clock clock, Metrics metrics) {
        this.config = config;
        this.policy = new ReminderPolicy(config);
        this.store = store;
        this.timers = timers;
        this.ledger = ledger;
        this.outcomes = outcomes;
        this.clock = clock;
        this.metrics = metrics;
    }

    public void reconcileShard(int shard) {
        Instant now = clock.now();
        try (Stream<String> ids = store.openCartIds(shard, now)) {
            Iterator<String> it = ids.iterator();
            List<String> batch = new ArrayList<>(BATCH);
            while (it.hasNext()) {
                batch.add(it.next());
                if (batch.size() == BATCH || !it.hasNext()) {
                    rebuildMissing(shard, batch, now);
                    batch.clear();
                }
            }
        }
    }

    private void rebuildMissing(int shard, List<String> batch, Instant now) {
        Set<String> present = timers.existing(shard, batch);
        List<String> missing = batch.stream().filter(id -> !present.contains(id)).toList();
        if (missing.isEmpty()) return;
        for (CartRecord r : store.getAll(missing)) {
            switch (r.status()) {
                case ACTIVE -> rebuilt(Timer.checkAbandon(
                    r.cartId(), r.version(), r.lastActivityAt().plus(config.window()), r.srcPartition()));
                case ABANDONED -> {
                    if (!policy.eligible(r, now)) continue;
                    int from = ledger.highestOffsetIndex(r.cartId(), r.version()) + 1;
                    int next = nextOnTimeOffset(r, from, now);
                    boolean settled;
                    if (next < config.offsets().size()) {
                        settled = rebuilt(Timer.reminder(r.cartId(), r.version(), next,
                            r.lastActivityAt().plus(config.offsets().get(next)), r.srcPartition()));
                    } else {
                        settled = store.endSequence(r.cartId(), r.version());
                        if (settled) metrics.increment("reconcile.sequences_ended");
                    }
                    if (settled) skippedLate(r, from, next, now);
                }
                case CLOSED -> { }
            }
        }
    }

    /** First offset from {@code from} whose due time plus lateness bound has not passed. */
    private int nextOnTimeOffset(CartRecord r, int from, Instant now) {
        int i = from;
        while (i < config.offsets().size()
            && r.lastActivityAt().plus(config.offsets().get(i)).plus(config.latenessBounds().get(i)).isBefore(now)) {
            i++;
        }
        return i;
    }

    private boolean rebuilt(Timer timer) {
        boolean written = timers.upsert(timer).written();
        if (written) metrics.increment("reconcile.timers_rebuilt");
        return written;
    }

    /** One SKIPPED_LATE per offset in [from, next): each passed its lateness bound before the rebuild. */
    private void skippedLate(CartRecord r, int from, int next, Instant now) {
        for (int j = from; j < next; j++) {
            outcomes.record(new Outcome(new LedgerKey(r.cartId(), r.version(), j).toString(), r.cartId(), r.version(),
                Arm.TREATMENT, OutcomeKind.SKIPPED_LATE, now, 0));
            metrics.increment("reconcile.skipped_late");
        }
    }
}
```

- [ ] **Step 4: Wire the recorder.**

  In `ReconcilerRole.java`, add `import com.quince.cartrecovery.infra.kafka.KafkaOutcomeRecorder;`. Then replace:

```java
                new DynamoSendLedger(ctx.dynamo(), DynamoTables.SEND_LEDGER, config.dispatch().lease(), config.shards()), Instant::now, metrics);
```

  with:

```java
                new DynamoSendLedger(ctx.dynamo(), DynamoTables.SEND_LEDGER, config.dispatch().lease(), config.shards()),
                new KafkaOutcomeRecorder(ctx.producer()), Instant::now, metrics);
```

  In `Pipeline.java`, replace `        this.reconciler = new Reconciler(config, store, timers, ledger, clock, metrics);` with `        this.reconciler = new Reconciler(config, store, timers, ledger, outcomes, clock, metrics);`.

  In `ReconcilerRoleIT.java`, add `import com.quince.cartrecovery.inmemory.InMemoryOutcomeRecorder;`. Then replace `            new InMemorySendLedger(c.dispatch().lease(), c.shards()), Instant::now, metrics);` with `            new InMemorySendLedger(c.dispatch().lease(), c.shards()), new InMemoryOutcomeRecorder(), Instant::now, metrics);`.

- [ ] **Step 5: Update the frozen contract.** In the master plan §1.3, replace `Reconciler(RecoveryConfig, CartStateStore, TimerStore, SendLedger, Clock, Metrics)` with `Reconciler(RecoveryConfig, CartStateStore, TimerStore, SendLedger, OutcomeRecorder, Clock, Metrics)`.

- [ ] **Step 6: Run the tests and watch them pass.** Run `./gradlew test`. Expected: BUILD SUCCESSFUL. Then run `./gradlew compileIntegrationTestJava`. Expected: BUILD SUCCESSFUL. With Docker, also run `./gradlew integrationTest --tests '*ReconcilerRoleIT'`. Expected: PASS.

- [ ] **Step 7: Commit.**

```bash
git add src/main/java/com/quince/cartrecovery/core/Reconciler.java src/main/java/com/quince/cartrecovery/app/ReconcilerRole.java src/main/java/com/quince/cartrecovery/Pipeline.java src/test/java/com/quince/cartrecovery/core/ReconcilerTest.java src/integrationTest/java/com/quince/cartrecovery/app/ReconcilerRoleIT.java docs/superpowers/plans/2026-09-26-production-infra.md
git commit -m "$(cat <<'EOF'
Record SKIPPED_LATE once for offsets a reconciler rebuild skips

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Loadgen accounting from outcomes

**Files:**
- Modify: `src/main/java/com/quince/cartrecovery/loadgen/Accounting.java:16,20,78-86` plus a new method
- Modify: `src/main/java/com/quince/cartrecovery/loadgen/Expected.java` (whole file)
- Modify: `src/main/java/com/quince/cartrecovery/loadgen/CorrectnessSummary.java` (whole file)
- Modify: `src/main/java/com/quince/cartrecovery/loadgen/LoadTestSummary.java:35`
- Modify: `src/main/java/com/quince/cartrecovery/loadgen/Report.java:54-74`
- Modify: `src/main/java/com/quince/cartrecovery/loadgen/LoadgenRole.java:122-123,130-142,160`
- Test: `AccountingTest.java`, `ExpectedTest.java` (add tests), `CorrectnessSummaryTest.java`, `ReportTest.java` (whole files), all under `src/test/java/com/quince/cartrecovery/loadgen/`

**Interfaces:**
- Consumes: outcome kind strings, including `"SUPERSEDED"` (Task 4 emits it; no compile dependency).
- Produces:
  - `static Map<String, Instant> Accounting.supersededAt(List<OutcomeRow>)`
  - `static Map<String, Instant> Expected.sendBy(List<CartScript>, RecoveryConfig, ArmAssigner)`
  - `CorrectnessSummary.compute(Set<String>, List<OutcomeRow>, List<CartScript>, RecoveryConfig, ArmAssigner, Duration shift)`
  - `CorrectnessSummary.Result(..., long outcomesOnNonExpectedKeys, MissingBreakdown.Result scriptInference)`
  - a new `LoadTestSummary` component, `MissingBreakdown.Result scriptInference`, after `neverSupersededNoOutcome`

- [ ] **Step 1: Write the failing tests.**

  Add to `AccountingTest.java` (before the closing brace):

```java
    @Test void supersededRanksBelowEveryOtherKind() {
        assertEquals("SKIPPED_LATE", Accounting.resolveOutcomes(List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SUPERSEDED", T0, 0),
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SKIPPED_LATE", T0, 1))).get("k1"));
        assertEquals("CANCELLED", Accounting.resolveOutcomes(List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "CANCELLED", T0, 1),
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SUPERSEDED", T0, 0))).get("k1"));
        assertEquals("SUPERSEDED", Accounting.resolveOutcomes(List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SUPERSEDED", T0, 0))).get("k1"));
    }

    @Test void supersededAtIsTheEarliestSupersededTimePerKey() {
        Map<String, Instant> at = Accounting.supersededAt(List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SUPERSEDED", T0.plusSeconds(5), 0),
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SUPERSEDED", T0, 0),
            new OutcomeRow("k2", "cart-2", 1, "TREATMENT", "SENT", T0, 1)));
        assertEquals(Map.of("k1", T0), at);
    }
```

  Add to `ExpectedTest.java`. First add the imports `import java.time.Duration;` and `import java.util.Map;`, then add the test:

```java
    @Test void sendByIsEachExpectedKeysDueTimePlusItsLatenessBound() {
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(new Cycle(1L, T0, null)));

        Map<String, Instant> sendBy = Expected.sendBy(List.of(script), CONFIG, ALL_TREATMENT);

        assertEquals(Map.of(
            "cart-1:1:0", T0.plus(Duration.ofMinutes(35)),
            "cart-1:1:1", T0.plus(Duration.ofMinutes(65)),
            "cart-1:1:2", T0.plus(Duration.ofHours(24)).plus(Duration.ofMinutes(30))), sendBy);
        assertEquals(sendBy.keySet(), Expected.keys(List.of(script), CONFIG, ALL_TREATMENT));
    }
```

  Replace `CorrectnessSummaryTest.java` with:

```java
package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Review fix 2: the superseded and missing buckets come from reminder-outcomes, not script inference. Outcome counts
// stay restricted to expected keys (fix round 2), the identity holds, and the script inference is a cross-check.
class CorrectnessSummaryTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final RecoveryConfig CONFIG = RecoveryConfig.defaults();   // offsets 30m, 1h, 24h
    private static final ArmAssigner ALL_TREATMENT = shopperKey -> Arm.TREATMENT;
    private static final Instant DUE0 = T0.plus(CONFIG.offsets().get(0));
    private static final Instant SEND_BY0 = DUE0.plus(CONFIG.latenessBounds().get(0));

    private static OutcomeRow row(String key, String kind, Instant at) {
        return new OutcomeRow(key, key.substring(0, key.indexOf(':')), 1, "TREATMENT", kind, at, 0);
    }

    private static CorrectnessSummary.Result compute(List<CartScript> scripts, List<OutcomeRow> outcomes, Duration shift) {
        return CorrectnessSummary.compute(Expected.keys(scripts, CONFIG, ALL_TREATMENT), outcomes, scripts, CONFIG,
            ALL_TREATMENT, shift);
    }

    @Test void aNonExpectedOutcomeDoesNotCancelOutARealMissAndTheIdentityHolds() {
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(new Cycle(1L, T0, null)));
        Set<String> expectedKeys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);
        assertEquals(Set.of("cart-1:1:0", "cart-1:1:1", "cart-1:1:2"), expectedKeys);

        CorrectnessSummary.Result result = compute(List.of(script), List.of(row("cart-1:1:99", "SENT", T0)), Duration.ZERO);

        assertEquals(0, result.sent(), "the stray outcome must not be counted as a real sent key");
        assertEquals(1, result.outcomesOnNonExpectedKeys());
        assertEquals(0, result.supersededBeforeSend());
        assertEquals(0, result.supersededAfterSendBy());
        assertEquals(3, result.neverSupersededNoOutcome(), "all three expected keys have no outcome");
        assertEquals(3, result.unexplainedMissing());
        assertIdentity(expectedKeys.size(), result);
    }

    @Test void theBuggyBehaviorWouldHaveMadeUnexplainedSmallerThanTheRealMisses() {
        List<OutcomeRow> outcomes = List.of(row("cart-1:1:99", "SENT", T0));
        long buggySent = Accounting.countByKind(Accounting.resolveOutcomes(outcomes), "SENT");
        assertEquals(1, buggySent);
        assertEquals(2, Accounting.unexplainedMissing(3, buggySent, 0, 0, 0, 0));
    }

    @Test void aRealisticMixIsClassifiedFromOutcomesAndTheIdentityHolds() {
        CartScript sentScript = new CartScript("cart-sent", "s1", List.of(new Cycle(1L, T0, null)));
        CartScript supersededScript = new CartScript("cart-superseded", "s2", List.of(new Cycle(1L, T0, DUE0.plusSeconds(1))));
        CartScript missedScript = new CartScript("cart-missed", "s3", List.of(new Cycle(1L, T0, null)));
        CartScript lateScript = new CartScript("cart-late", "s4", List.of(new Cycle(1L, T0, SEND_BY0.plusSeconds(1))));
        List<CartScript> scripts = List.of(sentScript, supersededScript, missedScript, lateScript);

        List<OutcomeRow> outcomes = List.of(
            row("cart-sent:1:0", "SENT", T0), row("cart-sent:1:1", "SENT", T0), row("cart-sent:1:2", "SENT", T0),
            row("cart-superseded:1:0", "SUPERSEDED", DUE0.plusSeconds(1)),
            row("cart-late:1:0", "SUPERSEDED", SEND_BY0.plusSeconds(1)),
            row("some-other-cart:1:0", "SENT", T0));

        CorrectnessSummary.Result result = compute(scripts, outcomes, Duration.ZERO);

        assertEquals(3, result.sent());
        assertEquals(1, result.outcomesOnNonExpectedKeys());
        assertEquals(1, result.supersededBeforeSend());
        assertEquals(1, result.supersededAfterSendBy());
        assertEquals(3, result.neverSupersededNoOutcome());   // cart-missed's three offsets
        assertEquals(new MissingBreakdown.Result(1, 1, 3), result.scriptInference(), "the cross-check agrees here");
        assertIdentity(Expected.keys(scripts, CONFIG, ALL_TREATMENT).size(), result);
    }

    @Test void supersededExactlyAtTheShiftedSendByCountsAsBefore() {
        Duration shift = Duration.ofHours(1);
        CartScript script = new CartScript("cart-1", "s1", List.of(new Cycle(1L, T0, SEND_BY0)));

        CorrectnessSummary.Result onTime = compute(List.of(script),
            List.of(row("cart-1:1:0", "SUPERSEDED", SEND_BY0.plus(shift))), shift);
        CorrectnessSummary.Result late = compute(List.of(script),
            List.of(row("cart-1:1:0", "SUPERSEDED", SEND_BY0.plus(shift).plusMillis(1))), shift);

        assertEquals(1, onTime.supersededBeforeSend());
        assertEquals(0, onTime.unexplainedMissing());
        assertEquals(1, late.supersededAfterSendBy());
        assertEquals(1, late.unexplainedMissing());
    }

    @Test void aKeyThatWasSupersededAndThenCancelledCountsAsCancelled() {
        CartScript script = new CartScript("cart-1", "s1", List.of(new Cycle(1L, T0, DUE0.plusSeconds(1))));

        CorrectnessSummary.Result result = compute(List.of(script), List.of(
            row("cart-1:1:0", "SUPERSEDED", DUE0.plusSeconds(1)), row("cart-1:1:0", "CANCELLED", DUE0.plusSeconds(2))),
            Duration.ZERO);

        assertEquals(1, result.cancelled());
        assertEquals(0, result.supersededBeforeSend());
        assertIdentity(1, result);
    }

    private static void assertIdentity(long expected, CorrectnessSummary.Result r) {
        assertEquals(expected,
            r.sent() + r.skippedLate() + r.cancelled() + r.dead() + r.supersededBeforeSend() + r.unexplainedMissing(),
            "expected = sent + skipped + cancelled + dead + superseded-before-sendBy + unexplained");
        assertEquals(r.unexplainedMissing(), r.supersededAfterSendBy() + r.neverSupersededNoOutcome(),
            "unexplained = superseded-after-sendBy + never-superseded-no-outcome");
    }
}
```

  Replace `ReportTest.java` with:

```java
package com.quince.cartrecovery.loadgen;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportTest {
    private static LoadTestSummary sample() {
        return sample(false, false);
    }

    private static LoadTestSummary sample(boolean watermarkEverStale, boolean finalReadTimedOut) {
        return new LoadTestSummary(
            "run-abc123",
            Instant.parse("2026-09-26T10:00:00Z"),
            Instant.parse("2026-09-26T10:05:12Z"),
            5000.0, 4870.3,
            300L, 305L,
            Map.of("detector", 12000L, "dispatcher-fast", 300L),
            Map.of("detector", 0L, "dispatcher-fast", 0L),
            "detector",
            420, watermarkEverStale, 7,
            Map.of("fast", new Percentiles.Result(800, 1500, 2200, 4000),
                   "slow", new Percentiles.Result(900, 1600, 2400, 500)),
            10000, 9985, 5, 3, 2,
            0, 0,
            4, 1, 2,
            new MissingBreakdown.Result(5, 0, 3),
            6,
            3, 0.0003,
            finalReadTimedOut,
            8, 16L * 1024 * 1024 * 1024);
    }

    @Test void rendersEveryFieldFromSpec85() {
        String md = Report.render(sample());

        assertTrue(md.contains("run-abc123"));
        assertTrue(md.contains("5000.0"));
        assertTrue(md.contains("4870.3"));
        assertTrue(md.contains("Bottleneck stage: **detector**"));
        assertTrue(md.contains("Max per-partition watermark lag: 420 ms"));
        assertTrue(md.contains("Max Redis timer backlog past due: 7"));
        assertTrue(md.contains("SENT > DEAD > CANCELLED > SKIPPED_LATE > SUPERSEDED"));
        assertTrue(md.contains("| 10000 | 9985 | 5 | 3 | 2 | 5 |"), "superseded column = before + after sendBy");
        assertTrue(md.contains("Duplicate sends: **0**"));
        assertTrue(md.contains("Post-purchase sends: **0**"));
        assertTrue(md.contains("Superseded before sendBy ("));
        assertTrue(md.contains("excluded from unexplained missing): 4\n"));
        assertTrue(md.contains("Superseded after sendBy (pure lateness"));
        assertTrue(md.contains("sendBy): 1\n"), "supersededAfterSendBy value must render");
        assertTrue(md.contains("Never superseded, no outcome ("));
        assertTrue(md.contains("possible silent loss): 2\n"), "neverSupersededNoOutcome value must render");
        assertTrue(md.contains("Outcomes on non-expected keys"));
        assertTrue(md.contains("): 6\n"), "outcomesOnNonExpectedKeys value must render");
        assertTrue(md.contains("never superseded, no outcome): 3 ("));
        assertTrue(md.contains("Script inference cross-check"));
        assertTrue(md.contains("superseded before send 5, superseded after sendBy 0, never superseded 3"));
        assertTrue(md.contains("305 s actual publish span"));
        assertTrue(md.contains("nominal DURATION was 300 s"));
        assertTrue(md.contains("Cores: 8"));
        assertTrue(md.contains("shares this machine"));
        assertTrue(md.contains("loadgen JVM's own max heap"));
    }

    @Test void aStaleWatermarkRendersAsStaleNotAsAMeaninglessMsFigure() {
        assertTrue(Report.render(sample(true, false)).contains("stale"));
    }

    @Test void aTimedOutFinalReadIsFlaggedInTheReport() {
        assertTrue(Report.render(sample(false, true)).contains("did not reach sink-sends'/reminder-outcomes' end offsets in time"));
    }

    @Test void writeCreatesATimestampedMarkdownFileUnderTheReportsDir(@TempDir Path tempDir) throws IOException {
        Path reportsDir = tempDir.resolve("build/reports/load");

        Path written = Report.write(sample(), reportsDir);

        assertTrue(Files.exists(written));
        assertTrue(written.getFileName().toString().endsWith(".md"));
        assertEquals(reportsDir, written.getParent());
        assertTrue(Files.readString(written).contains("run-abc123"));
    }
}
```

- [ ] **Step 2: Run the tests and watch them fail.** Run `./gradlew test --tests 'com.quince.cartrecovery.loadgen.*'`. Expected: FAIL at `compileTestJava` with "cannot find symbol" for `supersededAt`, `sendBy`, `scriptInference` and the six-argument `compute`.

- [ ] **Step 3: Implement `Accounting`.**
  1. Replace `    private static final List<String> PRECEDENCE = List.of("SENT", "DEAD", "CANCELLED", "SKIPPED_LATE");` with `    private static final List<String> PRECEDENCE = List.of("SENT", "DEAD", "CANCELLED", "SKIPPED_LATE", "SUPERSEDED");`.
  2. Replace the Javadoc line `    /** Each key resolves to exactly one kind, by precedence SENT > DEAD > CANCELLED > SKIPPED_LATE. Rows with a null key ({@code ABANDONED}) are dropped. */` with `    /** Each key resolves to exactly one kind, by precedence SENT > DEAD > CANCELLED > SKIPPED_LATE > SUPERSEDED. Rows with a null key ({@code ABANDONED}) are dropped. */`.
  3. Replace the Javadoc of `unexplainedMissing` (lines 78–86) with:

```java
    /**
     * expected - sent - skippedLate - cancelled - dead - supersededBeforeSend. A deliberate deviation from spec §8.5's
     * plain formula: a key resolved SUPERSEDED at or before its own sendBy is a correct non-send (the cart moved on
     * first), so it is excluded. A key superseded only after its sendBy, or with no outcome at all, stays inside.
     */
```

  4. Add after `countByKind`:

```java
    /** Earliest SUPERSEDED time per key (review fix 2: the detector records one per displaced, owed reminder). */
    public static Map<String, Instant> supersededAt(List<OutcomeRow> outcomes) {
        Map<String, Instant> at = new HashMap<>();
        for (OutcomeRow row : outcomes) {
            if (row.key() != null && "SUPERSEDED".equals(row.kind())) {
                at.merge(row.key(), row.at(), (a, b) -> a.isBefore(b) ? a : b);
            }
        }
        return at;
    }
```

- [ ] **Step 4: Implement `Expected`.** Replace the file with:

```java
package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Computes, from each cart's script alone, the set of reminder keys ({@code cartId:version:offsetIndex},
 * the same format as the real ledger key) a correct pipeline should send: holdout carts send nothing;
 * within a cycle, offset {@code i} is expected unless something (a resume starting the next cycle, or
 * the cart's purchase on the last cycle) cancelled it at or before its due time; a cycle beyond the
 * configured frequency cap sends nothing.
 */
public final class Expected {
    private Expected() {}

    public static Set<String> keys(List<CartScript> scripts, RecoveryConfig config, ArmAssigner assigner) {
        return new LinkedHashSet<>(sendBy(scripts, config, assigner).keySet());
    }

    /** Every expected key with its nominal sendBy: the offset's scheduled time plus its lateness bound. */
    public static Map<String, Instant> sendBy(List<CartScript> scripts, RecoveryConfig config, ArmAssigner assigner) {
        Map<String, Instant> out = new LinkedHashMap<>();
        for (CartScript script : scripts) {
            if (assigner.assign(script.shopperKey()) == Arm.HOLDOUT) continue;
            List<Cycle> cycles = script.cycles();
            for (int cycleIndex = 0; cycleIndex < cycles.size() && cycleIndex < config.frequencyCap(); cycleIndex++) {
                Cycle cycle = cycles.get(cycleIndex);
                for (int i = 0; i < config.offsets().size(); i++) {
                    Instant dueAt = cycle.lastActivityAt().plus(config.offsets().get(i));
                    if (cycle.cancelledAt() != null && !cycle.cancelledAt().isAfter(dueAt)) break;
                    out.put(script.cartId() + ":" + cycle.version() + ":" + i, dueAt.plus(config.latenessBounds().get(i)));
                }
            }
        }
        return out;
    }
}
```

- [ ] **Step 5: Implement `CorrectnessSummary`.** Replace the file with:

```java
package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Composes {@link Accounting} into the numbers the load test report needs, from outcomes (review fix 2). Every kind
 * count is restricted to {@code expectedKeys} first (fix round 2), so an outcome on a key {@link Expected} never counted
 * cannot cancel out a real miss. A key resolved SUPERSEDED at or before its sendBy is a correct non-send; after its
 * sendBy it is a lateness miss. The identity holds key for key:
 * {@code expected = sent + skippedLate + cancelled + dead + supersededBeforeSend + unexplainedMissing}, and
 * {@code unexplainedMissing = supersededAfterSendBy + neverSupersededNoOutcome}. {@link MissingBreakdown}'s script
 * inference, over the keys with no SENT, SKIPPED_LATE, CANCELLED or DEAD outcome, is reported beside it as a cross-check.
 */
public final class CorrectnessSummary {
    private CorrectnessSummary() {}

    public record Result(long sent, long skippedLate, long cancelled, long dead, long supersededBeforeSend,
                          long supersededAfterSendBy, long neverSupersededNoOutcome, long unexplainedMissing,
                          double unexplainedMissingRatio, long outcomesOnNonExpectedKeys,
                          MissingBreakdown.Result scriptInference) {}

    /**
     * @param expectedKeys {@code Expected.keys(scripts, config, assigner)}
     * @param shift        real minus nominal time: the loadgen replays the script shifted so its first event lands at the
     *                     test start, and a SUPERSEDED outcome carries the superseding event's real time
     */
    public static Result compute(Set<String> expectedKeys, List<OutcomeRow> outcomes, List<CartScript> scripts,
                                  RecoveryConfig config, ArmAssigner assigner, Duration shift) {
        Map<String, String> resolved = Accounting.resolveOutcomes(outcomes);
        Map<String, String> onExpected = Accounting.restrictToKeys(resolved, expectedKeys);

        long sent = Accounting.countByKind(onExpected, "SENT");
        long skippedLate = Accounting.countByKind(onExpected, "SKIPPED_LATE");
        long cancelled = Accounting.countByKind(onExpected, "CANCELLED");
        long dead = Accounting.countByKind(onExpected, "DEAD");
        long outcomesOnNonExpectedKeys = resolved.size() - onExpected.size();

        Map<String, Instant> sendBy = Expected.sendBy(scripts, config, assigner);
        Map<String, Instant> supersededAt = Accounting.supersededAt(outcomes);
        long before = 0;
        long after = 0;
        for (Map.Entry<String, String> e : onExpected.entrySet()) {
            if (!"SUPERSEDED".equals(e.getValue())) continue;
            if (supersededAt.get(e.getKey()).isAfter(sendBy.get(e.getKey()).plus(shift))) after++;
            else before++;
        }
        long neverSupersededNoOutcome = expectedKeys.size() - onExpected.size();
        long unexplainedMissing = Accounting.unexplainedMissing(expectedKeys.size(), sent, skippedLate, cancelled, dead, before);
        double ratio = Accounting.unexplainedMissingRatio(unexplainedMissing, expectedKeys.size());

        Set<String> otherOutcome = onExpected.entrySet().stream().filter(e -> !"SUPERSEDED".equals(e.getValue()))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
        MissingBreakdown.Result inferred = MissingBreakdown.compute(scripts, config, assigner, otherOutcome);

        return new Result(sent, skippedLate, cancelled, dead, before, after, neverSupersededNoOutcome,
            unexplainedMissing, ratio, outcomesOnNonExpectedKeys, inferred);
    }
}
```

- [ ] **Step 6: Implement the summary, report and role.**

  In `LoadTestSummary.java`, after the line `    long neverSupersededNoOutcome,` insert:

```java
    /** Review fix 2: the pre-outcome script inference (MissingBreakdown), kept as a cross-check. */
    MissingBreakdown.Result scriptInference,
```

  In `Report.java`, replace everything from `        md.append("## Outcomes\n\n");` through the line ending `.append("% of expected)\n\n");` (lines 54–74) with:

```java
        md.append("## Outcomes\n\n");
        md.append("Every count below is resolved from `reminder-outcomes`, one kind per key by precedence ")
          .append("SENT > DEAD > CANCELLED > SKIPPED_LATE > SUPERSEDED, and counted only over expected keys (fix round 2); ")
          .append("see \"Outcomes on non-expected keys\" for the rest.\n\n");
        md.append("| Expected | Sent | Skipped late | Cancelled | Dead | Superseded |\n|---|---|---|---|---|---|\n");
        md.append("| ").append(s.expectedSends()).append(" | ").append(s.sentSends()).append(" | ")
          .append(s.skippedLate()).append(" | ").append(s.cancelled()).append(" | ").append(s.dead()).append(" | ")
          .append(s.supersededBeforeSend() + s.supersededAfterSendBy()).append(" |\n\n");

        md.append("## Correctness at the sink\n\n");
        md.append("- Duplicate sends: **").append(s.duplicateSends()).append("**\n");
        md.append("- Post-purchase sends: **").append(s.postPurchaseSends()).append("**\n");
        md.append("- Superseded before sendBy (a SUPERSEDED outcome at or before that offset's sendBy: the cart moved on ")
          .append("first — a correct non-send, excluded from unexplained missing): ").append(s.supersededBeforeSend()).append("\n");
        md.append("- Superseded after sendBy (pure lateness: a SUPERSEDED outcome only after that offset's sendBy): ")
          .append(s.supersededAfterSendBy()).append("\n");
        md.append("- Never superseded, no outcome (an expected key with no outcome at all — possible silent loss): ")
          .append(s.neverSupersededNoOutcome()).append("\n");
        md.append("- Outcomes on non-expected keys (an outcome recorded for a key Expected never counted — e.g. a ")
          .append("real-vs-nominal timing edge at a cycle boundary; excluded from every count above so it can't ")
          .append("silently cancel out a real miss): ").append(s.outcomesOnNonExpectedKeys()).append("\n");
        md.append("- Unexplained missing (superseded after sendBy + never superseded, no outcome): ").append(s.unexplainedMissing())
          .append(" (").append(String.format("%.4f", s.unexplainedMissingRatio() * 100)).append("% of expected)\n");
        MissingBreakdown.Result inferred = s.scriptInference();
        md.append("- Script inference cross-check (the pre-outcome method, from the workload script alone, over keys with ")
          .append("no SENT, SKIPPED_LATE, CANCELLED or DEAD outcome): superseded before send ").append(inferred.supersededBeforeSend())
          .append(", superseded after sendBy ").append(inferred.supersededAfterSendBy())
          .append(", never superseded ").append(inferred.neverSupersededNoOutcome()).append("\n\n");
```

  In `LoadgenRole.java`:
  1. Replace

```java
                writeReport(config, health, runPrefix, rate, duration, testStart, publishFinished.get(), workload,
                    expectedKeys, assigner, purchaseAtByCart, sampler, collector, finalReadCaughtUp.get());
```

     with

```java
                writeReport(config, health, runPrefix, rate, duration, testStart, publishFinished.get(), workload,
                    expectedKeys, assigner, purchaseAtByCart, sampler, collector, finalReadCaughtUp.get(), shift);
```

  2. Replace `                              OutcomeCollector collector, boolean finalReadCaughtUp) {` with `                              OutcomeCollector collector, boolean finalReadCaughtUp, Duration shift) {`.
  3. Replace

```java
        CorrectnessSummary.Result correctness = CorrectnessSummary.compute(expectedKeys, outcomes, workload.scripts(),
            config.recovery(), assigner);
```

     with

```java
        CorrectnessSummary.Result correctness = CorrectnessSummary.compute(expectedKeys, outcomes, workload.scripts(),
            config.recovery(), assigner, shift);
```

  4. Replace

```java
            correctness.neverSupersededNoOutcome(),
            correctness.outcomesOnNonExpectedKeys(), correctness.unexplainedMissing(), correctness.unexplainedMissingRatio(),
```

     with

```java
            correctness.neverSupersededNoOutcome(), correctness.scriptInference(),
            correctness.outcomesOnNonExpectedKeys(), correctness.unexplainedMissing(), correctness.unexplainedMissingRatio(),
```

- [ ] **Step 7: Run the tests and watch them pass.** Run `./gradlew test --tests 'com.quince.cartrecovery.loadgen.*'`, then `./gradlew test`. Expected: PASS, BUILD SUCCESSFUL (`MissingBreakdownTest` is unchanged and green).

- [ ] **Step 8: Commit.**

```bash
git add src/main/java/com/quince/cartrecovery/loadgen/ src/test/java/com/quince/cartrecovery/loadgen/
git commit -m "$(cat <<'EOF'
Resolve loadgen accounting from outcomes, keeping script inference as a cross-check

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Return unused send tokens

**Files:**
- Modify: `src/main/java/com/quince/cartrecovery/ports/SendBudget.java` (whole file)
- Modify: `src/main/java/com/quince/cartrecovery/app/TokenBucket.java` (add `release` after `tryAcquire`, lines 32–38)
- Modify: `src/main/java/com/quince/cartrecovery/inmemory/UnlimitedSendBudget.java:7`
- Modify: `src/main/java/com/quince/cartrecovery/core/Dispatcher.java:34-39` (Javadoc), `:83-128` (`handle`, `retryDue`), `:141-185` (`attempt`), plus a new `refund` method
- Modify: `src/main/java/com/quince/cartrecovery/Pipeline.java:77-80,219-220` plus an import
- Modify: `src/main/java/com/quince/cartrecovery/app/DispatcherRole.java:162-163` (`GateHolds` Javadoc)
- Modify: `docs/superpowers/plans/2026-09-26-production-infra.md` §1.2 (`SendBudget`)
- Test: `src/test/java/com/quince/cartrecovery/core/DispatcherTest.java`, `src/test/java/com/quince/cartrecovery/app/TokenBucketTest.java`, `src/test/java/com/quince/cartrecovery/FakeClockVerifierTest.java` (scenario 16)

**Interfaces:**
- Produces:
  - `void SendBudget.release(Lane lane)`
  - metrics `dispatch.token_taken` and `dispatch.token_refunded` (Task 9 reads both)
  - `Dispatcher.attempt` now returns `boolean` (true only if the sink was called); it is private.

- [ ] **Step 1: Write the failing tests.**

  `TokenBucketTest.java`, add:

```java
    @Test
    void releaseNeverRaisesTheBucketAboveCapacity() {
        bucket.release(Lane.FAST);                     // full bucket: stays at 10
        int n = 0;
        while (bucket.tryAcquire(Lane.FAST)) n++;
        assertEquals(10, n);

        bucket.release(Lane.FAST);
        assertTrue(bucket.tryAcquire(Lane.FAST));
        assertFalse(bucket.tryAcquire(Lane.FAST));
    }
```

  `DispatcherTest.java`:

  1. Replace

```java
    private final List<Lane> tokens = new ArrayList<>();
    private boolean tokensAvailable = true;
    private final SendBudget budget = lane -> {
        if (!tokensAvailable) return false;
        tokens.add(lane);
        return true;
    };
```

     with

```java
    private final List<Lane> tokens = new ArrayList<>();
    private final List<Lane> refunds = new ArrayList<>();
    private boolean tokensAvailable = true;
    private final SendBudget budget = new SendBudget() {
        @Override public boolean tryAcquire(Lane lane) {
            if (!tokensAvailable) return false;
            tokens.add(lane);
            return true;
        }
        @Override public void release(Lane lane) { refunds.add(lane); }
    };
```

  2. Replace

```java
        assertEquals(1, metrics.get("dispatch.sent"));
        assertEquals(1, metrics.get("dispatch.duplicate"));
```

     with

```java
        assertEquals(1, metrics.get("dispatch.sent"));
        assertEquals(1, metrics.get("dispatch.duplicate"));
        assertEquals(List.of(Lane.FAST, Lane.FAST), tokens);
        assertEquals(List.of(Lane.FAST), refunds, "the duplicate returns its token");
```

  3. Replace

```java
        assertEquals(List.of(OutcomeKind.CANCELLED), outcomeKinds());
        assertEquals(1, metrics.get("dispatch.cancelled"));
```

     with

```java
        assertEquals(List.of(OutcomeKind.CANCELLED), outcomeKinds());
        assertEquals(1, metrics.get("dispatch.cancelled"));
        assertEquals(List.of(Lane.FAST), refunds, "a cancel after the cart re-read returns the token");
        assertEquals(1, metrics.get("dispatch.token_taken"));
        assertEquals(1, metrics.get("dispatch.token_refunded"));
```

  4. Replace

```java
        dispatcher.retryDue(SHARD, 10);

        assertEquals(0, sink.sent().size());
        assertEquals(Optional.of("CANCELLED"), ledger.status(KEY));
        assertEquals(1, metrics.get("dispatch.cancelled"));
```

     with

```java
        dispatcher.retryDue(SHARD, 10);

        assertEquals(0, sink.sent().size());
        assertEquals(Optional.of("CANCELLED"), ledger.status(KEY));
        assertEquals(1, metrics.get("dispatch.cancelled"));
        assertEquals(List.of(Lane.FAST, Lane.FAST), tokens);
        assertEquals(List.of(Lane.FAST), refunds, "the cancelled retry returns its token");
```

  5. Replace

```java
        assertEquals(List.of(Lane.FAST), tokens);
        assertEquals(0, ledger.size());
        assertEquals(0, sink.attempts());
        assertEquals(1, metrics.get("dispatch.held"));
```

     with

```java
        assertEquals(List.of(Lane.FAST), tokens);
        assertEquals(List.of(Lane.FAST), refunds, "the gate hold returns the token");
        assertEquals(0, ledger.size());
        assertEquals(0, sink.attempts());
        assertEquals(1, metrics.get("dispatch.held"));
```

  6. Replace

```java
        assertEquals(0, sink.attempts());
        assertEquals(1, metrics.get("dispatch.lease_expiring"));
```

     with

```java
        assertEquals(0, sink.attempts());
        assertEquals(1, metrics.get("dispatch.lease_expiring"));
        assertEquals(List.of(Lane.FAST), refunds, "a lease too short to send returns the token");
```

  7. Replace

```java
            dispatcher(RecoveryConfig.defaults(), store, ledger, sink, down, dlq, () -> 1.0).handle(intent(0)));
        assertEquals(Optional.of("SENDING"), ledger.status(KEY));
```

     with

```java
            dispatcher(RecoveryConfig.defaults(), store, ledger, sink, down, dlq, () -> 1.0).handle(intent(0)));
        assertEquals(Optional.of("SENDING"), ledger.status(KEY));
        assertEquals(List.of(), refunds, "a token is never returned after a send");
```

  8. Add before the closing brace:

```java
    @Test
    void aReminderFoundLateAfterTheCartReadReturnsTheToken() {
        CartStateStore slowRead = (CartStateStore) Proxy.newProxyInstance(CartStateStore.class.getClassLoader(),
            new Class<?>[] {CartStateStore.class}, (proxy, method, args) -> {
                if (method.getName().equals("get")) clock.advance(Duration.ofMinutes(6));
                return method.invoke(store, args);
            });

        dispatcher(RecoveryConfig.defaults(), slowRead, ledger, sink, outcomes, dlq, () -> 1.0).handle(intent(0));

        assertEquals(Optional.of("SKIPPED_LATE"), ledger.status(KEY));
        assertEquals(List.of(Lane.FAST), refunds);
    }

    @Test
    void aFailedSendKeepsItsToken() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);

        dispatcher.handle(intent(0));

        assertEquals(List.of(Lane.FAST), tokens);
        assertEquals(List.of(), refunds);
    }
```

  `FakeClockVerifierTest.java`, scenario 16: replace

```java
        assertEquals(Optional.of("CANCELLED"), p.ledger().status("cart-1:1:1"));
```

  with

```java
        assertEquals(Optional.of("CANCELLED"), p.ledger().status("cart-1:1:1"));
        assertEquals(1, p.tokensTaken(), "every held and cancelled attempt returned its token; only the send kept one");
```

- [ ] **Step 2: Run the tests and watch them fail.** Run `./gradlew test --tests 'com.quince.cartrecovery.core.DispatcherTest' --tests 'com.quince.cartrecovery.app.TokenBucketTest' --tests 'com.quince.cartrecovery.FakeClockVerifierTest'`. Expected: FAIL at `compileTestJava`. The anonymous class's `release` overrides nothing, and `TokenBucket.release` is an unknown symbol.

- [ ] **Step 3: Change the port and the budgets.**

  Replace `SendBudget.java` with:

```java
package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.Lane;

public interface SendBudget {
    /** Takes one send token for the lane if available; never blocks. */
    boolean tryAcquire(Lane lane);

    /** Returns a token that tryAcquire gave and no send used (review fix 3); never exceeds the budget's capacity. */
    void release(Lane lane);
}
```

  In `TokenBucket.java`, add after `tryAcquire`:

```java
    /** Returns one unused token, capped at capacity (a refund after a refill cannot overfill the bucket). */
    @Override
    public synchronized void release(Lane lane) {
        refill();
        tokens = Math.min(capacity, tokens + 1);
    }
```

  In `UnlimitedSendBudget.java`, after the `tryAcquire` line add `    @Override public void release(Lane lane) { }`.

  In `Pipeline.java`, add `import com.quince.cartrecovery.model.Lane;`. Then replace

```java
        SendBudget budget = lane -> {
            tokensTaken++;
            return true;
        };
```

  with

```java
        SendBudget budget = new SendBudget() {
            @Override public boolean tryAcquire(Lane lane) {
                tokensTaken++;
                return true;
            }
            @Override public void release(Lane lane) { tokensTaken--; }
        };
```

  and replace `    /** Send tokens taken from the (unlimited) budget. */` with `    /** Send tokens taken from the (unlimited) budget and not returned: one per actual send attempt. */`.

- [ ] **Step 4: Implement the refunds in `Dispatcher`.**
  1. Replace the class Javadoc (lines 34–39) with:

```java
/**
 * Sends reminders at most once per key. Every attempt takes a send token, passes the watermark gate for the cart's
 * recorded partition, and holds a fenced ledger lease; sendBy is checked before the token, after the claim, and
 * immediately before the send. Only a send keeps its token (review fix 3): a gate hold, a lost claim, a cancel or late
 * skip after the cart re-read, and a lease too short for the gateway each return it. An exception keeps it, so a refund
 * never follows a send. Outcome and dead-letter records are produced before the ledger finish, so a crash in between
 * only duplicates records that consumers already resolve.
 */
```

  2. Replace the `handle` method with:

```java
    /** Spec §6.2 steps 1 to 7 for one consumed intent. HOLD asks the caller to pause the partition and redeliver. */
    public HandleResult handle(ReminderIntent intent) {
        if (clock.now().isAfter(intent.sendBy())) {
            outcome(intent.key(), OutcomeKind.SKIPPED_LATE, 0);
            metrics.increment("dispatch.skipped_late_precheck");
            return HandleResult.DONE;
        }
        Lane lane = Lane.of(intent.offsetIndex(), dispatch.fastOffsets());
        if (!budget.tryAcquire(lane)) {
            metrics.increment("dispatch.no_token");
            return HandleResult.HOLD;
        }
        metrics.increment("dispatch.token_taken");
        if (lagging(intent.srcPartition())) {
            refund(lane);
            metrics.increment("dispatch.held");
            return HandleResult.HOLD;
        }
        ClaimResult claim = ledger.claim(intent.key(), intent.sendBy(), intent.srcPartition(), clock.now());
        if (claim instanceof ClaimResult.Claimed c) {
            if (!attempt(intent.key(), c, intent.scheduledFor())) refund(lane);
        } else {
            refund(lane);
            metrics.increment("dispatch.duplicate");
        }
        return HandleResult.DONE;
    }
```

  3. Replace the body of `retryDue` (keep its Javadoc) with:

```java
        for (DueRetry due : ledger.dueRetries(shard, clock.now(), limit)) {
            if (lagging(due.srcPartition())) {
                metrics.increment("dispatch.retry_held");
                continue;
            }
            Lane lane = Lane.of(LedgerKey.parse(due.key()).offsetIndex(), dispatch.fastOffsets());
            if (!budget.tryAcquire(lane)) {
                metrics.increment("dispatch.retry_no_token");
                continue;
            }
            metrics.increment("dispatch.token_taken");
            ClaimResult claim = ledger.claim(due.key(), GONE, due.srcPartition(), clock.now());
            if (claim instanceof ClaimResult.Claimed c) {
                if (!attempt(due.key(), c, null)) refund(lane);
            } else {
                refund(lane);
                metrics.increment("dispatch.duplicate");
            }
        }
```

  4. Replace the `attempt` method with:

```java
    /**
     * Steps 5 to 7, holding the lease c; true only if the sink was called. scheduledFor is null on the retry path,
     * where it is rebuilt from the cart.
     */
    private boolean attempt(String key, ClaimResult.Claimed c, Instant scheduledFor) {
        if (clock.now().isAfter(c.sendBy())) {
            skipLate(key, c);
            return false;
        }
        LedgerKey k = LedgerKey.parse(key);
        Optional<CartRecord> cart = store.get(k.cartId());
        if (cart.isEmpty() || cart.get().version() != k.version() || cart.get().status() != CartStatus.ABANDONED) {
            outcome(key, OutcomeKind.CANCELLED, c.attempts());
            metrics.increment("dispatch.cancelled");
            finish(key, c, OutcomeKind.CANCELLED, "cancelled");
            return false;
        }
        Instant now = clock.now();
        if (now.isAfter(c.sendBy())) {
            skipLate(key, c);
            return false;
        }
        if (Duration.between(now, c.leaseUntil()).compareTo(dispatch.gatewayTimeout()) < 0) {
            metrics.increment("dispatch.lease_expiring");
            return false;
        }
        CartRecord r = cart.get();
        SendResult result = sink.send(new ReminderMessage(key, r.cartId(), r.shopperKey(), r.firstName(), r.items()));
        switch (result) {
            case SENT -> {
                outcome(key, OutcomeKind.SENT, c.attempts());
                metrics.increment("dispatch.sent");
                finish(key, c, OutcomeKind.SENT, null);
            }
            case TRANSIENT_FAILURE -> {
                if (c.attempts() < config.maxSendAttempts()) {
                    if (ledger.markRetry(key, c.token(), now.plus(backoff(c.attempts())))) {
                        metrics.increment("dispatch.retry");
                    } else {
                        metrics.increment("dispatch.lease_lost");
                    }
                } else {
                    deadLetter(key, c, r, scheduledFor, "retries_exhausted");
                }
            }
            case PERMANENT_FAILURE -> deadLetter(key, c, r, scheduledFor, "permanent_failure");
        }
        return true;
    }

    /** A token taken for this attempt that no send used goes back to the budget. */
    private void refund(Lane lane) {
        budget.release(lane);
        metrics.increment("dispatch.token_refunded");
    }
```

- [ ] **Step 5: Fix the `GateHolds` comment and the frozen contract.**
  - In `DispatcherRole.java`, replace `     * redelivered every poll timeout and spends a send token each time, since the Dispatcher takes the token first.` with `     * redelivered every poll timeout, taking and returning a send token each time (the Dispatcher takes the token first).`.
  - In the master plan §1.2, replace `interface SendBudget { boolean tryAcquire(Lane lane); }                                  // never blocks` with `interface SendBudget { boolean tryAcquire(Lane lane); void release(Lane lane); }        // never blocks; release returns an unused token, capped at capacity (review fixes 2026-09-28)`.

- [ ] **Step 6: Run the tests and watch them pass.** Run the command from Step 2, then `./gradlew test`. Expected: PASS, BUILD SUCCESSFUL.

- [ ] **Step 7: Commit.**

```bash
git add src/main/java/com/quince/cartrecovery/ports/SendBudget.java src/main/java/com/quince/cartrecovery/app/TokenBucket.java src/main/java/com/quince/cartrecovery/inmemory/UnlimitedSendBudget.java src/main/java/com/quince/cartrecovery/core/Dispatcher.java src/main/java/com/quince/cartrecovery/Pipeline.java src/main/java/com/quince/cartrecovery/app/DispatcherRole.java src/test/java/com/quince/cartrecovery/core/DispatcherTest.java src/test/java/com/quince/cartrecovery/app/TokenBucketTest.java src/test/java/com/quince/cartrecovery/FakeClockVerifierTest.java docs/superpowers/plans/2026-09-26-production-infra.md
git commit -m "$(cat <<'EOF'
Return send tokens on every dispatcher path that does not send

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: DueRetry carries sendBy; late retries cost nothing

**Files:**
- Modify: `src/main/java/com/quince/cartrecovery/model/DueRetry.java`
- Modify: `src/main/java/com/quince/cartrecovery/infra/dynamo/DynamoTables.java:81-82`
- Modify: `src/main/java/com/quince/cartrecovery/infra/dynamo/DynamoSendLedger.java:130`
- Modify: `src/main/java/com/quince/cartrecovery/inmemory/InMemorySendLedger.java:76`
- Modify: `src/main/java/com/quince/cartrecovery/core/Dispatcher.java` (`GONE`, lines 41–42, and `retryDue`)
- Modify: `docs/superpowers/plans/2026-09-26-production-infra.md` §1.1 (`DueRetry`)
- Test: `src/test/java/com/quince/cartrecovery/contract/SendLedgerContract.java` (all `new DueRetry(` sites plus one test)
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoSendLedgerTest.java` (all `new DueRetry(` sites)
- Test: `src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoTablesTest.java:71`
- Test: `src/test/java/com/quince/cartrecovery/core/DispatcherTest.java` (one test)

**Interfaces:**
- Consumes (Task 7): `Dispatcher.refund(Lane)`, `attempt(...)` returning `boolean`, `skipLate(String, ClaimResult.Claimed)`.
- Produces: `record DueRetry(String key, int srcPartition, Instant sendBy)`. The `retrying-by-shard` projection becomes INCLUDE `srcPartition`, `sendBy`.

- [ ] **Step 1: Write the failing tests.**
  1. Update the contract's `DueRetry` literals with `sed -i '' -E 's/new DueRetry\(([a-z]+), 3\)/new DueRetry(\1, 3, SEND_BY)/g' src/test/java/com/quince/cartrecovery/contract/SendLedgerContract.java`.
  2. Update the DynamoDB test's literals with `sed -i '' -E 's/new DueRetry\(key, 4\)/new DueRetry(key, 4, SEND_BY)/g' src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoSendLedgerTest.java`.
  3. Confirm with `grep -n 'new DueRetry(' src/test/java/com/quince/cartrecovery/contract/SendLedgerContract.java src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoSendLedgerTest.java`. Every hit must have three arguments.
  4. Add to `SendLedgerContract.java`:

```java
    @Test
    void dueRetriesCarryTheStoredSendBy() {
        String k = key("a", 1, 0);
        ledger.markRetry(k, claimed(k, NOW).token(), NOW);

        assertEquals(SEND_BY, due(k, NOW).get(0).sendBy());
    }
```

  5. In `DynamoTablesTest.java`, add `import java.util.Set;`. Then replace `        assertEquals(List.of("srcPartition"), gsi.projection().nonKeyAttributes());` with `        assertEquals(Set.of("srcPartition", "sendBy"), Set.copyOf(gsi.projection().nonKeyAttributes()));`.
  6. Add to `DispatcherTest.java`:

```java
    @Test
    void aLateRetryIsSkippedWithoutATokenOrTheGate() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);
        dispatcher.handle(intent(0));                  // retry due at 31m, sendBy 35m
        clock.set(at(min(36)));                        // late, and partition 0's watermark is now stale too

        dispatcher.retryDue(SHARD, 10);

        assertEquals(Optional.of("SKIPPED_LATE"), ledger.status(KEY));
        assertEquals(List.of(Lane.FAST), tokens, "only the first attempt took a token");
        assertEquals(List.of(), refunds);
        assertEquals(0, metrics.get("dispatch.retry_held"));
        assertEquals(List.of(new Outcome(KEY, CART, 1, Arm.TREATMENT, OutcomeKind.SKIPPED_LATE, at(min(36)), 2)),
            outcomes.all());
    }
```

- [ ] **Step 2: Run the tests and watch them fail.** Run `./gradlew test --tests 'com.quince.cartrecovery.inmemory.InMemorySendLedgerContractTest' --tests 'com.quince.cartrecovery.core.DispatcherTest'`. Expected: FAIL at `compileTestJava`, because no three-argument `DueRetry` constructor exists.

- [ ] **Step 3: Implement.**
  - Replace `DueRetry.java` with:

```java
package com.quince.cartrecovery.model;

import java.time.Instant;

/** A retry-index row: its key, the cart's source partition, and the row's sendBy (review fix 3: known before any token). */
public record DueRetry(String key, int srcPartition, Instant sendBy) {}
```

  - In `DynamoTables.java`, replace `                                .nonKeyAttributes("srcPartition").build())` with `                                .nonKeyAttributes("srcPartition", "sendBy").build())`.
  - In `DynamoSendLedger.java`, replace `                .map(row -> new DueRetry(keyOf(row), (int) num(row, "srcPartition")))` with `                .map(row -> new DueRetry(keyOf(row), (int) num(row, "srcPartition"), instant(row, "sendBy")))`.
  - In `InMemorySendLedger.java`, replace `            .map(e -> new DueRetry(e.getKey(), e.getValue().srcPartition()))` with `            .map(e -> new DueRetry(e.getKey(), e.getValue().srcPartition(), e.getValue().sendBy()))`.
  - In `Dispatcher.java`, delete these two lines:

```java
    /** Stands in for sendBy on a retry row that has disappeared: the claim then recreates it already late. */
    private static final Instant GONE = Instant.EPOCH;
```

  - Replace the whole `retryDue` method and its Javadoc with:

```java
    /**
     * One pass of the retry loop over a shard. A row already past its sendBy is claimed and skipped late with no token
     * and no gate. An on-time row passes the watermark gate, then takes a token (before the claim, so the loop never
     * holds a lease while waiting for capacity), then claims and runs steps 5 to 7, returning the token if it does not send.
     */
    public void retryDue(int shard, int limit) {
        for (DueRetry due : ledger.dueRetries(shard, clock.now(), limit)) {
            if (clock.now().isAfter(due.sendBy())) {
                ClaimResult claim = ledger.claim(due.key(), due.sendBy(), due.srcPartition(), clock.now());
                if (claim instanceof ClaimResult.Claimed c) {
                    skipLate(due.key(), c);
                } else {
                    metrics.increment("dispatch.duplicate");
                }
                continue;
            }
            if (lagging(due.srcPartition())) {
                metrics.increment("dispatch.retry_held");
                continue;
            }
            Lane lane = Lane.of(LedgerKey.parse(due.key()).offsetIndex(), dispatch.fastOffsets());
            if (!budget.tryAcquire(lane)) {
                metrics.increment("dispatch.retry_no_token");
                continue;
            }
            metrics.increment("dispatch.token_taken");
            ClaimResult claim = ledger.claim(due.key(), due.sendBy(), due.srcPartition(), clock.now());
            if (claim instanceof ClaimResult.Claimed c) {
                if (!attempt(due.key(), c, null)) refund(lane);
            } else {
                refund(lane);
                metrics.increment("dispatch.duplicate");
            }
        }
    }
```

  - Add one sentence to the class Javadoc after "each return it.": `A retry row already past its sendBy is claimed and skipped late with no token and no gate.`
  - In the master plan §1.1, replace `record DueRetry(String key, int srcPartition)` with `record DueRetry(String key, int srcPartition, Instant sendBy)   // sendBy: review fixes 2026-09-28; GSI projects it`.

- [ ] **Step 4: Run the tests and watch them pass.** Run `./gradlew test`. Expected: BUILD SUCCESSFUL (existing `aReplayPastSendByIsSkippedAsLate` and `aRetryLandingAfterSendByIsSkippedNotSent` still pass through the late path). With Docker, run `./gradlew integrationTest --tests '*DynamoSendLedger*' --tests '*DynamoTablesTest' --tests '*ReplayRoleIT'`. Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add src/main/java/com/quince/cartrecovery/model/DueRetry.java src/main/java/com/quince/cartrecovery/infra/dynamo/DynamoTables.java src/main/java/com/quince/cartrecovery/infra/dynamo/DynamoSendLedger.java src/main/java/com/quince/cartrecovery/inmemory/InMemorySendLedger.java src/main/java/com/quince/cartrecovery/core/Dispatcher.java src/test/java/com/quince/cartrecovery/contract/SendLedgerContract.java src/test/java/com/quince/cartrecovery/core/DispatcherTest.java src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoSendLedgerTest.java src/integrationTest/java/com/quince/cartrecovery/infra/dynamo/DynamoTablesTest.java docs/superpowers/plans/2026-09-26-production-infra.md
git commit -m "$(cat <<'EOF'
Carry sendBy in the retry index and skip late retries without a token

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Parallel retry pass

**Files:**
- Modify: `src/main/java/com/quince/cartrecovery/app/DispatcherRole.java` (Javadoc lines 35–39, imports, `controlLoop` lines 145–154, new `retryPass`)
- Test: `src/test/java/com/quince/cartrecovery/app/DispatcherRoleTest.java` (two tests)
- Test: `src/integrationTest/java/com/quince/cartrecovery/app/DispatcherRoleIT.java` (one assertion, one test)

**Interfaces:**
- Consumes (Task 7): metrics `dispatch.token_taken` and `dispatch.token_refunded`. Consumes (Task 8): `DueRetry.sendBy`.
- Produces: `static void DispatcherRole.retryPass(IntConsumer retryShard, int shards, Metrics metrics)`.

- [ ] **Step 1: Write the failing unit tests.** In `DispatcherRoleTest.java`, add the imports `import com.quince.cartrecovery.core.Metrics;`, `import java.util.Set;` and `import java.util.concurrent.ConcurrentHashMap;`, then add:

```java
    @Test
    void aRetryPassRunsEveryShardConcurrentlyAndWaitsForAll() {
        CountDownLatch allStarted = new CountDownLatch(8);
        Set<Integer> done = ConcurrentHashMap.newKeySet();
        DispatcherRole.retryPass(shard -> {
            allStarted.countDown();
            try {
                if (allStarted.await(1, TimeUnit.SECONDS)) done.add(shard);   // a serial pass never gets past shard 0 in time
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 8, new Metrics());
        assertEquals(Set.of(0, 1, 2, 3, 4, 5, 6, 7), done);
    }

    @Test
    void aFailingShardDoesNotStopTheOthers() {
        Set<Integer> done = ConcurrentHashMap.newKeySet();
        Metrics metrics = new Metrics();
        DispatcherRole.retryPass(shard -> {
            if (shard == 3) throw new IllegalStateException("dynamo unreachable");
            done.add(shard);
        }, 8, metrics);
        assertEquals(7, done.size());
        assertEquals(1, metrics.get("dispatch.retry_error"));
    }
```

- [ ] **Step 2: Run the tests and watch them fail.** Run `./gradlew test --tests 'com.quince.cartrecovery.app.DispatcherRoleTest'`. Expected: FAIL at `compileTestJava` with "cannot find symbol: method retryPass".

- [ ] **Step 3: Implement.** In `DispatcherRole.java`:
  1. Add the imports `import java.util.concurrent.ExecutorService;`, `import java.util.concurrent.Executors;` and `import java.util.function.IntConsumer;`, and delete `import java.util.concurrent.ThreadLocalRandom;`.
  2. In the class Javadoc, replace `loop that polls the ledger retry index every RETRY_POLL, re-checks gate-held partitions, and re-reads the` with `loop that runs a retry pass over every shard at once every RETRY_POLL, re-checks gate-held partitions, and re-reads the`.
  3. In `controlLoop`, replace

```java
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
```

     with

```java
            if (!open && !metaPaused.get()) {
                retryPass(shard -> dispatcher.retryDue(shard, RETRY_LIMIT), config.shards(), metrics);
            }
```

  4. Add after `controlLoop`:

```java
    /**
     * One retry pass (review fix 3): every shard's retryDue at once, one virtual thread each, returning when all have
     * finished, so a slow shard no longer delays the rest. The Dispatcher has no mutable state of its own. A shard that
     * throws is counted and does not stop the others.
     */
    static void retryPass(IntConsumer retryShard, int shards, Metrics metrics) {
        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int s = 0; s < shards; s++) {
                int shard = s;
                exec.submit(() -> {
                    try {
                        retryShard.accept(shard);
                    } catch (RuntimeException e) {
                        metrics.increment("dispatch.retry_error");
                    }
                });
            }
        }
    }
```

- [ ] **Step 4: Run the unit tests and watch them pass.** Run the command from Step 2, then `./gradlew test`. Expected: PASS.

- [ ] **Step 5: Extend `DispatcherRoleIT`.**

  Add these imports:
  - `com.quince.cartrecovery.infra.dynamo.DynamoSendLedger`
  - `com.quince.cartrecovery.infra.dynamo.DynamoTables`
  - `com.quince.cartrecovery.model.ClaimResult`
  - `com.quince.cartrecovery.model.Outcome`
  - `com.quince.cartrecovery.model.OutcomeKind`
  - `com.quince.cartrecovery.model.Shards`
  - `java.util.ArrayList`
  - `java.util.HashSet`
  - `java.util.Set`
  - `java.util.stream.Collectors`

  In `gateHeldPartitionStaysPausedAndSpendsNoTokensUntilCaughtUp`, replace

```java
            assertEquals(1, dispatcher.metrics().get("dispatch.held"), "held partition kept consuming tokens");
```

  with

```java
            assertEquals(1, dispatcher.metrics().get("dispatch.held"), "held partition kept being redelivered");
            assertEquals(dispatcher.metrics().get("dispatch.token_taken"), dispatcher.metrics().get("dispatch.token_refunded"),
                "a held partition's token spend stays flat");
```

  Add this test:

```java
    /** Review fix 3: the retry pass covers every shard; each seeded row is due at once and has no cart, so it cancels. */
    @Test
    void dueRetriesOnEveryShardResolve() throws Exception {
        InfraConfig c = config();
        String prefix = RoleInfra.prefix("retry");
        int src = 98;   // a source partition no detector owns; the test keeps its watermark current
        DynamoSendLedger ledger = new DynamoSendLedger(RoleInfra.ctx().dynamo(), DynamoTables.SEND_LEDGER,
            c.dispatch().lease(), c.shards());
        RedisWatermark watermark = new RedisWatermark(RoleInfra.ctx().redis(), c.partitions());
        Set<Integer> shards = new HashSet<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; shards.size() < c.shards(); i++) {
            String cart = prefix + i;
            if (!shards.add(Shards.of(cart, c.shards()))) continue;
            String key = new LedgerKey(cart, 1, 0).toString();
            Instant now = Instant.now();
            ClaimResult.Claimed claimed = (ClaimResult.Claimed) ledger.claim(key, now.plusSeconds(60), src, now);
            ledger.markRetry(key, claimed.token(), now);
            keys.add(key);
        }
        try (TopicTail outcomes = new TopicTail(RoleInfra.bootstrap(), Topics.OUTCOMES);
             RoleThread dispatcher = new RoleThread(new DispatcherRole(), c)) {
            Await.until(() -> {
                watermark.publish(src, 1, watermark.now());
                Set<String> cancelled = outcomes.records(prefix).stream().map(r -> JsonCodec.decodeOutcome(r.value()))
                    .filter(o -> o.kind() == OutcomeKind.CANCELLED).map(Outcome::key).collect(Collectors.toSet());
                return cancelled.containsAll(keys);
            }, WAIT);
            assertNull(dispatcher.failure());
        }
    }
```

- [ ] **Step 6: Run the IT.** With Docker, run `./gradlew integrationTest --tests '*DispatcherRoleIT'`. Expected: PASS, 4 tests, none skipped.

- [ ] **Step 7: Commit.**

```bash
git add src/main/java/com/quince/cartrecovery/app/DispatcherRole.java src/test/java/com/quince/cartrecovery/app/DispatcherRoleTest.java src/integrationTest/java/com/quince/cartrecovery/app/DispatcherRoleIT.java
git commit -m "$(cat <<'EOF'
Run the retry pass over all shards concurrently on virtual threads

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Verification, 250/s load re-run, and docs

**Files:**
- Create: `docs/load-reports/<timestamp>.md` (a copy of the new generated report plus a comparison section). Keep `docs/load-reports/2026-09-26T21-41-08.md` unchanged.
- Modify: `DESIGN.md` §6 (line 110), §11 (lines 185 and 187), §12.2 (lines 217–218, the gate table, line 229), §12.4 (line 252), §13 (lines 330–356)
- Modify: `README.md:65` (load-test accounting paragraph) and `:75` (`watermark.lag_ms.p<n>`)

**Interfaces:**
- Consumes: everything above, and the loadgen report format from Task 6.
- Produces: the recorded evidence and the docs.

- [ ] **Step 1: Run the full suite.**

```bash
export JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current
./gradlew test integrationTest --rerun-tasks 2>&1 | tee "$TMPDIR/review-fixes-verify.log"
grep -c ' SKIPPED' "$TMPDIR/review-fixes-verify.log"
grep -rl '<== monitors' build/test-results/ || echo "no pinning reports"
./gradlew run -q | head -5
```

  Expected results:
  - BUILD SUCCESSFUL.
  - The `SKIPPED` count is `0`.
  - The grep prints `no pinning reports`.
  - `./gradlew run` prints the first lines of the scripted timeline.

  If anything fails, stop and fix it in the task that owns the file before continuing.

- [ ] **Step 2: Re-run the 250/s load test.** The GSI changed, so recreate the local tables first. Export both variables so every compose command, including `run`, sees them.

```bash
cd /Users/vikas/Work/Interviews/quince
export COMPOSE_ENV_FILES=demo.env
export DYNAMO_STORAGE=-inMemory
docker compose --profile load down -v
docker compose up -d --build
docker compose ps --format '{{.Service}} {{.State}} {{.Health}}'
docker compose --profile load run --rm -e RATE=250 -e DURATION=PT5M loadgen 2>&1 | tee "$TMPDIR/load-250.log"
NEW=$(ls -t build/reports/load/*.md | head -1); echo "$NEW"
```

  - The `ps` step must show every role `running healthy` and `init` exited 0 before you continue.
  - The previous run took about 18 minutes.
  - Expected result: the log ends with `Report written to build/reports/load/<timestamp>.md`.
  - Do not tune configuration to improve the numbers. Record what the run shows.

- [ ] **Step 3: Record the report.** Copy it with `cp "$NEW" docs/load-reports/$(basename "$NEW")`. At the very top of the copied file, above `# Load test report:`, insert the section below. Fill each "After" cell from the lines of the same file named in the right-hand column.

```markdown
## Comparison with 2026-09-26T21-41-08 (before review fixes 1–3)

Same machine, same `demo.env` timings, same 250/s target. Between the runs: the watermark keeps a 35 s history and
publishes however far behind it is, the scheduler's hold cap is 5 s at these bounds instead of 60 s, unused send tokens
are returned, late retries take no token, the retry pass runs all shards at once, and superseded and reconciler-skipped
reminders now record outcomes.

| Measure | Before | After | Source line in this report |
|---|---|---|---|
| Skipped late | 9,860 of 43,635 (22.6%) | SKIPPED of EXPECTED (SKIPPED ÷ EXPECTED × 100, one decimal) | Outcomes table, "Skipped late" and "Expected" |
| Sent | 32,108 (73.6%) | SENT (SENT ÷ EXPECTED × 100) | Outcomes table, "Sent" |
| Superseded before sendBy | 640 (script inference) | value | "Superseded before sendBy" |
| Superseded after sendBy | not split (inside 917) | value | "Superseded after sendBy" |
| Never superseded, no outcome | not split (inside 917) | value | "Never superseded, no outcome" |
| Unexplained missing | 917 (2.1015%) | value (percentage as printed) | "Unexplained missing" |
| Script inference cross-check | n/a | the three values as printed | "Script inference cross-check" |
| Max watermark lag | 6,820 ms, a partition read stale | value, and whether "stale" is noted | "Max per-partition watermark lag" |
| Detector max lag | 1,415 | value | "Consumer lag by group", detector row |
```

  Replace every `value`, `SKIPPED`, `SENT` and `EXPECTED` with the numbers from the report, formatted with thousands separators. Below the table, add one paragraph that says plainly:
  - whether skipped-late fell well below 22.6%;
  - whether "never superseded, no outcome" is 0. If it is not 0, state the count, set it beside the script inference's "never superseded" figure, and name the known causes: the documented crash window between the detector's timer write and its outcome (no role crashed during the run unless the log shows a restart), and the boundary where a reminder due exactly at the superseding event is outside `Expected` and so lands in "Outcomes on non-expected keys";
  - whether unexplained missing met the 0.1% target.

- [ ] **Step 4: Edit `DESIGN.md` §6.** Replace the whole paragraph that starts `**Superseded reminders leave no outcome.**` with:

```markdown
**Superseded reminders are recorded.** A reminder whose cycle is superseded by a resume or purchase before its intent is published is deliberately not sent: an active shopper is never reminded. Since review fix 2 it still leaves an outcome. The timer store returns the timer each write displaced (`TimerStore.Upsert.displaced()`, and `remove`'s return value). When the detector's write displaces a `REMINDER(v, i)`, it records `SUPERSEDED` for every offset `j ≥ i` of that cycle due at or before the event's `occurredAt`. When it displaces an overdue `CHECK_ABANDON(v)`, it reads the cart once and does the same from offset 0, for a treatment cart the frequency cap would have allowed. A redelivered event's write is a no-op and displaces nothing, so it records nothing twice. A crash between the timer write and the outcome loses that one outcome, and the key then counts as unexplained. The reconciler records `SKIPPED_LATE` for each offset a rebuild skips as already past its lateness bound, only when its rebuild wrote, so once. The load test reads all of this from `reminder-outcomes`, resolving each key by precedence `SENT > DEAD > CANCELLED > SKIPPED_LATE > SUPERSEDED` over expected keys only. A key resolved `SUPERSEDED` at or before its `sendBy` is a correct non-send and is excluded from unexplained missing. One resolved after its `sendBy` is a lateness miss ("superseded after sendBy"), and an expected key with no outcome at all is "never superseded, no outcome". `CorrectnessSummary` and `Accounting.unexplainedMissing` still compute unexplained missing as expected − sent − skipped late − cancelled − dead − superseded before sendBy, which deliberately deviates from the production-infra spec's §8.5 formula. The script-based inference that used to supply these buckets (`MissingBreakdown`) is reported beside them as a cross-check.
```

- [ ] **Step 5: Edit `DESIGN.md` §11.**
  1. Replace the whole paragraph that starts `**Holding on a stale watermark.**` with:

```markdown
**Holding on a stale watermark.** When a timer's source partition reads as stale (`Instant.EPOCH`: never published, or silent for more than 5 s), `ReminderScheduler` cannot tell how far behind the detector is, so it releases the timer for `maxHold`. A known gap makes it hold only for that gap, clamped to 1 s and `maxHold`. `maxHold = max(1 s, min(60 s, smallest lateness bound ÷ 4))`, computed once from the configuration. That is 60 s at production timings (5 minute smallest bound) and 5 s with `demo.env`'s 20 s bound, so one hold can no longer outlast a whole lateness bound. Stale is now rare: a lagging or cut-off detector keeps publishing an older time rather than going silent (§12.2), so the scheduler usually sees the real gap. The fixed 60 s hold was the main cause of the first recorded load run's 22.6% skipped-late (§13).
```

  2. In the core-classes paragraph, replace `fenced ledger claim, cart re-check, send), runs the retry pass over a shard's due ledger rows (\`retryDue\`)` with `fenced ledger claim, cart re-check, send, returning the token on every path that does not send), runs the retry pass over a shard's due ledger rows (\`retryDue\`, which settles a row already past its \`sendBy\` without a token)`.

- [ ] **Step 6: Edit `DESIGN.md` §12.2.**
  1. Replace the sentence `A failed call takes no snapshot. The latest 8 snapshots are kept.` with `A failed call takes no snapshot. Two rings are kept: the latest 8 snapshots (dense), and the first snapshot of each second back to \`max(latenessBounds) + CLOCK_SKEW\` (sparse; about 1,800 entries at production bounds, 35 with \`demo.env\`).`
  2. Replace the whole `- **Publishing.**` bullet with:

```markdown
- **Publishing.** After every loop iteration, including empty polls, in-flight iterations and backoff, the detector publishes for each assigned partition the `T` of the newest retained snapshot, dense first then sparse, whose `E[p]` is at or below the committed position, however old. A detector that has fallen behind therefore reports "behind by X", and both gates hold in proportion, instead of going stale. A detector cut off from the broker takes no new snapshots and keeps republishing its last satisfied time, which is frozen. That is as safe as a stale entry, because a frozen watermark only makes both gates hold, and the lag stays visible. It deliberately deviates from the production-infra spec's §5.4 ("its watermark goes stale"), with the same effect on sends. Only a partition behind every retained snapshot, and by then past every lateness bound, writes nothing and goes stale. A dead detector still goes stale after 5 s, because nothing writes. For a partition this member has not committed yet, the broker's committed offset seeds the position, so an idle partition still counts as caught up. `/ready`'s `watermark.lag_ms.p<n>` is the latest Redis `TIME` the detector read minus the published time.
```

  3. In the gate table, replace `release the timer for \`clamp(dueAt + CLOCK_SKEW − W, 1 s, 60 s)\`, or for 60 s (\`MAX_HOLD\`) when \`W\` reads as \`EPOCH\`; counts \`timers.held\`` with `release the timer for \`clamp(dueAt + CLOCK_SKEW − W, 1 s, maxHold)\`, or for \`maxHold\` when \`W\` reads as \`EPOCH\` (§11); counts \`timers.held\``.
  4. In the same table, replace `; a retry row is skipped this round |` with `; a retry row is skipped this round. Either way the send token taken before the gate is returned |`.
  5. Replace `§11, "Holding on a stale watermark", explains why the 60 s hold costs sends at \`demo.env\`'s short lateness bounds.` with `§11, "Holding on a stale watermark", gives the scheduler's hold cap.`

- [ ] **Step 7: Edit `DESIGN.md` §12.4.** In the paragraph after the transition table, replace

```
(partition `retryShard`, sort `nextAttemptAt`, projecting `srcPartition`) holds exactly the rows a retry loop may take: `RETRYING` rows once due, and `SENDING` rows whose lease expired. `Dispatcher.retryDue` reads it (eventually consistent), checks the watermark gate, takes a send token *before* claiming so it never holds a lease while waiting for capacity, and then claims.
```

  with

```
(partition `retryShard`, sort `nextAttemptAt`, projecting `srcPartition` and `sendBy`) holds exactly the rows a retry loop may take: `RETRYING` rows once due, and `SENDING` rows whose lease expired. `Dispatcher.retryDue` reads it (eventually consistent). A row already past its `sendBy` is claimed and finished `SKIPPED_LATE` at once, with no token and no gate. An on-time row passes the watermark gate, takes a send token *before* claiming so it never holds a lease while waiting for capacity, and then claims. `DispatcherRole` runs every shard's pass at once on virtual threads and waits for all of them before the next `RETRY_POLL`. On the intent path and the retry path alike, only a send keeps its token. A gate hold, a `NotClaimed` claim, a cancel or late skip after the cart re-read, and a lease too short for `GATEWAY_TIMEOUT` each return it (`SendBudget.release`; `TokenBucket` caps the refund at its capacity), counted as `dispatch.token_refunded`. An exception keeps the token, so a refund never follows a send. DynamoDB cannot change a GSI's projection in place: locally the tables are recreated (`docker compose --profile load down -v`), and a deployed table would need a new index and a cutover.
```

- [ ] **Step 8: Edit `DESIGN.md` §13.** Replace everything from `## 13. Measured results and known limitations` up to, but not including, `## 14. Alternatives considered` with the text below. Fill the "After fixes" column from the new report, using the same rules as Step 3, and use the new file's name where `<NEW>` appears.

```markdown
## 13. Measured results and known limitations

The recorded runs are `docs/load-reports/2026-09-26T21-41-08.md` (before review fixes 1–3) and `docs/load-reports/<NEW>` (after). Every role and every container shared one 12-core laptop, so the figures are a floor for that machine, not a capacity figure for the design.

**Rate step-down.** 5,000 events/s (the §2 baseline) was aborted after about 90 s, with detector lag rising from about 33k to 242k. 1,000/s was rejected with lag still climbing past 21k. 500/s was rejected because lag trended upward without bound. 250/s held: lag peaked at about 1.4k and drained to 0. The detector was the bottleneck at every rate. At 250/s target, the achieved rate was 87.3 events/s over an 859 s publish span, because the workload's resume and purchase tails stretch the span well past the nominal 300 s.

**250/s against the §3 targets** (`demo.env` timings):

| Measure | Target | Before fixes | After fixes |
|---|---|---|---|
| Sent on time | 99.5% | 73.6% (32,108 of 43,635) | SENT% (SENT of EXPECTED) |
| Skipped late | counted, never sent | 22.6% (9,860), mostly the fixed 60 s stale hold exceeding the 20 to 30 s demo bounds | SKIPPED% (SKIPPED) |
| Superseded | counted, never sent | not recorded (640 inferred from the script) | before sendBy BEFORE, after sendBy AFTER |
| Never superseded, no outcome | 0 | not split | NONE |
| Unexplained missing | under 0.1% | 2.1% (917); 0.35% in an earlier run | UNEXPLAINED% (UNEXPLAINED) |
| Duplicate sends | under 0.01% | 0 | DUPLICATES |
| Post-purchase sends | under 0.01% | 0 | POSTPURCHASE |

State in one sentence whether the after-fix skipped-late and unexplained-missing figures meet their targets, citing the new report's comparison section.

**Known limitations.**

- Detector throughput limits the local stack to about 250 events/s, far below the 5,000/s baseline. It has not been measured on dedicated hardware.
- A crash between the detector's timer write and its `SUPERSEDED` outcome loses that outcome; the key then counts as "never superseded, no outcome" (§6).
- The send budget is per replica; there is no global send-rate limit across dispatcher replicas.
- The guardrails have no automatic thresholds. Pausing is a manual `recovery-meta.paused` flip (§3).
- How long a real gateway honours the idempotency key has not been checked. The two at-most-once exceptions depend on it (§15).
- Only DynamoDB Local has been tested, never real AWS. The code retries unprocessed `BatchGetItem` keys, but that path, GSI propagation lag and throttling have never been exercised against real DynamoDB.
```

  Replace `SENT%`, `SENT`, `EXPECTED`, `SKIPPED%`, `SKIPPED`, `BEFORE`, `AFTER`, `NONE`, `UNEXPLAINED%`, `UNEXPLAINED`, `DUPLICATES`, `POSTPURCHASE` and `<NEW>` with the values and file name from the new report. Replace the instruction sentence under the table with the one-sentence finding it asks for.

- [ ] **Step 9: Edit `README.md`.**
  1. Replace line 75 (the `watermark.lag_ms.p<n>` bullet) with:

```markdown
  - `watermark.lag_ms.p<n>` (detector): how far partition `n`'s published watermark trails Redis `TIME` (the latest the detector read). It rises while the detector is behind or cut off from the broker, since the detector keeps publishing its last satisfied time, and gated sends for that partition hold in proportion. `stale` means the partition is behind every retained snapshot (more than the largest lateness bound plus `CLOCK_SKEW`), so nothing is written, the entry goes stale after 5 s, and gated sends for that partition pause.
```

  2. In line 65, replace the text from `The load test reports achieved throughput,` through `elsewhere in the subtraction.` with:

```markdown
The load test reports achieved throughput, per-stage lag, send latency per lane, and correctness (duplicate and post-purchase sends, superseded before and after sendBy, never-superseded-no-outcome, unexplained missing) to the console and to `build/reports/load/<timestamp>.md`; `docs/load-reports/` holds the recorded runs. Every correctness count comes from `reminder-outcomes`, with each key resolved by precedence SENT > DEAD > CANCELLED > SKIPPED_LATE > SUPERSEDED. The detector records `SUPERSEDED` when a resume or purchase displaces a reminder that was already due, and the reconciler records `SKIPPED_LATE` for offsets a rebuild skips (DESIGN §6). Unexplained missing is expected minus sent minus skipped-late minus cancelled minus dead minus superseded-before-sendBy. That deliberately deviates from spec §8.5's plain formula, because a cart that moved on at or before an offset's send deadline was correctly not reminded. A key superseded only *after* its send deadline ("superseded after sendBy", pure lateness) or left with no outcome at all ("never superseded, no outcome", possible silent loss) stays inside unexplained missing. The two are reported on separate lines and sum to it exactly, key for key. The old script-based inference of those buckets is printed beside them as a cross-check. Sent, skipped-late, cancelled, dead and superseded are counted only over expected keys. An outcome recorded for a key nothing expected (a real-vs-nominal timing edge at a cycle boundary, say) is reported on its own line instead of being folded in, so it can never silently cancel out a real miss elsewhere in the subtraction.
```

  3. At the end of line 65 append ` After a change to the \`send-ledger\` retry index, recreate the local tables first with \`docker compose --profile load down -v\`.`

- [ ] **Step 10: Check the docs and tear down.**

```bash
grep -n 'MAX_HOLD\|leave no outcome\|| value |' DESIGN.md README.md docs/load-reports/$(basename "$NEW")
docker compose --profile load down -v
```

  Expected: no hits for `MAX_HOLD` or `leave no outcome`, and no unfilled `value` cells.

- [ ] **Step 11: Commit.**

```bash
git add DESIGN.md README.md docs/load-reports/
git commit -m "$(cat <<'EOF'
Record the 250/s re-run after review fixes 1-3 and update DESIGN and README

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```
