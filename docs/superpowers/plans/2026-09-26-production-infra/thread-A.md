# Thread A: core semantics and in-memory adapters (A1 to A4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The master plan (`docs/superpowers/plans/2026-09-26-production-infra.md`) holds the header, global constraints, and frozen contracts; the spec (`docs/superpowers/specs/2026-09-25-production-infra-design.md`) is the binding authority.

**Goal:** Reshape the model, ports, core classes, and in-memory adapters to the spec's semantics (fenced ledger, monotonic timers, per-partition watermark gate, `sendBy`, timer-first detector, key-diff reconciler), and prove in-memory parity with shared contract tests that thread B reuses on real infrastructure.

**Frozen contracts are binding.** Every name and signature in master §1.1 to §1.4 is implemented exactly as written. Where this file adds anything beyond them (an extra constructor overload, test-only accessors), it is additive and listed under "Contract issues" at the end.

**Why a temporary `legacy` package.** A1 must replace ports that every existing core class, `Pipeline`, and the verifier use (`TimerStore.popDue`, `CartStateStore.put`, `SendLedger.recordIfAbsent`, `Outbox`, `DeadLetter(NotificationIntent)`), yet `./gradlew test` must stay green after every task. A1 therefore moves the current in-memory pipeline, unchanged, into `com.quince.cartrecovery.legacy` (a `git mv` plus import rewrites), where it keeps running its existing 78 tests. A2 and A3 then create the new core classes at their real paths, and A4 creates the new `Pipeline`, moves the verifier and `PipelineTest` back, and deletes `legacy`. No throwaway logic is written; each task's diff is only its own scope.

**Commands.** Every Gradle command runs from the repository root as:
`JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests '<pattern>' --console=plain` (omit `--tests` for the full suite).

**Commit trailer.** Every commit message ends with a blank line and `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.

Paths below abbreviate `src/main/java/com/quince/cartrecovery/` as `main/` and `src/test/java/com/quince/cartrecovery/` as `test/`.

## File ownership

| Task | Creates | Modifies | Moves or deletes |
|---|---|---|---|
| **A1** | `main/model/{ReminderIntent,ReminderMessage,LedgerKey,OutcomeKind,Outcome,DeadLetter,Lane,DispatchConfig,Shards,ClaimResult,DueRetry,TimerDecision,HandleResult}.java`; `main/ports/{CartStateStore,TimerStore,Watermark,IntentPublisher,SendLedger,NotificationSink,OutcomeRecorder,DeadLetterQueue,SendBudget}.java` (new content at the moved files' paths); `main/inmemory/{InMemoryCartStateStore,PriorityQueueTimerStore,InMemoryWatermark,InMemoryIntentQueue,InMemorySendLedger,InMemoryOutcomeRecorder,RecordingNotificationSink,InMemoryDeadLetterQueue,UnlimitedSendBudget}.java` (new content at the moved files' paths); `test/contract/{CartStateStoreContract,TimerStoreContract,WatermarkContract,SendLedgerContract}.java`; `test/inmemory/{InMemoryCartStateStoreContractTest,PriorityQueueTimerStoreContractTest,InMemoryWatermarkContractTest,InMemorySendLedgerContractTest}.java`; `test/model/{LedgerKeyTest,ModelTest}.java`; `test/core/MetricsTest.java` | `main/model/{CartEvent,CartRecord,Timer}.java`; `main/core/Metrics.java`; `main/Main.java` (two import lines only) | `git mv` into `main/legacy/…`: `Pipeline`, `core/{AbandonmentDetector,ReminderScheduler,Dispatcher,Reconciler}`, `ports/{CartStateStore,TimerStore,SendLedger,NotificationSink,DeadLetterQueue,Outbox}`, `model/{DeadLetter,NotificationIntent,OutboxEntry}`, `inmemory/{InMemoryCartStateStore,PriorityQueueTimerStore,InMemorySendLedger,InMemoryOutbox,InMemoryDeadLetterQueue,RecordingNotificationSink}`; into `test/legacy/…`: `PipelineTest`, `FakeClockVerifierTest`, `core/{AbandonmentDetectorTest,ReminderSchedulerTest,DispatcherTest}`, `inmemory/{InMemoryCartStateStoreTest,PriorityQueueTimerStoreTest}` |
| **A2** | `main/core/{AbandonmentDetector,ReminderScheduler}.java`; `test/core/{AbandonmentDetectorTest,ReminderSchedulerTest}.java` | none | none |
| **A3** | `main/core/Dispatcher.java`; `test/core/DispatcherTest.java` | none | none |
| **A4** | `main/core/Reconciler.java`; `main/Pipeline.java`; `test/core/ReconcilerTest.java`; `test/PipelineTest.java`; `test/FakeClockVerifierTest.java` | `main/Main.java` | deletes `main/legacy/` and `test/legacy/` entirely |

Unchanged and shared by all tasks: `main/model/{Arm,CartItem,CartStatus,RecoveryConfig,SendResult,TimerKind}.java`, `main/ports/{Clock,ArmAssigner}.java`, `main/core/ReminderPolicy.java`, `main/inmemory/{FakeClock,HashArmAssigner}.java`, `test/TestSupport.java`, `test/core/ReminderPolicyTest.java`, `test/model/RecoveryConfigTest.java`.

---

### Task A1: Model, ports, in-memory adapters, thread-safe Metrics, shared contract tests

**Model:** opus (defines every semantic the other tasks build on). Reviewer: opus.

**Files:** see the ownership table (A1 row).

**Interfaces:**
- Consumes: T0's Gradle setup (JUnit 5 only; A1 needs no new dependency).
- Produces (exact, master §1.1 to §1.4):
  - Model: `CartEvent.CartEdited(String cartId, String shopperKey, long version, Instant occurredAt, List<CartItem> items, String firstName)` plus the 5-arg overload; `CartRecord(String cartId, String shopperKey, CartStatus status, long version, Instant lastActivityAt, List<CartItem> items, Arm arm, List<Instant> sequenceStarts, String firstName, int srcPartition)` plus the 8-arg overload, `activity` (caps items at 50), `List<Instant> startsWith(Instant start, Instant now, Duration frequencyWindow)`; `Timer(String cartId, TimerKind kind, long version, int offsetIndex, Instant dueAt, int srcPartition)` plus the 5-arg overload, `checkAbandon(cartId, version, dueAt, srcPartition)`, `reminder(cartId, version, offsetIndex, dueAt, srcPartition)` and the old 3/4-arg factories; `ReminderIntent(String key, String cartId, long version, int offsetIndex, int srcPartition, Instant scheduledFor, Instant sendBy)`; `ReminderMessage(String key, String cartId, String shopperKey, String firstName, List<CartItem> items)`; `LedgerKey(String cartId, long version, int offsetIndex)` with `toString()` and `static LedgerKey parse(String)`; `enum OutcomeKind { ABANDONED, SENT, SKIPPED_LATE, CANCELLED, DEAD }`; `Outcome(String key, String cartId, long version, Arm arm, OutcomeKind kind, Instant at, int attempts)`; `DeadLetter(ReminderIntent intent, String reason, Instant at)` with `REASON_POISON = "poison"`; `enum Lane { FAST, SLOW; static Lane of(int offsetIndex, int fastOffsets) }`; `DispatchConfig(Duration lease, Duration gatewayTimeout, Duration clockSkew, int fastOffsets)` with `defaults()`; `Shards.of(String cartId, int shards)`; `sealed interface ClaimResult { Claimed(String token, int attempts, Instant sendBy, int srcPartition, Instant leaseUntil); NotClaimed(String reason) }`; `DueRetry(String key, int srcPartition)`; `sealed interface TimerDecision { Ack(); Release(Duration delay) }`; `enum HandleResult { DONE, HOLD }`.
  - Ports: `CartStateStore`, `TimerStore`, `Watermark`, `IntentPublisher`, `SendLedger`, `NotificationSink`, `OutcomeRecorder`, `DeadLetterQueue`, `SendBudget` exactly as master §1.2.
  - In-memory: `InMemoryCartStateStore(RecoveryConfig config, int shards)`; `PriorityQueueTimerStore(Clock clock, Duration lease)` + `Optional<Instant> nextDueAt()`, `int size()`, `void clear()`; `InMemoryWatermark(Clock clock)` + `setLagging(int partition, Instant eventTime)`, `clearLag()`; `InMemoryIntentQueue` + `List<ReminderIntent> drain()`, `int size()`; `InMemorySendLedger(Duration lease, int shards)` + `Optional<Instant> nextRetryAt()`, `int size()`, `Optional<String> status(String key)`; `InMemoryOutcomeRecorder` + `List<Outcome> all()`; `RecordingNotificationSink(Clock)` with `record Sent(ReminderMessage message, Instant sentAt)`, `scriptOutcomes(SendResult...)`, `List<Sent> sent()`, `int attempts()`; `InMemoryDeadLetterQueue` + `List<DeadLetter> drain()`, `int size()`; `UnlimitedSendBudget`.
  - `Metrics`: same API (`increment`, `get`, `snapshot`), now thread-safe.
  - Contract tests (for thread B): `test/contract/CartStateStoreContract` (`protected abstract CartStateStore newStore(RecoveryConfig config, int shards)`), `TimerStoreContract` (`protected abstract TimerStore newStore(Duration lease)`, `protected abstract Instant now()`, `protected abstract void advance(Duration d)`; the store must use `TimerStoreContract.SHARDS` = 8 shards), `WatermarkContract` (`protected abstract Watermark newWatermark()`, `protected abstract void advance(Duration d)`), `SendLedgerContract` (`protected abstract SendLedger newLedger(Duration lease, int shards)`). `CartStateStore` and `SendLedger` take time as an argument, so their contracts need no time hook. All cart ids and keys carry a per-test random prefix, so a subclass may share one table or Redis across tests; B's factories must still return a watermark with no partitions and a timer store with no due timers from other tests.

- [ ] **Step 1: Move the current pipeline into the temporary `legacy` package**

Run this script from the repository root (it uses only `git mv` and `perl`, and edits nothing but package and import lines):

```bash
set -euo pipefail
M=src/main/java/com/quince/cartrecovery
T=src/test/java/com/quince/cartrecovery
mkdir -p $M/legacy/core $M/legacy/ports $M/legacy/model $M/legacy/inmemory $T/legacy/core $T/legacy/inmemory
for f in AbandonmentDetector ReminderScheduler Dispatcher Reconciler; do git mv $M/core/$f.java $M/legacy/core/$f.java; done
for f in CartStateStore TimerStore SendLedger NotificationSink DeadLetterQueue Outbox; do git mv $M/ports/$f.java $M/legacy/ports/$f.java; done
for f in DeadLetter NotificationIntent OutboxEntry; do git mv $M/model/$f.java $M/legacy/model/$f.java; done
for f in InMemoryCartStateStore PriorityQueueTimerStore InMemorySendLedger InMemoryOutbox InMemoryDeadLetterQueue RecordingNotificationSink; do git mv $M/inmemory/$f.java $M/legacy/inmemory/$f.java; done
git mv $M/Pipeline.java $M/legacy/Pipeline.java
git mv $T/PipelineTest.java $T/legacy/PipelineTest.java
git mv $T/FakeClockVerifierTest.java $T/legacy/FakeClockVerifierTest.java
for f in AbandonmentDetectorTest ReminderSchedulerTest DispatcherTest; do git mv $T/core/$f.java $T/legacy/core/$f.java; done
for f in InMemoryCartStateStoreTest PriorityQueueTimerStoreTest; do git mv $T/inmemory/$f.java $T/legacy/inmemory/$f.java; done
FILES=$(find $M/legacy $T/legacy -name '*.java')
perl -pi -e '
  s/^package com\.quince\.cartrecovery((?:\.\w+)?);/package com.quince.cartrecovery.legacy$1;/;
  s/^import com\.quince\.cartrecovery\.core\.(AbandonmentDetector|ReminderScheduler|Dispatcher|Reconciler);/import com.quince.cartrecovery.legacy.core.$1;/;
  s/^import com\.quince\.cartrecovery\.ports\.(CartStateStore|TimerStore|SendLedger|NotificationSink|DeadLetterQueue|Outbox);/import com.quince.cartrecovery.legacy.ports.$1;/;
  s/^import com\.quince\.cartrecovery\.model\.(DeadLetter|NotificationIntent|OutboxEntry);/import com.quince.cartrecovery.legacy.model.$1;/;
  s/^import com\.quince\.cartrecovery\.inmemory\.(InMemoryCartStateStore|PriorityQueueTimerStore|InMemorySendLedger|InMemoryOutbox|InMemoryDeadLetterQueue|RecordingNotificationSink);/import com.quince.cartrecovery.legacy.inmemory.$1;/;
' $FILES
# Classes that moved out of core still use Metrics and ReminderPolicy, which stay in core.
perl -0pi -e 's/^(package com\.quince\.cartrecovery\.legacy\.core;\n)/$1\nimport com.quince.cartrecovery.core.Metrics;\nimport com.quince.cartrecovery.core.ReminderPolicy;\n/m' $(find $M/legacy/core $T/legacy/core -name '*.java')
# NotificationIntent used CartItem from its old package.
perl -0pi -e 's/^(package com\.quince\.cartrecovery\.legacy\.model;\n)/$1\nimport com.quince.cartrecovery.model.CartItem;\n/m' $M/legacy/model/NotificationIntent.java
# Main keeps running the legacy pipeline until A4.
perl -pi -e '
  s/^import com\.quince\.cartrecovery\.inmemory\.RecordingNotificationSink;/import com.quince.cartrecovery.legacy.Pipeline;\nimport com.quince.cartrecovery.legacy.inmemory.RecordingNotificationSink;/;
' $M/Main.java
```

- [ ] **Step 2: Run the full suite to verify the move changed no behaviour**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`; the 78 existing tests pass, now reported under `com.quince.cartrecovery.legacy.*` (plus `ReminderPolicyTest` and `RecoveryConfigTest` in place).

- [ ] **Step 3: Commit the move**

```bash
git add -A src
git commit -m "$(cat <<'MSG'
Move the in-memory pipeline to a temporary legacy package

The production-infra ports replace the ones the current core uses. The old
pipeline keeps running its tests from com.quince.cartrecovery.legacy until
thread A rebuilds the core on the new ports; A4 deletes the package.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

- [ ] **Step 4: Write the failing model tests**

Create `test/model/LedgerKeyTest.java` (Review Focus 1: a cart id containing `:` round-trips):

```java
package com.quince.cartrecovery.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class LedgerKeyTest {

    @Test
    void formatsAsCartIdVersionOffset() {
        assertEquals("cart-1:3:0", new LedgerKey("cart-1", 3, 0).toString());
    }

    @Test
    void parsesFromTheRightSoACartIdMayContainColons() {
        LedgerKey k = new LedgerKey("shop:eu:cart:9", 12, 2);
        assertEquals(k, LedgerKey.parse(k.toString()));
        assertEquals(new LedgerKey("a:b", 1, 0), LedgerKey.parse("a:b:1:0"));
    }

    @Test
    void rejectsMalformedKeys() {
        assertThrows(IllegalArgumentException.class, () -> LedgerKey.parse("cart-1:3"));
        assertThrows(IllegalArgumentException.class, () -> LedgerKey.parse(":3:0"));
        assertThrows(IllegalArgumentException.class, () -> LedgerKey.parse("cart-1:x:0"));
        assertThrows(IllegalArgumentException.class, () -> LedgerKey.parse("cart-1:3:"));
    }
}
```

Create `test/model/ModelTest.java`:

```java
package com.quince.cartrecovery.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ModelTest {
    private static final Instant T0 = Instant.parse("2026-01-01T09:00:00Z");

    @Test
    void legacyConstructorsDefaultTheNewComponents() {
        CartRecord r = new CartRecord("c", "u", CartStatus.ACTIVE, 1, T0, List.of(), Arm.TREATMENT, List.of());
        assertNull(r.firstName());
        assertEquals(-1, r.srcPartition());
        assertEquals(-1, Timer.checkAbandon("c", 1, T0).srcPartition());
        assertEquals(-1, Timer.reminder("c", 1, 0, T0).srcPartition());
        assertNull(new CartEvent.CartEdited("c", "u", 1, T0, List.of()).firstName());
    }

    @Test
    void activityCapsItemsAtFiftyAndKeepsNameAndPartition() {
        List<CartItem> many = IntStream.range(0, 60).mapToObj(i -> new CartItem("S" + i, "n", 1, 1)).toList();
        CartRecord r = new CartRecord("c", "u", CartStatus.ACTIVE, 1, T0, List.of(), Arm.TREATMENT, List.of(), "Ada", 4)
            .activity(2, T0, many);
        assertEquals(50, r.items().size());
        assertEquals("Ada", r.firstName());
        assertEquals(4, r.srcPartition());
    }

    @Test
    void startsWithPrunesToTheFrequencyWindowAndAppendsTheStart() {
        Instant now = T0.plus(Duration.ofDays(10));
        CartRecord r = new CartRecord("c", "u", CartStatus.ACTIVE, 1, now, List.of(), Arm.TREATMENT,
            List.of(now.minus(Duration.ofDays(8)), now.minus(Duration.ofDays(7)), now.minus(Duration.ofDays(1))));

        assertEquals(List.of(now.minus(Duration.ofDays(7)), now.minus(Duration.ofDays(1)), now),
            r.startsWith(now, now, Duration.ofDays(7)));
    }

    @Test
    void laneSplitsAtFastOffsets() {
        assertEquals(Lane.FAST, Lane.of(0, 2));
        assertEquals(Lane.FAST, Lane.of(1, 2));
        assertEquals(Lane.SLOW, Lane.of(2, 2));
    }

    @Test
    void shardsIsFloorModOfTheHash() {
        assertEquals(Math.floorMod("cart-1".hashCode(), 8), Shards.of("cart-1", 8));
        for (String id : List.of("a", "zz", "cart-9", "ÿþ")) {
            int s = Shards.of(id, 8);
            assertEquals(true, s >= 0 && s < 8);
        }
    }

    @Test
    void dispatchConfigDefaultsAndLeaseRule() {
        DispatchConfig d = DispatchConfig.defaults();
        assertEquals(Duration.ofSeconds(90), d.lease());
        assertEquals(Duration.ofSeconds(30), d.gatewayTimeout());
        assertEquals(Duration.ofSeconds(5), d.clockSkew());
        assertEquals(2, d.fastOffsets());
        new DispatchConfig(Duration.ofSeconds(90), Duration.ofSeconds(30), Duration.ZERO, 2);
        assertThrows(IllegalArgumentException.class, () ->
            new DispatchConfig(Duration.ofSeconds(89), Duration.ofSeconds(30), Duration.ofSeconds(5), 2));
    }
}
```

- [ ] **Step 5: Run the model tests to verify they fail**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.model.*' --console=plain`
Expected: FAIL at `compileTestJava` with `cannot find symbol` for `LedgerKey`, `Lane`, `Shards`, `DispatchConfig`, and the 10-argument `CartRecord` constructor.

- [ ] **Step 6: Write the model**

Replace `main/model/CartEvent.java`:

```java
package com.quince.cartrecovery.model;

import java.time.Instant;
import java.util.List;

/** Events published by the Cart Service. Version is per cart and strictly increasing. */
public sealed interface CartEvent permits CartEvent.CartEdited, CartEvent.CartResumed,
        CartEvent.CartCleared, CartEvent.CartPurchased {

    String cartId();
    String shopperKey();
    long version();
    Instant occurredAt();

    /** firstName is optional (null when the shopper is anonymous or the name is unknown). */
    record CartEdited(String cartId, String shopperKey, long version, Instant occurredAt,
                      List<CartItem> items, String firstName) implements CartEvent {
        public CartEdited(String cartId, String shopperKey, long version, Instant occurredAt, List<CartItem> items) {
            this(cartId, shopperKey, version, occurredAt, items, null);
        }
    }

    record CartResumed(String cartId, String shopperKey, long version, Instant occurredAt)
            implements CartEvent {}

    record CartCleared(String cartId, String shopperKey, long version, Instant occurredAt)
            implements CartEvent {}

    record CartPurchased(String cartId, String shopperKey, long version, Instant occurredAt)
            implements CartEvent {}
}
```

Replace `main/model/CartRecord.java`:

```java
package com.quince.cartrecovery.model;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * One record per cart. sequenceStarts holds the last-activity time of each reminder sequence started.
 * firstName is optional (null). srcPartition is the cart-events partition the detector consumed the
 * cart's latest event from, or -1 when unknown (records written before the field existed).
 */
public record CartRecord(String cartId, String shopperKey, CartStatus status, long version,
                         Instant lastActivityAt, List<CartItem> items, Arm arm,
                         List<Instant> sequenceStarts, String firstName, int srcPartition) {

    public static final int MAX_ITEMS = 50;

    public CartRecord {
        items = List.copyOf(items);
        sequenceStarts = List.copyOf(sequenceStarts);
    }

    public CartRecord(String cartId, String shopperKey, CartStatus status, long version,
                      Instant lastActivityAt, List<CartItem> items, Arm arm, List<Instant> sequenceStarts) {
        this(cartId, shopperKey, status, version, lastActivityAt, items, arm, sequenceStarts, null, -1);
    }

    public static CartRecord fresh(String cartId, String shopperKey, Arm arm) {
        return new CartRecord(cartId, shopperKey, CartStatus.ACTIVE, 0L, Instant.EPOCH, List.of(), arm, List.of());
    }

    /** Items are capped at MAX_ITEMS. */
    public CartRecord activity(long newVersion, Instant at, List<CartItem> newItems) {
        List<CartItem> capped = newItems.size() > MAX_ITEMS ? newItems.subList(0, MAX_ITEMS) : newItems;
        return new CartRecord(cartId, shopperKey, CartStatus.ACTIVE, newVersion, at, capped, arm, sequenceStarts,
            firstName, srcPartition);
    }

    public CartRecord closed(long newVersion, Instant at) {
        return new CartRecord(cartId, shopperKey, CartStatus.CLOSED, newVersion, at, items, arm, sequenceStarts,
            firstName, srcPartition);
    }

    public CartRecord abandoned() {
        List<Instant> starts = new ArrayList<>(sequenceStarts);
        starts.add(lastActivityAt);
        return new CartRecord(cartId, shopperKey, CartStatus.ABANDONED, version, lastActivityAt, items, arm, starts,
            firstName, srcPartition);
    }

    /** sequenceStarts pruned to [now - frequencyWindow, now], plus start. */
    public List<Instant> startsWith(Instant start, Instant now, Duration frequencyWindow) {
        Instant from = now.minus(frequencyWindow);
        List<Instant> kept = new ArrayList<>();
        for (Instant s : sequenceStarts) {
            if (!s.isBefore(from) && !s.isAfter(now)) kept.add(s);
        }
        kept.add(start);
        return List.copyOf(kept);
    }
}
```

Replace `main/model/Timer.java`:

```java
package com.quince.cartrecovery.model;

import java.time.Instant;

/** One pending timer per cart. offsetIndex is -1 for CHECK_ABANDON. srcPartition is -1 when unknown. */
public record Timer(String cartId, TimerKind kind, long version, int offsetIndex, Instant dueAt, int srcPartition) {

    public Timer(String cartId, TimerKind kind, long version, int offsetIndex, Instant dueAt) {
        this(cartId, kind, version, offsetIndex, dueAt, -1);
    }

    public static Timer checkAbandon(String cartId, long version, Instant dueAt, int srcPartition) {
        return new Timer(cartId, TimerKind.CHECK_ABANDON, version, -1, dueAt, srcPartition);
    }

    public static Timer reminder(String cartId, long version, int offsetIndex, Instant dueAt, int srcPartition) {
        return new Timer(cartId, TimerKind.REMINDER, version, offsetIndex, dueAt, srcPartition);
    }

    public static Timer checkAbandon(String cartId, long version, Instant dueAt) {
        return checkAbandon(cartId, version, dueAt, -1);
    }

    public static Timer reminder(String cartId, long version, int offsetIndex, Instant dueAt) {
        return reminder(cartId, version, offsetIndex, dueAt, -1);
    }
}
```

Create `main/model/ReminderIntent.java`:

```java
package com.quince.cartrecovery.model;

import java.time.Instant;

/** What the scheduler hands the dispatcher. sendBy = scheduledFor + the offset's lateness bound. */
public record ReminderIntent(String key, String cartId, long version, int offsetIndex, int srcPartition,
                             Instant scheduledFor, Instant sendBy) {}
```

Create `main/model/ReminderMessage.java`:

```java
package com.quince.cartrecovery.model;

import java.util.List;

/** What the gateway receives, built at send time from a consistent cart read. firstName may be null. */
public record ReminderMessage(String key, String cartId, String shopperKey, String firstName, List<CartItem> items) {
    public ReminderMessage {
        items = List.copyOf(items);
    }
}
```

Create `main/model/LedgerKey.java`:

```java
package com.quince.cartrecovery.model;

/** The idempotency key "cartId:version:offsetIndex". Parsed from the right, so a cart id may contain ':'. */
public record LedgerKey(String cartId, long version, int offsetIndex) {

    @Override public String toString() {
        return cartId + ":" + version + ":" + offsetIndex;
    }

    public static LedgerKey parse(String key) {
        int last = key.lastIndexOf(':');
        int middle = last <= 0 ? -1 : key.lastIndexOf(':', last - 1);
        if (middle <= 0) throw new IllegalArgumentException("not a ledger key: " + key);
        try {
            return new LedgerKey(key.substring(0, middle),
                Long.parseLong(key.substring(middle + 1, last)),
                Integer.parseInt(key.substring(last + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not a ledger key: " + key, e);
        }
    }
}
```

Create `main/model/OutcomeKind.java`:

```java
package com.quince.cartrecovery.model;

public enum OutcomeKind { ABANDONED, SENT, SKIPPED_LATE, CANCELLED, DEAD }
```

Create `main/model/Outcome.java`:

```java
package com.quince.cartrecovery.model;

import java.time.Instant;

/**
 * One accounting record. key is null for ABANDONED. Reminder outcomes always carry Arm.TREATMENT,
 * because only eligible (never holdout) carts get reminders.
 */
public record Outcome(String key, String cartId, long version, Arm arm, OutcomeKind kind, Instant at, int attempts) {}
```

Create `main/model/DeadLetter.java` (the old file was moved to `legacy` in Step 1):

```java
package com.quince.cartrecovery.model;

import java.time.Instant;

public record DeadLetter(ReminderIntent intent, String reason, Instant at) {
    /** Reason for an intent that could not be deserialized; replay skips it. */
    public static final String REASON_POISON = "poison";
}
```

Create `main/model/Lane.java`:

```java
package com.quince.cartrecovery.model;

public enum Lane {
    FAST, SLOW;

    /** Early offsets (offsetIndex < fastOffsets) go to the fast lane. */
    public static Lane of(int offsetIndex, int fastOffsets) {
        return offsetIndex < fastOffsets ? FAST : SLOW;
    }
}
```

Create `main/model/DispatchConfig.java`:

```java
package com.quince.cartrecovery.model;

import java.time.Duration;

public record DispatchConfig(Duration lease, Duration gatewayTimeout, Duration clockSkew, int fastOffsets) {

    public DispatchConfig {
        if (lease.compareTo(gatewayTimeout.multipliedBy(3)) < 0)
            throw new IllegalArgumentException("lease " + lease + " must be >= 3 x gatewayTimeout " + gatewayTimeout);
    }

    public static DispatchConfig defaults() {
        return new DispatchConfig(Duration.ofSeconds(90), Duration.ofSeconds(30), Duration.ofSeconds(5), 2);
    }
}
```

Create `main/model/Shards.java`:

```java
package com.quince.cartrecovery.model;

public final class Shards {
    private Shards() {}

    public static int of(String cartId, int shards) {
        return Math.floorMod(cartId.hashCode(), shards);
    }
}
```

Create `main/model/ClaimResult.java`:

```java
package com.quince.cartrecovery.model;

import java.time.Instant;

public sealed interface ClaimResult {
    /** sendBy and srcPartition are the row's stored values, which a takeover never changes. */
    record Claimed(String token, int attempts, Instant sendBy, int srcPartition, Instant leaseUntil)
            implements ClaimResult {}

    /** reason is "final" (the row reached a final status) or "leased" (held, or a retry not yet due). */
    record NotClaimed(String reason) implements ClaimResult {}
}
```

Create `main/model/DueRetry.java`:

```java
package com.quince.cartrecovery.model;

public record DueRetry(String key, int srcPartition) {}
```

Create `main/model/TimerDecision.java`:

```java
package com.quince.cartrecovery.model;

import java.time.Duration;

/** What the caller does with a claimed timer after the scheduler handled it. */
public sealed interface TimerDecision {
    record Ack() implements TimerDecision {}
    record Release(Duration delay) implements TimerDecision {}
}
```

Create `main/model/HandleResult.java`:

```java
package com.quince.cartrecovery.model;

/** DONE: the intent is settled. HOLD: pause the intent's partition and redeliver it later. */
public enum HandleResult { DONE, HOLD }
```

- [ ] **Step 7: Run the model tests to verify they pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.model.*' --console=plain`
Expected: PASS (`LedgerKeyTest` 3, `ModelTest` 6, `RecoveryConfigTest` 7).

- [ ] **Step 8: Write the shared contract tests and their in-memory subclasses**

Create `test/contract/CartStateStoreContract.java` (monotonic `applyEvent`, first-write `shopperKey` and arm, name and item rules, conditional `markAbandoned` including a concurrent detector write, `endSequence`, and `openCartIds` with hour-rounded inclusive `openUntil` and every removal rule):

```java
package com.quince.cartrecovery.contract;

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
import com.quince.cartrecovery.ports.CartStateStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour every CartStateStore must share. Time is an explicit argument of this port, so no time hook is needed.
 * Cart ids carry a per-test prefix, so a subclass may share one table across tests.
 */
public abstract class CartStateStoreContract {
    protected static final int SHARDS = 8;
    protected static final Instant T0 = Instant.parse("2026-01-01T09:00:00Z");
    protected static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));

    protected final String prefix = "c" + UUID.randomUUID().toString().substring(0, 8) + "-";
    protected CartStateStore store;

    /** A store holding none of this test's carts, using config for openUntil and shards for the open index. */
    protected abstract CartStateStore newStore(RecoveryConfig config, int shards);

    @BeforeEach
    void createStore() {
        store = newStore(RecoveryConfig.defaults(), SHARDS);
    }

    protected String cart(String name) { return prefix + name; }

    private static CartEvent.CartEdited edited(String id, long v, Instant at, String firstName) {
        return new CartEvent.CartEdited(id, "shopper-" + id, v, at, ITEMS, firstName);
    }

    private Set<String> open(String id, Instant now) {
        try (Stream<String> ids = store.openCartIds(Shards.of(id, SHARDS), now)) {
            return ids.filter(x -> x.startsWith(prefix)).collect(Collectors.toSet());
        }
    }

    private CartRecord stored(String id) { return store.get(id).orElseThrow(); }

    @Test
    void applyEventCreatesAnActiveRecordWithArmNameAndPartition() {
        String id = cart("a");
        CartRecord r = store.applyEvent(edited(id, 1, T0, "Ada"), Arm.HOLDOUT, 3).orElseThrow();

        assertEquals(new CartRecord(id, "shopper-" + id, CartStatus.ACTIVE, 1, T0, ITEMS, Arm.HOLDOUT, List.of(), "Ada", 3), r);
        assertEquals(r, stored(id));
    }

    @Test
    void staleAndDuplicateEventsAreRejected() {
        String id = cart("a");
        store.applyEvent(edited(id, 2, T0.plusSeconds(20), null), Arm.TREATMENT, 0);

        assertEquals(Optional.empty(), store.applyEvent(edited(id, 2, T0.plusSeconds(20), null), Arm.TREATMENT, 0));
        assertEquals(Optional.empty(), store.applyEvent(edited(id, 1, T0, null), Arm.TREATMENT, 0));
        assertEquals(Optional.empty(), store.applyEvent(new CartEvent.CartPurchased(id, "s", 1, T0), Arm.TREATMENT, 0));
        assertEquals(2, stored(id).version());
        assertEquals(CartStatus.ACTIVE, stored(id).status());
    }

    @Test
    void shopperKeyAndArmAreFixedOnFirstWrite() {
        String id = cart("a");
        store.applyEvent(new CartEvent.CartEdited(id, "guest-1", 1, T0, ITEMS), Arm.TREATMENT, 0);
        store.applyEvent(new CartEvent.CartEdited(id, "user-9", 2, T0.plusSeconds(1), ITEMS), Arm.HOLDOUT, 0);

        assertEquals("guest-1", stored(id).shopperKey());
        assertEquals(Arm.TREATMENT, stored(id).arm());
    }

    @Test
    void editWithoutANameKeepsTheStoredNameAndResumeKeepsItems() {
        String id = cart("a");
        store.applyEvent(edited(id, 1, T0, "Ada"), Arm.TREATMENT, 0);
        store.applyEvent(new CartEvent.CartEdited(id, "s", 2, T0.plusSeconds(1), List.of()), Arm.TREATMENT, 5);
        assertEquals("Ada", stored(id).firstName());
        assertEquals(List.of(), stored(id).items());
        assertEquals(5, stored(id).srcPartition());

        store.applyEvent(edited(id, 3, T0.plusSeconds(2), null), Arm.TREATMENT, 5);
        CartRecord resumed = store.applyEvent(new CartEvent.CartResumed(id, "s", 4, T0.plusSeconds(3)), Arm.TREATMENT, 6)
            .orElseThrow();

        assertEquals(ITEMS, resumed.items());
        assertEquals("Ada", resumed.firstName());
        assertEquals(T0.plusSeconds(3), resumed.lastActivityAt());
        assertEquals(6, resumed.srcPartition());
    }

    @Test
    void absentNameAndEmptyItemsReadBackAsNullAndEmpty() {
        String id = cart("a");
        store.applyEvent(new CartEvent.CartEdited(id, "s", 1, T0, List.of(), null), Arm.TREATMENT, 0);

        assertNull(stored(id).firstName());
        assertEquals(List.of(), stored(id).items());
    }

    @Test
    void itemsAreCappedAtFifty() {
        String id = cart("a");
        List<CartItem> many = IntStream.range(0, 60).mapToObj(i -> new CartItem("SKU-" + i, "Item " + i, 1, 100)).toList();
        store.applyEvent(new CartEvent.CartEdited(id, "s", 1, T0, many), Arm.TREATMENT, 0);

        assertEquals(many.subList(0, 50), stored(id).items());
    }

    @Test
    void purchaseOfAnUnknownCartCreatesAClosedRecordThatBlocksOlderEvents() {
        String id = cart("a");
        CartRecord closed = store.applyEvent(new CartEvent.CartPurchased(id, "s", 5, T0), Arm.TREATMENT, 1).orElseThrow();

        assertEquals(CartStatus.CLOSED, closed.status());
        assertEquals(5, closed.version());
        assertEquals(Optional.empty(), store.applyEvent(edited(id, 3, T0.plusSeconds(1), null), Arm.TREATMENT, 1));
    }

    @Test
    void getAllReturnsPresentRecordsOnly() {
        String a = cart("a");
        String b = cart("b");
        store.applyEvent(edited(a, 1, T0, null), Arm.TREATMENT, 0);
        store.applyEvent(edited(b, 1, T0, null), Arm.TREATMENT, 0);

        List<CartRecord> all = store.getAll(List.of(a, b, cart("missing")));

        assertEquals(Set.of(a, b), all.stream().map(CartRecord::cartId).collect(Collectors.toSet()));
    }

    @Test
    void markAbandonedSetsStatusAndStartsAndKeepsDetectorFields() {
        String id = cart("a");
        CartRecord r = store.applyEvent(edited(id, 1, T0, "Ada"), Arm.TREATMENT, 2).orElseThrow();

        assertTrue(store.markAbandoned(r, List.of(T0), true));

        CartRecord a = stored(id);
        assertEquals(CartStatus.ABANDONED, a.status());
        assertEquals(List.of(T0), a.sequenceStarts());
        assertEquals(ITEMS, a.items());
        assertEquals("Ada", a.firstName());
        assertEquals(2, a.srcPartition());
        assertEquals(1, a.version());
    }

    @Test
    void markAbandonedFailsWhenNotActiveOrWhenANewerEventWon() {
        String id = cart("a");
        CartRecord v1 = store.applyEvent(edited(id, 1, T0, null), Arm.TREATMENT, 0).orElseThrow();
        assertTrue(store.markAbandoned(v1, List.of(T0), true));
        assertFalse(store.markAbandoned(v1, List.of(T0, T0), true));
        assertEquals(List.of(T0), stored(id).sequenceStarts());

        String other = cart("b");
        CartRecord b1 = store.applyEvent(edited(other, 1, T0, null), Arm.TREATMENT, 0).orElseThrow();
        store.applyEvent(edited(other, 2, T0.plusSeconds(1), null), Arm.TREATMENT, 0);

        assertFalse(store.markAbandoned(b1, List.of(T0), true));
        assertEquals(CartStatus.ACTIVE, stored(other).status());
        assertEquals(2, stored(other).version());
        assertEquals(List.of(), stored(other).sequenceStarts());
    }

    @Test
    void endSequenceNeedsTheSameVersionAndAbandonedStatus() {
        String id = cart("a");
        CartRecord r = store.applyEvent(edited(id, 1, T0, null), Arm.TREATMENT, 0).orElseThrow();
        assertFalse(store.endSequence(id, 1));

        store.markAbandoned(r, List.of(T0), true);

        assertFalse(store.endSequence(id, 2));
        assertTrue(store.endSequence(id, 1));
        assertEquals(CartStatus.ABANDONED, stored(id).status());
        assertFalse(store.endSequence(cart("missing"), 1));
    }

    @Test
    void openUntilIsLastActivityPlusLastOffsetAndBoundRoundedUpToTheHour() {
        String id = cart("a");
        store.applyEvent(edited(id, 1, T0, null), Arm.TREATMENT, 0);
        // 09:00 + 24h + 30m = 09:30 next day, rounded up to 10:00 (T0 + 25h), inclusive.
        Instant until = T0.plus(Duration.ofHours(25));

        assertEquals(Set.of(id), open(id, T0));
        assertEquals(Set.of(id), open(id, until.minusSeconds(1)));
        assertEquals(Set.of(id), open(id, until));
        assertEquals(Set.of(), open(id, until.plusMillis(1)));
    }

    @Test
    void aLaterEditMovesOpenUntil() {
        String id = cart("a");
        store.applyEvent(edited(id, 1, T0, null), Arm.TREATMENT, 0);
        store.applyEvent(edited(id, 2, T0.plus(Duration.ofHours(2)), null), Arm.TREATMENT, 0);

        assertEquals(Set.of(id), open(id, T0.plus(Duration.ofHours(26))));
        assertEquals(Set.of(), open(id, T0.plus(Duration.ofHours(27)).plusMillis(1)));
    }

    @Test
    void openCartIdsListsOnlyTheGivenShard() {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 40; i++) ids.add(cart("s" + i));
        for (String id : ids) store.applyEvent(edited(id, 1, T0, null), Arm.TREATMENT, 0);

        for (int shard = 0; shard < SHARDS; shard++) {
            int s = shard;
            Set<String> expected = ids.stream().filter(id -> Shards.of(id, SHARDS) == s).collect(Collectors.toSet());
            try (Stream<String> listed = store.openCartIds(shard, T0)) {
                assertEquals(expected, listed.filter(x -> x.startsWith(prefix)).collect(Collectors.toSet()));
            }
        }
    }

    @Test
    void closingIneligibleAbandonmentAndEndOfSequenceRemoveTheCartFromTheOpenIndex() {
        String purchased = cart("p");
        store.applyEvent(edited(purchased, 1, T0, null), Arm.TREATMENT, 0);
        store.applyEvent(new CartEvent.CartPurchased(purchased, "s", 2, T0.plusSeconds(1)), Arm.TREATMENT, 0);
        assertEquals(Set.of(), open(purchased, T0));

        String ineligible = cart("i");
        CartRecord i = store.applyEvent(edited(ineligible, 1, T0, null), Arm.HOLDOUT, 0).orElseThrow();
        store.markAbandoned(i, List.of(T0), false);
        assertEquals(Set.of(), open(ineligible, T0));

        String eligible = cart("e");
        CartRecord e = store.applyEvent(edited(eligible, 1, T0, null), Arm.TREATMENT, 0).orElseThrow();
        store.markAbandoned(e, List.of(T0), true);
        assertEquals(Set.of(eligible), open(eligible, T0));
        store.endSequence(eligible, 1);
        assertEquals(Set.of(), open(eligible, T0));

        store.applyEvent(edited(purchased, 3, T0.plusSeconds(2), null), Arm.TREATMENT, 0);
        assertEquals(Set.of(purchased), open(purchased, T0));
    }
}
```

Create `test/contract/TimerStoreContract.java` (claim order and limit, leases and redelivery, ack and release only if unchanged, monotonic upsert, equal-data no-op keeping the lease, conditional remove, delimiter round trip):

```java
package com.quince.cartrecovery.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour every TimerStore must share. The store keeps its own time (Redis TIME in production), so a subclass
 * supplies now() and advance(); a real-time subclass may sleep in advance(). Durations stay at a few seconds.
 * The factory's store must use SHARDS shards and hold no due timers at all (a Redis subclass flushes first):
 * claimDue would otherwise lease another test's timers.
 * Every assertion here keeps at least 100 ms of margin, so it holds on a real clock. Millisecond-exact time
 * boundaries are tested only in the in-memory subclass (controller ruling R3); infra subclasses add none.
 */
public abstract class TimerStoreContract {
    protected static final int SHARDS = 8;
    protected static final Duration LEASE = Duration.ofSeconds(1);
    private static final Duration PAST_LEASE = LEASE.plusMillis(100);

    protected final String prefix = "c" + UUID.randomUUID().toString().substring(0, 8) + "-";
    protected TimerStore store;

    protected abstract TimerStore newStore(Duration lease);

    /** The store's current time. */
    protected abstract Instant now();

    /** Moves the store's time forward by at least d. */
    protected abstract void advance(Duration d);

    @BeforeEach
    void createStore() {
        store = newStore(LEASE);
    }

    protected String cart(String name) { return prefix + name; }

    /** Store time plus offset, at millisecond precision (what Redis keeps). */
    protected Instant at(Duration offset) {
        return now().plus(offset).truncatedTo(ChronoUnit.MILLIS);
    }

    private List<Timer> claimMine(int limit) {
        return store.claimDue(limit).stream().filter(t -> t.cartId().startsWith(prefix)).toList();
    }

    private boolean exists(String cartId) {
        return store.existing(Shards.of(cartId, SHARDS), List.of(cartId)).contains(cartId);
    }

    @Test
    void claimDueReturnsOnlyDueTimersEarliestFirst() {
        Timer a = Timer.checkAbandon(cart("a"), 1, at(Duration.ofSeconds(-2)), 0);
        Timer b = Timer.checkAbandon(cart("b"), 1, at(Duration.ofSeconds(-1)), 0);
        Timer c = Timer.checkAbandon(cart("c"), 1, at(Duration.ofHours(1)), 0);
        store.upsert(c);
        store.upsert(b);
        store.upsert(a);

        assertEquals(List.of(a, b), claimMine(10));
    }

    @Test
    void claimDueRespectsTheLimit() {
        store.upsert(Timer.checkAbandon(cart("a"), 1, at(Duration.ofSeconds(-3)), 0));
        store.upsert(Timer.checkAbandon(cart("b"), 1, at(Duration.ofSeconds(-2)), 0));
        store.upsert(Timer.checkAbandon(cart("c"), 1, at(Duration.ofSeconds(-1)), 0));

        assertEquals(2, claimMine(2).size());
        assertEquals(1, claimMine(10).size());
    }

    @Test
    void aClaimedTimerIsLeasedAndRedeliveredAfterTheLease() {
        Timer a = Timer.reminder(cart("a"), 1, 0, at(Duration.ofSeconds(-1)), 4);
        store.upsert(a);

        assertEquals(List.of(a), claimMine(10));
        assertEquals(List.of(), claimMine(10));
        advance(PAST_LEASE);
        assertEquals(List.of(a), claimMine(10));
    }

    @Test
    void ackRemovesTheTimerOnlyIfUnchanged() {
        Timer v1 = Timer.checkAbandon(cart("a"), 1, at(Duration.ofSeconds(-1)), 0);
        store.upsert(v1);
        claimMine(10);
        Timer v2 = Timer.checkAbandon(cart("a"), 2, at(Duration.ofHours(1)), 0);
        store.upsert(v2);

        store.ack(v1);
        assertTrue(exists(cart("a")));

        store.ack(v2);
        assertFalse(exists(cart("a")));
    }

    @Test
    void releaseMakesAnUnchangedTimerDueAgainAfterTheDelay() {
        Timer a = Timer.checkAbandon(cart("a"), 1, at(Duration.ofSeconds(-1)), 0);
        store.upsert(a);
        claimMine(10);

        store.release(a, Duration.ofMillis(500));

        assertEquals(List.of(), claimMine(10));
        advance(Duration.ofMillis(600));
        assertEquals(List.of(a), claimMine(10));
    }

    @Test
    void releaseOfAReplacedTimerChangesNothing() {
        Timer v1 = Timer.checkAbandon(cart("a"), 1, at(Duration.ofSeconds(-1)), 0);
        store.upsert(v1);
        claimMine(10);
        Timer v2 = Timer.checkAbandon(cart("a"), 2, at(Duration.ofHours(1)), 0);
        store.upsert(v2);

        store.release(v1, Duration.ZERO);

        assertEquals(List.of(), claimMine(10));
        assertTrue(exists(cart("a")));
    }

    @Test
    void upsertOnlyMovesForwardByVersionThenOffset() {
        String id = cart("a");
        Instant due = at(Duration.ofHours(1));
        assertTrue(store.upsert(Timer.checkAbandon(id, 2, due, 0)));
        assertFalse(store.upsert(Timer.checkAbandon(id, 1, due, 0)));
        assertFalse(store.upsert(Timer.reminder(id, 1, 2, due, 0)));
        assertTrue(store.upsert(Timer.reminder(id, 2, 0, due, 0)));
        assertFalse(store.upsert(Timer.checkAbandon(id, 2, due, 0)));
        assertTrue(store.upsert(Timer.reminder(id, 2, 1, due, 0)));
        assertTrue(store.upsert(Timer.checkAbandon(id, 3, due, 0)));
    }

    @Test
    void anEqualUpsertIsANoOpThatKeepsTheLease() {
        Timer a = Timer.checkAbandon(cart("a"), 1, at(Duration.ofSeconds(-1)), 0);
        store.upsert(a);
        claimMine(10);

        assertFalse(store.upsert(a));
        assertEquals(List.of(), claimMine(10));
    }

    @Test
    void theSameVersionAndOffsetWithDifferentDataIsRejected() {
        Timer first = Timer.reminder(cart("a"), 1, 0, at(Duration.ofSeconds(-2)), 0);
        store.upsert(first);

        assertFalse(store.upsert(Timer.reminder(cart("a"), 1, 0, at(Duration.ofSeconds(-1)), 7)));
        assertEquals(List.of(first), claimMine(10));
    }

    @Test
    void removeOnlyIfTheStoredVersionIsAtMostTheGivenOne() {
        String id = cart("a");
        store.upsert(Timer.checkAbandon(id, 3, at(Duration.ofHours(1)), 0));

        store.remove(id, 2);
        assertTrue(exists(id));
        store.remove(id, 3);
        assertFalse(exists(id));

        store.upsert(Timer.reminder(id, 5, 1, at(Duration.ofHours(1)), 0));
        store.remove(id, 9);
        assertFalse(exists(id));
    }

    @Test
    void aRemovedTimerCanBeRecreatedByALateOlderUpsert() {
        String id = cart("a");
        store.upsert(Timer.checkAbandon(id, 2, at(Duration.ofHours(1)), 0));
        store.remove(id, 2);

        assertTrue(store.upsert(Timer.checkAbandon(id, 1, at(Duration.ofHours(1)), 0)));
    }

    @Test
    void existingReturnsOnlyCartsWithATimer() {
        String with = cart("with");
        String without = cart("without");
        store.upsert(Timer.checkAbandon(with, 1, at(Duration.ofHours(1)), 0));

        assertEquals(Set.of(with), store.existing(Shards.of(with, SHARDS), List.of(with)));
        assertEquals(Set.of(), store.existing(Shards.of(without, SHARDS), List.of(without)));
    }

    @Test
    void everyFieldRoundTripsIncludingDelimitersInTheCartId() {
        Timer odd = Timer.reminder(cart("a:b|c"), 12, 2, at(Duration.ofSeconds(-1)), 6);
        store.upsert(odd);

        assertEquals(List.of(odd), claimMine(10));
    }
}
```

Create `test/contract/WatermarkContract.java` (unknown and stale read as `EPOCH`, generation fencing, max within a generation, minimum over all partitions for `-1`):

```java
package com.quince.cartrecovery.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour every Watermark must share. Staleness is measured on the watermark's own time source, so a subclass
 * supplies advance(); a real-time subclass may sleep. The factory returns a watermark with no partitions written
 * (a Redis subclass flushes first and reads partitions 0 to 2, the ones this contract writes, for current(-1)).
 * Millisecond-exact time boundaries (for example staleness starting strictly after 5 s) are tested only in the
 * in-memory subclass, never here (controller ruling R3); infra subclasses add none.
 */
public abstract class WatermarkContract {
    private static final Instant E = Instant.parse("2026-01-01T09:00:00Z");

    protected Watermark watermark;

    protected abstract Watermark newWatermark();

    /** Moves the watermark's time source forward by at least d. */
    protected abstract void advance(Duration d);

    @BeforeEach
    void createWatermark() {
        watermark = newWatermark();
    }

    @Test
    void anUnknownPartitionReadsAsEpoch() {
        assertEquals(Instant.EPOCH, watermark.current(0));
        assertEquals(Instant.EPOCH, watermark.current(-1));
    }

    @Test
    void aPublishedValueIsCurrent() {
        watermark.publish(0, 1, E);
        assertEquals(E, watermark.current(0));
        assertEquals(Instant.EPOCH, watermark.current(1));
    }

    @Test
    void theSameGenerationKeepsTheMaximum() {
        watermark.publish(0, 4, E.plusSeconds(10));
        watermark.publish(0, 4, E);
        assertEquals(E.plusSeconds(10), watermark.current(0));

        watermark.publish(0, 4, E.plusSeconds(11));
        assertEquals(E.plusSeconds(11), watermark.current(0));
    }

    @Test
    void aLowerGenerationIsRejected() {
        watermark.publish(0, 5, E);
        watermark.publish(0, 4, E.plusSeconds(60));
        assertEquals(E, watermark.current(0));
    }

    @Test
    void aHigherGenerationOverwritesEvenWithAnEarlierTime() {
        watermark.publish(0, 4, E.plusSeconds(60));
        watermark.publish(0, 5, E);
        assertEquals(E, watermark.current(0));
    }

    @Test
    void minusOneReadsTheMinimumOverAllPartitions() {
        watermark.publish(0, 1, E.plusSeconds(10));
        watermark.publish(1, 1, E.plusSeconds(5));
        watermark.publish(2, 1, E.plusSeconds(20));
        assertEquals(E.plusSeconds(5), watermark.current(-1));
    }

    @Test
    void aPartitionNotWrittenForMoreThanFiveSecondsReadsAsEpoch() {
        watermark.publish(0, 1, E);
        watermark.publish(1, 1, E);

        advance(Duration.ofSeconds(6));
        watermark.publish(1, 1, E.plusSeconds(1));

        assertEquals(Instant.EPOCH, watermark.current(0));
        assertEquals(E.plusSeconds(1), watermark.current(1));
        assertEquals(Instant.EPOCH, watermark.current(-1));

        watermark.publish(0, 1, E.plusSeconds(2));
        assertEquals(E.plusSeconds(2), watermark.current(0));
    }
}
```

Create `test/contract/SendLedgerContract.java` (claim and takeover with attempt increment at inclusive `leaseUntil` and `nextAttemptAt` boundaries, stale-token rejection, `finish`, `markRetry`, `reopen`, `dueRetries`, `highestOffsetIndex`, and a cart id with `:`; Review Focus 3's "claim exactly at `leaseUntil`" is `aHeldLeaseIsNotClaimedAgainUntilExactlyLeaseUntil`):

```java
package com.quince.cartrecovery.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.ClaimResult.Claimed;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.SendLedger;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour every SendLedger must share. Time is an explicit argument of this port, so boundaries are tested exactly
 * and no time hook is needed. Keys carry a per-test prefix, so a subclass may share one table across tests.
 */
public abstract class SendLedgerContract {
    protected static final Duration LEASE = Duration.ofSeconds(90);
    protected static final int SHARDS = 4;
    protected static final Instant NOW = Instant.parse("2026-01-01T09:30:00Z");
    protected static final Instant SEND_BY = NOW.plus(Duration.ofMinutes(5));

    protected final String prefix = "c" + UUID.randomUUID().toString().substring(0, 8) + "-";
    protected SendLedger ledger;

    /** A ledger holding none of this test's keys. */
    protected abstract SendLedger newLedger(Duration lease, int shards);

    @BeforeEach
    void createLedger() {
        ledger = newLedger(LEASE, SHARDS);
    }

    protected String key(String cart, long version, int offset) {
        return new LedgerKey(prefix + cart, version, offset).toString();
    }

    private Claimed claimed(String key, Instant now) {
        return assertInstanceOf(Claimed.class, ledger.claim(key, SEND_BY, 3, now));
    }

    private List<DueRetry> due(String key, Instant now) {
        int shard = Shards.of(LedgerKey.parse(key).cartId(), SHARDS);
        return ledger.dueRetries(shard, now, 100).stream().filter(d -> d.key().startsWith(prefix)).toList();
    }

    @Test
    void theFirstClaimCreatesASendingRowWithOneAttempt() {
        Claimed c = claimed(key("a", 1, 0), NOW);

        assertEquals(1, c.attempts());
        assertEquals(SEND_BY, c.sendBy());
        assertEquals(3, c.srcPartition());
        assertEquals(NOW.plus(LEASE), c.leaseUntil());
    }

    @Test
    void aHeldLeaseIsNotClaimedAgainUntilExactlyLeaseUntil() {
        String k = key("a", 1, 0);
        Claimed first = claimed(k, NOW);

        assertEquals(new ClaimResult.NotClaimed("leased"), ledger.claim(k, SEND_BY, 3, NOW));
        assertEquals(new ClaimResult.NotClaimed("leased"), ledger.claim(k, SEND_BY, 3, first.leaseUntil().minusMillis(1)));

        Claimed takeover = claimed(k, first.leaseUntil());
        assertEquals(2, takeover.attempts());
        assertNotEquals(first.token(), takeover.token());
        assertEquals(first.leaseUntil().plus(LEASE), takeover.leaseUntil());
    }

    @Test
    void aTakeoverKeepsTheStoredSendByAndPartition() {
        String k = key("a", 1, 0);
        Claimed first = claimed(k, NOW);

        ClaimResult again = ledger.claim(k, SEND_BY.plusSeconds(999), 7, first.leaseUntil());

        Claimed c = assertInstanceOf(Claimed.class, again);
        assertEquals(SEND_BY, c.sendBy());
        assertEquals(3, c.srcPartition());
    }

    @Test
    void aStaleTokenCanNeitherFinishNorMarkRetry() {
        String k = key("a", 1, 0);
        Claimed stale = claimed(k, NOW);
        Claimed current = claimed(k, stale.leaseUntil());

        assertFalse(ledger.finish(k, stale.token(), OutcomeKind.SENT, null));
        assertFalse(ledger.markRetry(k, stale.token(), NOW.plusSeconds(60)));
        assertTrue(ledger.finish(k, current.token(), OutcomeKind.SENT, null));
    }

    @Test
    void aFinishedRowIsFinal() {
        String k = key("a", 1, 0);
        Claimed c = claimed(k, NOW);
        assertTrue(ledger.finish(k, c.token(), OutcomeKind.CANCELLED, "cancelled"));

        assertEquals(new ClaimResult.NotClaimed("final"), ledger.claim(k, SEND_BY, 3, NOW.plus(Duration.ofDays(1))));
        assertFalse(ledger.finish(k, c.token(), OutcomeKind.SENT, null));
        assertEquals(List.of(), due(k, NOW.plus(Duration.ofDays(1))));
    }

    @Test
    void markRetryReleasesTheLeaseUntilExactlyNextAttemptAt() {
        String k = key("a", 1, 0);
        Claimed c = claimed(k, NOW);
        Instant next = NOW.plusSeconds(60);

        assertTrue(ledger.markRetry(k, c.token(), next));
        assertFalse(ledger.finish(k, c.token(), OutcomeKind.SENT, null));
        assertEquals(new ClaimResult.NotClaimed("leased"), ledger.claim(k, SEND_BY, 3, next.minusMillis(1)));

        Claimed retry = claimed(k, next);
        assertEquals(2, retry.attempts());
        assertEquals(SEND_BY, retry.sendBy());
    }

    @Test
    void dueRetriesListsSendingRowsFromLeaseUntilAndRetryingRowsFromNextAttemptAt() {
        String sending = key("s", 1, 0);
        String retrying = key("r", 1, 1);
        String done = key("d", 1, 0);
        Claimed s = claimed(sending, NOW);
        Claimed r = claimed(retrying, NOW);
        ledger.markRetry(retrying, r.token(), NOW.plusSeconds(30));
        Claimed d = claimed(done, NOW);
        ledger.finish(done, d.token(), OutcomeKind.SENT, null);

        assertEquals(List.of(), due(sending, s.leaseUntil().minusMillis(1)));
        assertEquals(List.of(new DueRetry(sending, 3)), due(sending, s.leaseUntil()));
        assertEquals(List.of(), due(retrying, NOW.plusSeconds(29)));
        assertEquals(List.of(new DueRetry(retrying, 3)), due(retrying, NOW.plusSeconds(30)));
        assertEquals(List.of(), due(done, NOW.plus(Duration.ofDays(1))));
    }

    @Test
    void dueRetriesIsPerShardEarliestFirstAndLimited() {
        String first = null;
        String second = null;
        String otherShard = null;
        int shard = Shards.of(prefix + "x0", SHARDS);
        for (int i = 0; first == null || second == null || otherShard == null; i++) {
            String cart = "x" + i;
            boolean same = Shards.of(prefix + cart, SHARDS) == shard;
            if (same && first == null) first = key(cart, 1, 0);
            else if (same && second == null) second = key(cart, 1, 0);
            else if (!same && otherShard == null) otherShard = key(cart, 1, 0);
        }
        ledger.markRetry(second, claimed(second, NOW).token(), NOW.plusSeconds(20));
        ledger.markRetry(first, claimed(first, NOW).token(), NOW.plusSeconds(10));
        ledger.markRetry(otherShard, claimed(otherShard, NOW).token(), NOW.plusSeconds(5));

        List<DueRetry> all = ledger.dueRetries(shard, NOW.plusSeconds(60), 100).stream()
            .filter(d -> d.key().startsWith(prefix)).toList();
        assertEquals(List.of(new DueRetry(first, 3), new DueRetry(second, 3)), all);
        assertEquals(1, ledger.dueRetries(shard, NOW.plusSeconds(60), 1).size());
    }

    @Test
    void reopenMovesOnlyADeadRowBackToRetryingWithAttemptsReset() {
        String k = key("a", 1, 0);
        Claimed c = claimed(k, NOW);
        assertFalse(ledger.reopen(k, NOW));
        ledger.finish(k, c.token(), OutcomeKind.DEAD, "permanent_failure");
        Instant later = NOW.plusSeconds(120);

        assertTrue(ledger.reopen(k, later));
        assertFalse(ledger.reopen(k, later));
        assertEquals(List.of(new DueRetry(k, 3)), due(k, later));
        assertEquals(1, claimed(k, later).attempts());
        assertFalse(ledger.reopen(key("missing", 1, 0), later));
    }

    @Test
    void reopenLeavesOtherFinalRowsAlone() {
        String k = key("a", 1, 0);
        ledger.finish(k, claimed(k, NOW).token(), OutcomeKind.SENT, null);

        assertFalse(ledger.reopen(k, NOW));
        assertEquals(new ClaimResult.NotClaimed("final"), ledger.claim(k, SEND_BY, 3, NOW));
    }

    @Test
    void highestOffsetIndexIsPerCartAndVersionOverAnyStatus() {
        assertEquals(-1, ledger.highestOffsetIndex(prefix + "a", 1));
        ledger.finish(key("a", 1, 0), claimed(key("a", 1, 0), NOW).token(), OutcomeKind.SKIPPED_LATE, "late");
        claimed(key("a", 1, 2), NOW);
        claimed(key("a", 2, 1), NOW);
        claimed(key("ab", 1, 3), NOW);

        assertEquals(2, ledger.highestOffsetIndex(prefix + "a", 1));
        assertEquals(1, ledger.highestOffsetIndex(prefix + "a", 2));
    }

    @Test
    void aCartIdContainingColonsWorksEverywhere() {
        String k = key("x:1:2", 7, 1);
        Claimed c = claimed(k, NOW);
        ledger.markRetry(k, c.token(), NOW);

        assertEquals(List.of(new DueRetry(k, 3)), due(k, NOW));
        assertEquals(1, ledger.highestOffsetIndex(prefix + "x:1:2", 7));
        assertEquals(-1, ledger.highestOffsetIndex(prefix + "x", 1));
    }
}
```

Create `test/inmemory/InMemoryCartStateStoreContractTest.java`:

```java
package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.contract.CartStateStoreContract;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.CartStateStore;

class InMemoryCartStateStoreContractTest extends CartStateStoreContract {
    @Override protected CartStateStore newStore(RecoveryConfig config, int shards) {
        return new InMemoryCartStateStore(config, shards);
    }
}
```

Create `test/inmemory/PriorityQueueTimerStoreContractTest.java`:

```java
package com.quince.cartrecovery.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.contract.TimerStoreContract;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PriorityQueueTimerStoreContractTest extends TimerStoreContract {
    private final FakeClock clock = new FakeClock(Instant.parse("2026-01-01T09:00:00Z"));

    @Override protected TimerStore newStore(Duration lease) { return new PriorityQueueTimerStore(clock, lease); }
    @Override protected Instant now() { return clock.now(); }
    @Override protected void advance(Duration d) { clock.advance(d); }

    @Test
    void nextDueAtIsTheEarliestDueTimeOrLeaseExpiry() {
        PriorityQueueTimerStore timers = (PriorityQueueTimerStore) store;
        assertEquals(Optional.empty(), timers.nextDueAt());
        timers.upsert(Timer.checkAbandon("a", 1, clock.now().plusSeconds(30)));
        timers.upsert(Timer.checkAbandon("b", 1, clock.now()));
        assertEquals(Optional.of(clock.now()), timers.nextDueAt());

        timers.claimDue(1);

        assertEquals(Optional.of(clock.now().plus(LEASE)), timers.nextDueAt());
        assertEquals(2, timers.size());
    }

    @Test
    void clearDropsEverything() {
        PriorityQueueTimerStore timers = (PriorityQueueTimerStore) store;
        timers.upsert(Timer.checkAbandon("a", 1, clock.now()));
        timers.clear();
        assertEquals(0, timers.size());
        assertEquals(Optional.empty(), timers.nextDueAt());
    }
}
```

Create `test/inmemory/InMemoryWatermarkContractTest.java`:

```java
package com.quince.cartrecovery.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.contract.WatermarkContract;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class InMemoryWatermarkContractTest extends WatermarkContract {
    private final FakeClock clock = new FakeClock(Instant.parse("2026-01-01T09:00:00Z"));

    @Override protected Watermark newWatermark() { return new InMemoryWatermark(clock); }
    @Override protected void advance(Duration d) { clock.advance(d); }

    @Test
    void stalenessStartsStrictlyAfterFiveSeconds() {
        watermark.publish(0, 1, clock.now());
        Instant written = clock.now();

        clock.advance(Duration.ofSeconds(5));
        assertEquals(written, watermark.current(0));
        clock.advance(Duration.ofMillis(1));
        assertEquals(Instant.EPOCH, watermark.current(0));
    }

    @Test
    void setLaggingPinsAPartitionUntilClearLag() {
        InMemoryWatermark w = (InMemoryWatermark) watermark;
        Instant stalledAt = clock.now();
        w.setLagging(0, stalledAt);
        clock.advance(Duration.ofMinutes(10));
        w.publish(0, 1, clock.now());
        w.publish(1, 1, clock.now());

        assertEquals(stalledAt, w.current(0));
        assertEquals(clock.now(), w.current(1));
        assertEquals(stalledAt, w.current(-1));

        w.clearLag();
        assertEquals(clock.now(), w.current(0));
    }

    @Test
    void nowIsTheClock() {
        assertEquals(clock.now(), watermark.now());
    }
}
```

Create `test/inmemory/InMemorySendLedgerContractTest.java`:

```java
package com.quince.cartrecovery.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.quince.cartrecovery.contract.SendLedgerContract;
import com.quince.cartrecovery.model.ClaimResult.Claimed;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.ports.SendLedger;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class InMemorySendLedgerContractTest extends SendLedgerContract {
    @Override protected SendLedger newLedger(Duration lease, int shards) { return new InMemorySendLedger(lease, shards); }

    @Test
    void statusSizeAndNextRetryAtTrackTheRows() {
        InMemorySendLedger l = (InMemorySendLedger) ledger;
        String k = key("a", 1, 0);
        assertEquals(Optional.empty(), l.status(k));
        assertEquals(Optional.empty(), l.nextRetryAt());

        Claimed c = (Claimed) l.claim(k, SEND_BY, 0, NOW);
        assertEquals(Optional.of("SENDING"), l.status(k));
        assertEquals(Optional.of(c.leaseUntil()), l.nextRetryAt());

        l.markRetry(k, c.token(), NOW.plusSeconds(60));
        assertEquals(Optional.of("RETRYING"), l.status(k));
        assertEquals(Optional.of(NOW.plusSeconds(60)), l.nextRetryAt());

        Claimed again = (Claimed) l.claim(k, SEND_BY, 0, NOW.plusSeconds(60));
        l.finish(k, again.token(), OutcomeKind.SENT, null);
        assertEquals(Optional.of("SENT"), l.status(k));
        assertEquals(Optional.empty(), l.nextRetryAt());
        assertEquals(1, l.size());
    }

    @Test
    void abandonedIsNotALedgerOutcome() {
        String k = key("a", 1, 0);
        Claimed c = (Claimed) ledger.claim(k, SEND_BY, 0, NOW);
        assertThrows(IllegalArgumentException.class, () -> ledger.finish(k, c.token(), OutcomeKind.ABANDONED, null));
    }
}
```

- [ ] **Step 9: Run the contract tests to verify they fail**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.inmemory.*' --console=plain`
Expected: FAIL at `compileTestJava` with `cannot find symbol` for `Watermark`, `InMemoryWatermark`, `CartStateStore` (the port was moved to `legacy`), and the other missing ports and adapters.

- [ ] **Step 10: Write the ports**

Create `main/ports/CartStateStore.java`:

```java
package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartRecord;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Durable per-cart state. Production: DynamoDB "carts" with field-scoped conditional updates.
 * A cart is "open" (listed by openCartIds) while it has a next step: from an edit or resume until it
 * closes, is abandoned but ineligible, or ends its sequence. openUntil = lastActivityAt + last offset
 * + last lateness bound, rounded up to the next whole hour.
 */
public interface CartStateStore {
    /** Consistent read. */
    Optional<CartRecord> get(String cartId);

    /** Consistent reads in input order; absent ids are omitted and a duplicate id appears once. */
    List<CartRecord> getAll(Collection<String> cartIds);

    /**
     * Applies the event if the cart is absent or its stored version is lower; empty when stale.
     * Edit or resume: status ACTIVE, version, lastActivityAt = occurredAt, items (edit only, capped at 50),
     * firstName (edit only, and only when non-null), srcPartition, shopperKey and arm only if the cart is new;
     * opens the cart. Purchase or clear: status CLOSED, version, lastActivityAt = occurredAt, srcPartition;
     * closes the cart. sequenceStarts are never touched here.
     */
    Optional<CartRecord> applyEvent(CartEvent event, Arm arm, int srcPartition);

    /**
     * Condition: stored version = record.version() AND status = ACTIVE. Sets ABANDONED and sequenceStarts;
     * an ineligible cart is closed in the open index. Returns whether it wrote.
     */
    boolean markAbandoned(CartRecord record, List<Instant> sequenceStarts, boolean eligible);

    /** Condition: stored version = version AND status = ABANDONED. Removes the cart from the open index. */
    boolean endSequence(String cartId, long version);

    /** Open carts of this shard with openUntil >= now. Close the stream after use. */
    Stream<String> openCartIds(int shard, Instant now);
}
```

Create `main/ports/TimerStore.java`:

```java
package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.Timer;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Derived due-time index, one timer per cart, rebuildable from durable state. Production: Redis scripts
 * on per-shard sorted sets, timed by Redis TIME. Timers compare by (version, offsetIndex), CHECK_ABANDON
 * having offsetIndex -1.
 */
public interface TimerStore {
    /** Writes only if (version, offsetIndex) is greater than the stored timer's; equal data is a no-op that keeps any lease. */
    boolean upsert(Timer timer);

    /** Removes the cart's timer only if its version is <= version. */
    void remove(String cartId, long version);

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

Create `main/ports/Watermark.java`:

```java
package com.quince.cartrecovery.ports;

import java.time.Instant;

/**
 * Per-partition event-time watermark: every cart event appended to the partition before current(p)
 * has been processed. Production: Redis hash "watermarks", timed by Redis TIME.
 */
public interface Watermark {
    /** Rejects a lower generation than stored; same generation keeps max(stored, eventTime); a higher one overwrites. */
    void publish(int partition, long generation, Instant eventTime);

    /** Instant.EPOCH if unknown or not written for more than 5 s; srcPartition -1 gives the minimum over all partitions. */
    Instant current(int srcPartition);

    /** The watermark's time source. */
    Instant now();
}
```

Create `main/ports/IntentPublisher.java`:

```java
package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.ReminderIntent;

public interface IntentPublisher {
    /** Blocks until the intent is acknowledged; the lane is chosen from offsetIndex. */
    void publish(ReminderIntent intent);
}
```

Create `main/ports/SendLedger.java`:

```java
package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.OutcomeKind;
import java.time.Instant;
import java.util.List;

/**
 * One row per idempotency key with a fenced lease. Statuses: SENDING (leaseToken, leaseUntil), RETRYING,
 * and the final SENT, SKIPPED_LATE, CANCELLED, DEAD. Every non-final row is in the retry index with
 * nextAttemptAt (for SENDING equal to leaseUntil). Production: DynamoDB "send-ledger".
 */
public interface SendLedger {
    /**
     * Creates the row as SENDING with attempts 1 if absent; or takes it over, incrementing attempts, if
     * RETRYING with nextAttemptAt <= now or SENDING with leaseUntil <= now. Every claim gets a fresh token
     * and leaseUntil = now + lease. Otherwise NotClaimed("final") or NotClaimed("leased").
     */
    ClaimResult claim(String key, Instant sendBy, int srcPartition, Instant now);

    /** SENDING with this token → RETRYING due at nextAt. False if the lease was lost. */
    boolean markRetry(String key, String token, Instant nextAt);

    /** SENDING with this token → the final outcome (SENT, SKIPPED_LATE, CANCELLED, DEAD). False if the lease was lost. */
    boolean finish(String key, String token, OutcomeKind outcome, String reason);

    /** Non-final rows of this shard with nextAttemptAt <= now, earliest first. */
    List<DueRetry> dueRetries(int shard, Instant now, int limit);

    /** DEAD → RETRYING due now with attempts reset to 0. False for any other status. */
    boolean reopen(String key, Instant now);

    /** Highest offsetIndex with a row for this cart and version, any status; -1 if none. */
    int highestOffsetIndex(String cartId, long version);
}
```

Create `main/ports/NotificationSink.java`:

```java
package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;

/** The notification gateway boundary. No implementation in this repo performs a real send. */
public interface NotificationSink {
    SendResult send(ReminderMessage message);
}
```

Create `main/ports/OutcomeRecorder.java`:

```java
package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.Outcome;

public interface OutcomeRecorder {
    void record(Outcome outcome);
}
```

Create `main/ports/DeadLetterQueue.java`:

```java
package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.DeadLetter;

public interface DeadLetterQueue {
    void add(DeadLetter letter);
}
```

Create `main/ports/SendBudget.java`:

```java
package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.Lane;

public interface SendBudget {
    /** Takes one send token for the lane if available; never blocks. */
    boolean tryAcquire(Lane lane);
}
```

- [ ] **Step 11: Write the in-memory adapters**

Create `main/inmemory/InMemoryCartStateStore.java`:

```java
package com.quince.cartrecovery.inmemory;

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
import java.util.Optional;
import java.util.stream.Stream;

/** Map-backed cart store with the same conditional semantics as the DynamoDB "carts" table. */
public final class InMemoryCartStateStore implements CartStateStore {
    private final RecoveryConfig config;
    private final int shards;
    private final Map<String, CartRecord> records = new HashMap<>();
    /** cartId → openUntil, present while the cart is open (the sparse open-by-shard index). */
    private final Map<String, Instant> openUntil = new HashMap<>();

    public InMemoryCartStateStore(RecoveryConfig config, int shards) {
        this.config = config;
        this.shards = shards;
    }

    @Override public synchronized Optional<CartRecord> get(String cartId) {
        return Optional.ofNullable(records.get(cartId));
    }

    @Override public synchronized List<CartRecord> getAll(Collection<String> cartIds) {
        List<CartRecord> out = new ArrayList<>();
        for (String id : new LinkedHashSet<>(cartIds)) {   // input order, duplicates once (controller ruling R4)
            CartRecord r = records.get(id);
            if (r != null) out.add(r);
        }
        return out;
    }

    @Override public synchronized Optional<CartRecord> applyEvent(CartEvent event, Arm arm, int srcPartition) {
        CartRecord stored = records.get(event.cartId());
        if (stored != null && stored.version() >= event.version()) return Optional.empty();
        String shopperKey = stored == null ? event.shopperKey() : stored.shopperKey();
        Arm keptArm = stored == null ? arm : stored.arm();
        List<CartItem> items = stored == null ? List.of() : stored.items();
        String firstName = stored == null ? null : stored.firstName();
        List<Instant> starts = stored == null ? List.of() : stored.sequenceStarts();
        CartRecord updated = switch (event) {
            case CartEvent.CartEdited e -> new CartRecord(e.cartId(), shopperKey, CartStatus.ACTIVE, e.version(),
                e.occurredAt(), capped(e.items()), keptArm, starts,
                e.firstName() != null ? e.firstName() : firstName, srcPartition);
            case CartEvent.CartResumed e -> new CartRecord(e.cartId(), shopperKey, CartStatus.ACTIVE, e.version(),
                e.occurredAt(), items, keptArm, starts, firstName, srcPartition);
            case CartEvent.CartCleared e -> new CartRecord(e.cartId(), shopperKey, CartStatus.CLOSED, e.version(),
                e.occurredAt(), items, keptArm, starts, firstName, srcPartition);
            case CartEvent.CartPurchased e -> new CartRecord(e.cartId(), shopperKey, CartStatus.CLOSED, e.version(),
                e.occurredAt(), items, keptArm, starts, firstName, srcPartition);
        };
        records.put(updated.cartId(), updated);
        if (updated.status() == CartStatus.ACTIVE) {
            openUntil.put(updated.cartId(), openUntil(updated.lastActivityAt()));
        } else {
            openUntil.remove(updated.cartId());
        }
        return Optional.of(updated);
    }

    @Override public synchronized boolean markAbandoned(CartRecord record, List<Instant> sequenceStarts, boolean eligible) {
        CartRecord stored = records.get(record.cartId());
        if (stored == null || stored.version() != record.version() || stored.status() != CartStatus.ACTIVE) return false;
        records.put(stored.cartId(), new CartRecord(stored.cartId(), stored.shopperKey(), CartStatus.ABANDONED,
            stored.version(), stored.lastActivityAt(), stored.items(), stored.arm(), sequenceStarts,
            stored.firstName(), stored.srcPartition()));
        if (!eligible) openUntil.remove(stored.cartId());
        return true;
    }

    @Override public synchronized boolean endSequence(String cartId, long version) {
        CartRecord stored = records.get(cartId);
        if (stored == null || stored.version() != version || stored.status() != CartStatus.ABANDONED) return false;
        openUntil.remove(cartId);
        return true;
    }

    @Override public synchronized Stream<String> openCartIds(int shard, Instant now) {
        return openUntil.entrySet().stream()
            .filter(e -> Shards.of(e.getKey(), shards) == shard && !e.getValue().isBefore(now))
            .map(Map.Entry::getKey)
            .sorted()
            .toList()
            .stream();
    }

    private Instant openUntil(Instant lastActivityAt) {
        int last = config.offsets().size() - 1;
        Instant until = lastActivityAt.plus(config.offsets().get(last)).plus(config.latenessBounds().get(last));
        Instant hour = until.truncatedTo(ChronoUnit.HOURS);
        return hour.equals(until) ? until : hour.plus(Duration.ofHours(1));
    }

    private static List<CartItem> capped(List<CartItem> items) {
        return items.size() > CartRecord.MAX_ITEMS ? items.subList(0, CartRecord.MAX_ITEMS) : items;
    }
}
```

Create `main/inmemory/PriorityQueueTimerStore.java`:

```java
package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Same semantics as the Redis timer scripts, timed by the given clock: one timer per cart, scored by due time
 * or lease expiry. Queue entries whose slot is no longer the live one for their cart are skipped lazily.
 */
public final class PriorityQueueTimerStore implements TimerStore {
    private record Slot(Timer timer, Instant score) {}

    private static final Comparator<Slot> BY_SCORE =
        Comparator.comparing(Slot::score).thenComparing(s -> s.timer().cartId());

    private final Clock clock;
    private final Duration lease;
    private final Map<String, Slot> live = new HashMap<>();
    private final PriorityQueue<Slot> queue = new PriorityQueue<>(BY_SCORE);

    public PriorityQueueTimerStore(Clock clock, Duration lease) {
        this.clock = clock;
        this.lease = lease;
    }

    @Override public synchronized boolean upsert(Timer timer) {
        Slot current = live.get(timer.cartId());
        if (current != null && !greater(timer, current.timer())) return false;
        put(new Slot(timer, timer.dueAt()));
        return true;
    }

    @Override public synchronized void remove(String cartId, long version) {
        Slot current = live.get(cartId);
        if (current != null && current.timer().version() <= version) live.remove(cartId);
    }

    @Override public synchronized List<Timer> claimDue(int limit) {
        Instant now = clock.now();
        List<Timer> claimed = new ArrayList<>();
        while (claimed.size() < limit) {
            Slot head = liveHead();
            if (head == null || head.score().isAfter(now)) break;
            queue.poll();
            put(new Slot(head.timer(), now.plus(lease)));
            claimed.add(head.timer());
        }
        return claimed;
    }

    @Override public synchronized void release(Timer timer, Duration delay) {
        Slot current = live.get(timer.cartId());
        if (current != null && current.timer().equals(timer)) put(new Slot(timer, clock.now().plus(delay)));
    }

    @Override public synchronized void ack(Timer timer) {
        Slot current = live.get(timer.cartId());
        if (current != null && current.timer().equals(timer)) live.remove(timer.cartId());
    }

    @Override public synchronized Set<String> existing(int shard, Collection<String> cartIds) {
        Set<String> out = new HashSet<>();
        for (String id : cartIds) if (live.containsKey(id)) out.add(id);
        return out;
    }

    /** The earliest score: a due time, or the lease expiry of a claimed timer. */
    public synchronized Optional<Instant> nextDueAt() {
        return Optional.ofNullable(liveHead()).map(Slot::score);
    }

    public synchronized int size() { return live.size(); }

    public synchronized void clear() {
        live.clear();
        queue.clear();
    }

    /** (version, offsetIndex) strictly greater; CHECK_ABANDON already carries offsetIndex -1. */
    private static boolean greater(Timer a, Timer b) {
        if (a.version() != b.version()) return a.version() > b.version();
        return a.offsetIndex() > b.offsetIndex();
    }

    private void put(Slot slot) {
        live.put(slot.timer().cartId(), slot);
        queue.add(slot);
    }

    private Slot liveHead() {
        while (!queue.isEmpty() && live.get(queue.peek().timer().cartId()) != queue.peek()) queue.poll();
        return queue.peek();
    }
}
```

Create `main/inmemory/InMemoryWatermark.java`:

```java
package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Same semantics as the Redis watermark scripts, timed by the given clock. setLagging pins a partition's
 * watermark (a stalled detector) regardless of publishes until clearLag.
 */
public final class InMemoryWatermark implements Watermark {
    static final Duration STALE_AFTER = Duration.ofSeconds(5);

    private record Entry(long generation, Instant eventTime, Instant updatedAt) {}

    private final Clock clock;
    private final Map<Integer, Entry> entries = new HashMap<>();
    private final Map<Integer, Instant> lagging = new HashMap<>();

    public InMemoryWatermark(Clock clock) { this.clock = clock; }

    @Override public synchronized void publish(int partition, long generation, Instant eventTime) {
        Entry stored = entries.get(partition);
        if (stored != null && generation < stored.generation()) return;
        Instant time = stored != null && generation == stored.generation() && stored.eventTime().isAfter(eventTime)
            ? stored.eventTime() : eventTime;
        entries.put(partition, new Entry(generation, time, clock.now()));
    }

    @Override public synchronized Instant current(int srcPartition) {
        if (srcPartition >= 0) return valueOf(srcPartition);
        Instant min = null;
        for (Integer p : allPartitions()) {
            Instant v = valueOf(p);
            if (min == null || v.isBefore(min)) min = v;
        }
        return min == null ? Instant.EPOCH : min;
    }

    @Override public Instant now() { return clock.now(); }

    public synchronized void setLagging(int partition, Instant eventTime) { lagging.put(partition, eventTime); }

    public synchronized void clearLag() { lagging.clear(); }

    private Instant valueOf(int partition) {
        Instant pinned = lagging.get(partition);
        if (pinned != null) return pinned;
        Entry e = entries.get(partition);
        if (e == null || Duration.between(e.updatedAt(), clock.now()).compareTo(STALE_AFTER) > 0) return Instant.EPOCH;
        return e.eventTime();
    }

    private Set<Integer> allPartitions() {
        Set<Integer> all = new HashSet<>(entries.keySet());
        all.addAll(lagging.keySet());
        return all;
    }
}
```

Create `main/inmemory/InMemoryIntentQueue.java`:

```java
package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.ports.IntentPublisher;
import java.util.ArrayList;
import java.util.List;

public final class InMemoryIntentQueue implements IntentPublisher {
    private final List<ReminderIntent> intents = new ArrayList<>();

    @Override public synchronized void publish(ReminderIntent intent) { intents.add(intent); }

    /** Removes and returns everything, in publish order. */
    public synchronized List<ReminderIntent> drain() {
        List<ReminderIntent> out = List.copyOf(intents);
        intents.clear();
        return out;
    }

    public synchronized int size() { return intents.size(); }
}
```

Create `main/inmemory/InMemorySendLedger.java`:

```java
package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.SendLedger;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Same state machine as the DynamoDB send-ledger: fenced leases, tokens, and a retry index of non-final rows. */
public final class InMemorySendLedger implements SendLedger {
    private static final String SENDING = "SENDING";
    private static final String RETRYING = "RETRYING";

    private record Row(String status, String token, Instant leaseUntil, Instant sendBy, int srcPartition,
                       int attempts, Instant nextAttemptAt, String reason) {
        boolean isFinal() { return !status.equals(SENDING) && !status.equals(RETRYING); }
    }

    private final Duration lease;
    private final int shards;
    private final Map<String, Row> rows = new HashMap<>();

    public InMemorySendLedger(Duration lease, int shards) {
        this.lease = lease;
        this.shards = shards;
    }

    @Override public synchronized ClaimResult claim(String key, Instant sendBy, int srcPartition, Instant now) {
        Row row = rows.get(key);
        if (row != null && row.isFinal()) return new ClaimResult.NotClaimed("final");
        boolean takeover = row != null && (
            (row.status().equals(RETRYING) && !row.nextAttemptAt().isAfter(now))
            || (row.status().equals(SENDING) && !row.leaseUntil().isAfter(now)));
        if (row != null && !takeover) return new ClaimResult.NotClaimed("leased");
        Instant until = now.plus(lease);
        String token = UUID.randomUUID().toString();
        Row claimed = row == null
            ? new Row(SENDING, token, until, sendBy, srcPartition, 1, until, null)
            : new Row(SENDING, token, until, row.sendBy(), row.srcPartition(), row.attempts() + 1, until, row.reason());
        rows.put(key, claimed);
        return new ClaimResult.Claimed(token, claimed.attempts(), claimed.sendBy(), claimed.srcPartition(), until);
    }

    @Override public synchronized boolean markRetry(String key, String token, Instant nextAt) {
        Row row = held(key, token);
        if (row == null) return false;
        rows.put(key, new Row(RETRYING, null, null, row.sendBy(), row.srcPartition(), row.attempts(), nextAt, row.reason()));
        return true;
    }

    @Override public synchronized boolean finish(String key, String token, OutcomeKind outcome, String reason) {
        if (outcome == OutcomeKind.ABANDONED) throw new IllegalArgumentException("ABANDONED is not a ledger outcome");
        Row row = held(key, token);
        if (row == null) return false;
        rows.put(key, new Row(outcome.name(), null, null, row.sendBy(), row.srcPartition(), row.attempts(), null, reason));
        return true;
    }

    @Override public synchronized List<DueRetry> dueRetries(int shard, Instant now, int limit) {
        return rows.entrySet().stream()
            .filter(e -> !e.getValue().isFinal()
                && !e.getValue().nextAttemptAt().isAfter(now)
                && Shards.of(LedgerKey.parse(e.getKey()).cartId(), shards) == shard)
            .sorted(Comparator.comparing((Map.Entry<String, Row> e) -> e.getValue().nextAttemptAt())
                .thenComparing(Map.Entry::getKey))
            .limit(limit)
            .map(e -> new DueRetry(e.getKey(), e.getValue().srcPartition()))
            .toList();
    }

    @Override public synchronized boolean reopen(String key, Instant now) {
        Row row = rows.get(key);
        if (row == null || !row.status().equals(OutcomeKind.DEAD.name())) return false;
        rows.put(key, new Row(RETRYING, null, null, row.sendBy(), row.srcPartition(), 0, now, null));
        return true;
    }

    @Override public synchronized int highestOffsetIndex(String cartId, long version) {
        int highest = -1;
        for (String key : rows.keySet()) {
            LedgerKey k = LedgerKey.parse(key);
            if (k.cartId().equals(cartId) && k.version() == version) highest = Math.max(highest, k.offsetIndex());
        }
        return highest;
    }

    /** Earliest nextAttemptAt over non-final rows (a SENDING row's is its lease expiry). */
    public synchronized Optional<Instant> nextRetryAt() {
        return rows.values().stream().filter(r -> !r.isFinal()).map(Row::nextAttemptAt).min(Instant::compareTo);
    }

    public synchronized int size() { return rows.size(); }

    /** The row's status name (SENDING, RETRYING, SENT, SKIPPED_LATE, CANCELLED, DEAD), empty if absent. */
    public synchronized Optional<String> status(String key) {
        return Optional.ofNullable(rows.get(key)).map(Row::status);
    }

    private Row held(String key, String token) {
        Row row = rows.get(key);
        return row != null && row.status().equals(SENDING) && row.token().equals(token) ? row : null;
    }
}
```

Create `main/inmemory/InMemoryOutcomeRecorder.java`:

```java
package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import java.util.ArrayList;
import java.util.List;

public final class InMemoryOutcomeRecorder implements OutcomeRecorder {
    private final List<Outcome> outcomes = new ArrayList<>();

    @Override public synchronized void record(Outcome outcome) { outcomes.add(outcome); }

    public synchronized List<Outcome> all() { return List.copyOf(outcomes); }
}
```

Create `main/inmemory/RecordingNotificationSink.java`:

```java
package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.NotificationSink;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** Records what would have been sent. Never sends. Outcomes can be scripted for failure scenarios. */
public final class RecordingNotificationSink implements NotificationSink {
    public record Sent(ReminderMessage message, Instant sentAt) {}

    private final Clock clock;
    private final List<Sent> sent = new ArrayList<>();
    private final Deque<SendResult> scripted = new ArrayDeque<>();
    private int attempts = 0;

    public RecordingNotificationSink(Clock clock) { this.clock = clock; }

    /** The next calls to send return these results in order, then SENT. */
    public synchronized void scriptOutcomes(SendResult... results) {
        scripted.addAll(List.of(results));
    }

    @Override public synchronized SendResult send(ReminderMessage message) {
        attempts++;
        SendResult result = scripted.isEmpty() ? SendResult.SENT : scripted.poll();
        if (result == SendResult.SENT) sent.add(new Sent(message, clock.now()));
        return result;
    }

    /** Successful sends only, in order. */
    public synchronized List<Sent> sent() { return List.copyOf(sent); }

    /** Every send call, including failures. */
    public synchronized int attempts() { return attempts; }
}
```

Create `main/inmemory/InMemoryDeadLetterQueue.java`:

```java
package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.ports.DeadLetterQueue;
import java.util.ArrayList;
import java.util.List;

public final class InMemoryDeadLetterQueue implements DeadLetterQueue {
    private final List<DeadLetter> letters = new ArrayList<>();

    @Override public synchronized void add(DeadLetter letter) { letters.add(letter); }

    /** Removes and returns everything, for replay. */
    public synchronized List<DeadLetter> drain() {
        List<DeadLetter> out = List.copyOf(letters);
        letters.clear();
        return out;
    }

    public synchronized int size() { return letters.size(); }
}
```

Create `main/inmemory/UnlimitedSendBudget.java`:

```java
package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.ports.SendBudget;

public final class UnlimitedSendBudget implements SendBudget {
    @Override public boolean tryAcquire(Lane lane) { return true; }
}
```

- [ ] **Step 12: Run the contract tests to verify they pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.inmemory.*' --console=plain`
Expected: PASS (`InMemoryCartStateStoreContractTest` 15, `PriorityQueueTimerStoreContractTest` 15, `InMemoryWatermarkContractTest` 10, `InMemorySendLedgerContractTest` 14).

- [ ] **Step 13: Write the failing Metrics concurrency test**

Create `test/core/MetricsTest.java`:

```java
package com.quince.cartrecovery.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class MetricsTest {

    @Test
    void countsFromManyThreadsWithoutLosingIncrements() throws Exception {
        Metrics metrics = new Metrics();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 1000; i++) pool.submit(() -> metrics.increment("x"));
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);
        }
        assertEquals(1000, metrics.get("x"));
        assertEquals(0, metrics.get("never"));
    }

    @Test
    void snapshotIsSortedByName() {
        Metrics metrics = new Metrics();
        metrics.increment("b");
        metrics.increment("a");
        assertEquals(List.of("a", "b"), List.copyOf(metrics.snapshot().keySet()));
    }
}
```

- [ ] **Step 14: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.core.MetricsTest' --console=plain`
Expected: `countsFromManyThreadsWithoutLosingIncrements` FAILS (fewer than 1000 counted, or a `ConcurrentModificationException` from the unsynchronized `TreeMap`). A data race can occasionally pass; Step 15 is required either way.

- [ ] **Step 15: Make Metrics thread-safe**

Replace `main/core/Metrics.java`:

```java
package com.quince.cartrecovery.core;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/** Named counters, safe to use from many threads. Production: emitted to the metrics backend from every stage. */
public final class Metrics {
    private final Map<String, LongAdder> counters = new ConcurrentHashMap<>();

    public void increment(String name) {
        counters.computeIfAbsent(name, k -> new LongAdder()).increment();
    }

    public long get(String name) {
        LongAdder c = counters.get(name);
        return c == null ? 0L : c.sum();
    }

    /** A read-only copy, sorted by name. */
    public Map<String, Long> snapshot() {
        Map<String, Long> copy = new TreeMap<>();
        counters.forEach((k, v) -> copy.put(k, v.sum()));
        return Collections.unmodifiableMap(copy);
    }
}
```

- [ ] **Step 16: Run the full suite**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`, 0 failures (legacy tests, model tests, contract tests, `MetricsTest`).

- [ ] **Step 17: Commit**

```bash
git add -A src
git commit -m "$(cat <<'MSG'
Add production-infra model, ports, in-memory adapters, and contract tests

Frozen contracts from the master plan: fenced send ledger, monotonic timer
store with leases, per-partition watermark, intent publisher, outcome
recorder, send budget. In-memory adapters implement the same semantics and
pass the shared contract tests that the infra adapters will reuse. Metrics
is now thread-safe.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task A2: `AbandonmentDetector` (timer first) and `ReminderScheduler` (gate, release delay, re-arm, `sendBy`, `endSequence`, poison, `ABANDONED` outcomes)

**Model:** sonnet (spec §6.2 is precise and this task carries full code). Reviewer: sonnet.

**Files:**
- Create: `main/core/AbandonmentDetector.java`, `main/core/ReminderScheduler.java`
- Test: `test/core/AbandonmentDetectorTest.java`, `test/core/ReminderSchedulerTest.java`
- The old classes and tests stay in `legacy` untouched until A4.

**Interfaces:**
- Consumes (A1): `CartStateStore.applyEvent/get/markAbandoned/endSequence`, `TimerStore.upsert/remove`, `Watermark.current/now`, `IntentPublisher.publish`, `OutcomeRecorder.record`, `Timer.checkAbandon(cartId, version, dueAt, srcPartition)`, `Timer.reminder(cartId, version, offsetIndex, dueAt, srcPartition)`, `CartRecord.startsWith(start, now, frequencyWindow)`, `ReminderIntent`, `LedgerKey`, `Outcome`, `OutcomeKind.ABANDONED`, `TimerDecision.Ack/Release`, `DispatchConfig.clockSkew()`, `ReminderPolicy.eligible(CartRecord, Instant)`, `Metrics`; test adapters `InMemoryCartStateStore(RecoveryConfig, int)`, `PriorityQueueTimerStore(Clock, Duration)`, `InMemoryWatermark(Clock)`, `InMemoryIntentQueue`, `InMemoryOutcomeRecorder`, `FakeClock`.
- Produces (master §1.3): `AbandonmentDetector(RecoveryConfig, CartStateStore, TimerStore, ArmAssigner, Metrics)` with `void handle(CartEvent event, int srcPartition)`; `ReminderScheduler(RecoveryConfig, DispatchConfig, CartStateStore, TimerStore, Watermark, IntentPublisher, OutcomeRecorder, Metrics)` with `TimerDecision onTimer(Timer timer)`. The caller acks on `Ack`, calls `release(timer, delay)` on `Release`, and leaves the timer un-acked when `onTimer` throws. Metrics emitted: `events.handled`, `events.ignored`, `timers.held`, `timers.stale`, `timers.conflict`, `timers.wrong_status`, `timers.poison`, `carts.abandoned`, `carts.holdout`, `carts.cap_reached`, `reminders.published`.

Behaviour pinned by the tests (spec §5.4, §6.2):
- Detector: edit or resume → `upsert(CHECK_ABANDON at occurredAt + window, srcPartition)` then `applyEvent`; purchase or clear → `remove(cartId, version)` then `applyEvent`; stale `applyEvent` → `events.ignored`.
- `CHECK_ABANDON`: if `watermark.current(timer.srcPartition) < dueAt + CLOCK_SKEW` → `Release(clamp(dueAt + CLOCK_SKEW − W, 1 s, 60 s))`, or 60 s when `W` is `EPOCH`; otherwise consistent reload (version mismatch → `timers.stale`). `ACTIVE`: pruned starts plus this start, eligibility, `markAbandoned` (false → `timers.conflict`, drop), `ABANDONED` outcome, eligible → `upsert(REMINDER 0)`. `ABANDONED` at the same version (redelivery after a crash between writes): `ABANDONED` outcome again, eligible → `upsert(REMINDER 0)` (monotonic, so it never regresses a later reminder). `CLOSED` → `timers.wrong_status`.
- `REMINDER i`: `i` outside `OFFSETS` → `timers.poison`, `Ack`, before any read; not `ABANDONED` at the same version → stale or wrong status; else publish `ReminderIntent(key, cartId, version, i, record.srcPartition, scheduledFor = dueAt, sendBy = dueAt + bound[i])`, then `upsert(REMINDER i+1)` or `endSequence`. No lateness check here: the dispatcher owns `sendBy`. Reminder timers are not gated.
- Time for eligibility and outcomes is `watermark.now()` (Redis `TIME` in production); the scheduler has no `Clock`.

- [ ] **Step 1: Write the failing detector test**

Create `test/core/AbandonmentDetectorTest.java` (the legacy cases, adapted: `handle` takes a partition; the stale-event case now expects the stale timer the timer-first order leaves; the conflict-retry expectation is gone; two new cases prove the timer is written before the cart):

```java
package com.quince.cartrecovery.core;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.PriorityQueueTimerStore;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerKind;
import com.quince.cartrecovery.ports.CartStateStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbandonmentDetectorTest {
    private FakeClock clock;
    private InMemoryCartStateStore store;
    private PriorityQueueTimerStore timers;
    private Metrics metrics;
    private AbandonmentDetector detector;

    @BeforeEach
    void setUp() {
        clock = new FakeClock(T0);
        store = new InMemoryCartStateStore(RecoveryConfig.defaults(), 4);
        timers = new PriorityQueueTimerStore(clock, Duration.ofSeconds(90));
        metrics = new Metrics();
        detector = new AbandonmentDetector(RecoveryConfig.defaults(), store, timers, key -> Arm.TREATMENT, metrics);
    }

    private List<Timer> dueAt(Instant t) {
        clock.set(t);
        return timers.claimDue(100);
    }

    /** A cart store whose applyEvent fails, as if the process died between the timer write and the cart write. */
    private CartStateStore failingApply() {
        return (CartStateStore) Proxy.newProxyInstance(CartStateStore.class.getClassLoader(),
            new Class<?>[] {CartStateStore.class}, (proxy, method, args) -> {
                if (method.getName().equals("applyEvent")) throw new IllegalStateException("cart store down");
                try {
                    return method.invoke(store, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
    }

    @Test
    void editCreatesAnActiveRecordAndACheckTimerCarryingTheSourcePartition() {
        detector.handle(edited(1, min(0)), 3);

        CartRecord r = store.get(CART).orElseThrow();
        assertEquals(CartStatus.ACTIVE, r.status());
        assertEquals(1, r.version());
        assertEquals(T0, r.lastActivityAt());
        assertEquals(ITEMS, r.items());
        assertEquals(3, r.srcPartition());
        assertEquals(List.of(Timer.checkAbandon(CART, 1, at(min(30)), 3)), dueAt(at(min(30))));
        assertEquals(1, metrics.get("events.handled"));
    }

    @Test
    void laterEditResetsTheClockAndReplacesTheTimer() {
        detector.handle(edited(1, min(0)), 0);
        detector.handle(edited(2, min(20)), 0);

        assertTrue(dueAt(at(min(30))).isEmpty());
        List<Timer> due = dueAt(at(min(50)));
        assertEquals(1, due.size());
        assertEquals(2, due.get(0).version());
        assertEquals(TimerKind.CHECK_ABANDON, due.get(0).kind());
    }

    @Test
    void duplicateAndOutOfOrderEventsAreIgnored() {
        detector.handle(edited(2, min(20)), 0);
        detector.handle(edited(2, min(20)), 0);
        detector.handle(edited(1, min(0)), 0);

        assertEquals(2, store.get(CART).orElseThrow().version());
        assertEquals(at(min(20)), store.get(CART).orElseThrow().lastActivityAt());
        assertEquals(2, metrics.get("events.ignored"));
        assertEquals(1, timers.size());
        assertEquals(2, dueAt(at(min(50))).get(0).version());
    }

    @Test
    void purchaseClosesTheRecordAndRemovesTheTimer() {
        detector.handle(edited(1, min(0)), 0);
        detector.handle(purchased(2, min(10)), 0);

        assertEquals(CartStatus.CLOSED, store.get(CART).orElseThrow().status());
        assertEquals(0, timers.size());
    }

    @Test
    void clearClosesTheRecordAndRemovesTheTimer() {
        detector.handle(edited(1, min(0)), 0);
        detector.handle(cleared(2, min(10)), 0);

        assertEquals(CartStatus.CLOSED, store.get(CART).orElseThrow().status());
        assertEquals(0, timers.size());
    }

    @Test
    void resumeCountsAsActivityAndKeepsItems() {
        detector.handle(edited(1, min(0)), 0);
        detector.handle(resumed(2, min(15)), 0);

        CartRecord r = store.get(CART).orElseThrow();
        assertEquals(CartStatus.ACTIVE, r.status());
        assertEquals(ITEMS, r.items());
        assertEquals(at(min(15)), r.lastActivityAt());
        assertEquals(List.of(Timer.checkAbandon(CART, 2, at(min(45)), 0)), dueAt(at(min(45))));
    }

    @Test
    void aStaleEditAfterAPurchaseLeavesOnlyAStaleTimerThatFiresAsStale() {
        detector.handle(purchased(5, min(0)), 0);
        detector.handle(edited(3, min(1)), 0);

        CartRecord r = store.get(CART).orElseThrow();
        assertEquals(CartStatus.CLOSED, r.status());
        assertEquals(5, r.version());
        assertEquals(1, metrics.get("events.ignored"));
        assertEquals(List.of(Timer.checkAbandon(CART, 3, at(min(31)), 0)), dueAt(at(min(31))));
    }

    @Test
    void redeliveredEditAfterAbandonmentLeavesTheStateAndTheReminderAlone() {
        detector.handle(edited(1, min(0)), 0);
        CartRecord active = store.get(CART).orElseThrow();
        store.markAbandoned(active, List.of(T0), true);
        timers.upsert(Timer.reminder(CART, 1, 0, at(min(30)), 0));

        detector.handle(edited(1, min(0)), 0);

        assertEquals(CartStatus.ABANDONED, store.get(CART).orElseThrow().status());
        assertEquals(TimerKind.REMINDER, dueAt(at(min(30))).get(0).kind());
    }

    @Test
    void assignsArmOnFirstSightAndKeepsIt() {
        AbandonmentDetector holdoutDetector = new AbandonmentDetector(
            RecoveryConfig.defaults(), store, timers, key -> Arm.HOLDOUT, metrics);
        holdoutDetector.handle(edited(1, min(0)), 0);
        detector.handle(edited(2, min(1)), 0);

        assertEquals(Arm.HOLDOUT, store.get(CART).orElseThrow().arm());
    }

    @Test
    void anEditWritesTheTimerBeforeTheCart() {
        AbandonmentDetector crashing = new AbandonmentDetector(
            RecoveryConfig.defaults(), failingApply(), timers, key -> Arm.TREATMENT, metrics);

        assertThrows(IllegalStateException.class, () -> crashing.handle(edited(1, min(0)), 2));

        assertTrue(store.get(CART).isEmpty());
        assertEquals(List.of(Timer.checkAbandon(CART, 1, at(min(30)), 2)), dueAt(at(min(30))));
    }

    @Test
    void aPurchaseRemovesTheTimerBeforeTheCart() {
        detector.handle(edited(1, min(0)), 0);
        AbandonmentDetector crashing = new AbandonmentDetector(
            RecoveryConfig.defaults(), failingApply(), timers, key -> Arm.TREATMENT, metrics);

        assertThrows(IllegalStateException.class, () -> crashing.handle(purchased(2, min(10)), 0));

        assertEquals(CartStatus.ACTIVE, store.get(CART).orElseThrow().status());
        assertEquals(0, timers.size());
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.core.AbandonmentDetectorTest' --console=plain`
Expected: FAIL at `compileTestJava` with `cannot find symbol: class AbandonmentDetector` (it moved to `legacy.core` in A1).

- [ ] **Step 3: Write the detector**

Create `main/core/AbandonmentDetector.java`:

```java
package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.ArmAssigner;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.TimerStore;

/**
 * Consumes cart events. Writes the timer first (monotonic upsert or conditional remove), then the conditional
 * cart update, so a crash between the two leaves at most a missing or extra timer: a stale timer is rejected by
 * the monotonic upsert or dropped on fire, and a redelivered event repeats both writes.
 */
public final class AbandonmentDetector {
    private final RecoveryConfig config;
    private final CartStateStore store;
    private final TimerStore timers;
    private final ArmAssigner arms;
    private final Metrics metrics;

    public AbandonmentDetector(RecoveryConfig config, CartStateStore store, TimerStore timers,
                               ArmAssigner arms, Metrics metrics) {
        this.config = config;
        this.store = store;
        this.timers = timers;
        this.arms = arms;
        this.metrics = metrics;
    }

    /** srcPartition is the cart-events partition the event was actually consumed from. */
    public void handle(CartEvent event, int srcPartition) {
        switch (event) {
            case CartEvent.CartEdited e -> timers.upsert(checkAbandon(e, srcPartition));
            case CartEvent.CartResumed e -> timers.upsert(checkAbandon(e, srcPartition));
            case CartEvent.CartCleared e -> timers.remove(e.cartId(), e.version());
            case CartEvent.CartPurchased e -> timers.remove(e.cartId(), e.version());
        }
        if (store.applyEvent(event, arms.assign(event.shopperKey()), srcPartition).isEmpty()) {
            metrics.increment("events.ignored");
            return;
        }
        metrics.increment("events.handled");
    }

    private Timer checkAbandon(CartEvent e, int srcPartition) {
        return Timer.checkAbandon(e.cartId(), e.version(), e.occurredAt().plus(config.window()), srcPartition);
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.core.AbandonmentDetectorTest' --console=plain`
Expected: PASS (11 tests).

- [ ] **Step 5: Write the failing scheduler test**

Create `test/core/ReminderSchedulerTest.java`. Per spec §8.4 the duplicate-timer case now asserts that the same key is published twice; the send-side dedupe (`dispatch.duplicate`) is asserted in A3's `DispatcherTest` and in verifier scenario 6 (A4):

```java
package com.quince.cartrecovery.core;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.InMemoryIntentQueue;
import com.quince.cartrecovery.inmemory.InMemoryOutcomeRecorder;
import com.quince.cartrecovery.inmemory.InMemoryWatermark;
import com.quince.cartrecovery.inmemory.PriorityQueueTimerStore;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.DispatchConfig;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerDecision;
import com.quince.cartrecovery.ports.CartStateStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReminderSchedulerTest {
    private static final TimerDecision ACK = new TimerDecision.Ack();
    private static final int SHARDS = 4;

    private FakeClock clock;
    private InMemoryCartStateStore store;
    private PriorityQueueTimerStore timers;
    private InMemoryWatermark watermark;
    private InMemoryIntentQueue intents;
    private InMemoryOutcomeRecorder outcomes;
    private Metrics metrics;
    private ReminderScheduler scheduler;

    @BeforeEach
    void setUp() {
        clock = new FakeClock(T0);
        store = new InMemoryCartStateStore(RecoveryConfig.defaults(), SHARDS);
        timers = new PriorityQueueTimerStore(clock, Duration.ofSeconds(90));
        watermark = new InMemoryWatermark(clock);
        intents = new InMemoryIntentQueue();
        outcomes = new InMemoryOutcomeRecorder();
        metrics = new Metrics();
        scheduler = scheduler(RecoveryConfig.defaults(), store);
    }

    private ReminderScheduler scheduler(RecoveryConfig config, CartStateStore cartStore) {
        return new ReminderScheduler(config, DispatchConfig.defaults(), cartStore, timers, watermark, intents, outcomes, metrics);
    }

    /** Moves the clock and marks partition 0 caught up to it. */
    private void caughtUpAt(Instant t) {
        clock.set(t);
        watermark.publish(0, 1, t);
    }

    private CartRecord active(long version, Instant at, Arm arm, int partition) {
        return store.applyEvent(new CartEvent.CartEdited(CART, SHOPPER, version, at, ITEMS, "Ada"), arm, partition)
            .orElseThrow();
    }

    private CartRecord abandoned(long version, int partition) {
        CartRecord r = active(version, T0, Arm.TREATMENT, partition);
        store.markAbandoned(r, List.of(T0), true);
        return store.get(CART).orElseThrow();
    }

    private static Timer check(long version) {
        return Timer.checkAbandon(CART, version, at(min(30)), 0);
    }

    private List<String> openCarts() {
        return store.openCartIds(Shards.of(CART, SHARDS), clock.now()).toList();
    }

    private List<Outcome> abandonedOutcomes() {
        return outcomes.all().stream().filter(o -> o.kind() == OutcomeKind.ABANDONED).toList();
    }

    @Test
    void checkAbandonMarksAbandonedRecordsTheOutcomeAndArmsTheFirstReminder() {
        active(1, T0, Arm.TREATMENT, 0);
        caughtUpAt(at(min(30)).plusSeconds(5));

        assertEquals(ACK, scheduler.onTimer(check(1)));

        CartRecord r = store.get(CART).orElseThrow();
        assertEquals(CartStatus.ABANDONED, r.status());
        assertEquals(List.of(T0), r.sequenceStarts());
        assertEquals(List.of(new Outcome(null, CART, 1, Arm.TREATMENT, OutcomeKind.ABANDONED, clock.now(), 0)),
            outcomes.all());
        assertEquals(List.of(Timer.reminder(CART, 1, 0, at(min(30)), 0)), timers.claimDue(10));
        assertEquals(1, metrics.get("carts.abandoned"));
    }

    @Test
    void checkAbandonWaitsUntilTheWatermarkPassesDueAtPlusClockSkew() {
        active(1, T0, Arm.TREATMENT, 0);
        caughtUpAt(at(min(30)));

        assertEquals(new TimerDecision.Release(Duration.ofSeconds(5)), scheduler.onTimer(check(1)));
        assertEquals(CartStatus.ACTIVE, store.get(CART).orElseThrow().status());
        assertEquals(1, metrics.get("timers.held"));

        caughtUpAt(at(min(30)).plusSeconds(5));
        assertEquals(ACK, scheduler.onTimer(check(1)));
        assertEquals(CartStatus.ABANDONED, store.get(CART).orElseThrow().status());
    }

    @Test
    void theReleaseDelayGrowsWithTheLagBetweenOneAndSixtySeconds() {
        active(1, T0, Arm.TREATMENT, 0);
        Instant needed = at(min(30)).plusSeconds(5);
        clock.set(needed);

        assertEquals(new TimerDecision.Release(Duration.ofSeconds(60)), scheduler.onTimer(check(1)));
        watermark.publish(0, 1, needed.minus(Duration.ofMinutes(10)));
        assertEquals(new TimerDecision.Release(Duration.ofSeconds(60)), scheduler.onTimer(check(1)));
        watermark.publish(0, 1, needed.minusSeconds(20));
        assertEquals(new TimerDecision.Release(Duration.ofSeconds(20)), scheduler.onTimer(check(1)));
        watermark.publish(0, 1, needed.minusMillis(200));
        assertEquals(new TimerDecision.Release(Duration.ofSeconds(1)), scheduler.onTimer(check(1)));
        assertEquals(CartStatus.ACTIVE, store.get(CART).orElseThrow().status());
    }

    @Test
    void aTimerWithoutAPartitionGatesOnTheSlowestPartition() {
        active(1, T0, Arm.TREATMENT, -1);
        Instant needed = at(min(30)).plusSeconds(5);
        clock.set(needed);
        watermark.publish(0, 1, needed);
        watermark.publish(1, 1, needed.minusSeconds(3));

        assertEquals(new TimerDecision.Release(Duration.ofSeconds(3)),
            scheduler.onTimer(Timer.checkAbandon(CART, 1, at(min(30)), -1)));
    }

    @Test
    void staleVersionTimerIsDropped() {
        active(2, T0, Arm.TREATMENT, 0);
        caughtUpAt(at(min(31)));

        assertEquals(ACK, scheduler.onTimer(check(1)));

        assertEquals(CartStatus.ACTIVE, store.get(CART).orElseThrow().status());
        assertEquals(0, timers.size());
        assertEquals(1, metrics.get("timers.stale"));
        assertEquals(List.of(), outcomes.all());
    }

    @Test
    void checkAbandonOnAClosedCartIsDropped() {
        active(1, T0, Arm.TREATMENT, 0);
        store.applyEvent(new CartEvent.CartPurchased(CART, SHOPPER, 2, at(min(10))), Arm.TREATMENT, 0);
        caughtUpAt(at(min(31)));

        assertEquals(ACK, scheduler.onTimer(check(2)));

        assertEquals(0, timers.size());
        assertEquals(1, metrics.get("timers.wrong_status"));
    }

    @Test
    void aNewerEventLandingBetweenReloadAndAbandonmentWins() {
        active(1, T0, Arm.TREATMENT, 0);
        CartStateStore racing = (CartStateStore) Proxy.newProxyInstance(CartStateStore.class.getClassLoader(),
            new Class<?>[] {CartStateStore.class}, (proxy, method, args) -> {
                try {
                    Object result = method.invoke(store, args);
                    if (method.getName().equals("get")) {
                        store.applyEvent(new CartEvent.CartEdited(CART, SHOPPER, 2, at(min(29)), ITEMS), Arm.TREATMENT, 0);
                    }
                    return result;
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
        caughtUpAt(at(min(31)));

        assertEquals(ACK, scheduler(RecoveryConfig.defaults(), racing).onTimer(check(1)));

        assertEquals(CartStatus.ACTIVE, store.get(CART).orElseThrow().status());
        assertEquals(2, store.get(CART).orElseThrow().version());
        assertEquals(1, metrics.get("timers.conflict"));
        assertEquals(List.of(), outcomes.all());
        assertEquals(0, timers.size());
    }

    @Test
    void holdoutCartIsAbandonedRecordedAndClosedInTheOpenIndexWithoutAReminder() {
        active(1, T0, Arm.HOLDOUT, 0);
        caughtUpAt(at(min(31)));

        scheduler.onTimer(check(1));

        assertEquals(CartStatus.ABANDONED, store.get(CART).orElseThrow().status());
        assertEquals(0, timers.size());
        assertEquals(1, metrics.get("carts.holdout"));
        assertEquals(Arm.HOLDOUT, abandonedOutcomes().get(0).arm());
        assertEquals(List.of(), openCarts());
    }

    @Test
    void frequencyCapStopsFurtherSequences() {
        CartRecord first = active(1, T0, Arm.TREATMENT, 0);
        store.markAbandoned(first, List.of(T0), true);
        active(2, at(min(40)), Arm.TREATMENT, 0);
        caughtUpAt(at(min(70)).plusSeconds(5));

        scheduler(RecoveryConfig.defaults().withFrequencyCap(1), store)
            .onTimer(Timer.checkAbandon(CART, 2, at(min(70)), 0));

        CartRecord r = store.get(CART).orElseThrow();
        assertEquals(CartStatus.ABANDONED, r.status());
        assertEquals(List.of(T0, at(min(40))), r.sequenceStarts());
        assertEquals(0, timers.size());
        assertEquals(1, metrics.get("carts.cap_reached"));
        assertEquals(List.of(), openCarts());
    }

    @Test
    void aRedeliveredCheckOnAnAbandonedCartReArmsTheFirstReminderAndRecordsTheOutcomeAgain() {
        abandoned(1, 0);
        caughtUpAt(at(min(32)));

        assertEquals(ACK, scheduler.onTimer(check(1)));
        assertEquals(List.of(Timer.reminder(CART, 1, 0, at(min(30)), 0)), timers.claimDue(10));

        assertEquals(ACK, scheduler.onTimer(check(1)));
        assertEquals(1, timers.size());
        assertEquals(2, abandonedOutcomes().size());
        assertEquals(List.of(T0), store.get(CART).orElseThrow().sequenceStarts());
        assertEquals(0, metrics.get("timers.wrong_status"));
    }

    @Test
    void aRedeliveredCheckNeverRegressesALaterReminder() {
        abandoned(1, 0);
        timers.upsert(Timer.reminder(CART, 1, 1, at(hrs(1)), 0));
        caughtUpAt(at(min(40)));

        scheduler.onTimer(check(1));

        clock.set(at(hrs(1)));
        assertEquals(List.of(Timer.reminder(CART, 1, 1, at(hrs(1)), 0)), timers.claimDue(10));
    }

    @Test
    void aRedeliveredCheckOnAHoldoutCartRecordsTheOutcomeButArmsNothing() {
        CartRecord r = active(1, T0, Arm.HOLDOUT, 0);
        store.markAbandoned(r, List.of(T0), false);
        caughtUpAt(at(min(32)));

        scheduler.onTimer(check(1));

        assertEquals(1, abandonedOutcomes().size());
        assertEquals(0, timers.size());
    }

    @Test
    void reminderPublishesAnIntentStampedWithSendByAndChainsTheNextOffset() {
        abandoned(1, 5);
        clock.set(at(min(30)));

        assertEquals(ACK, scheduler.onTimer(Timer.reminder(CART, 1, 0, at(min(30)), 5)));

        assertEquals(List.of(new ReminderIntent("cart-1:1:0", CART, 1, 0, 5, at(min(30)), at(min(35)))), intents.drain());
        clock.set(at(hrs(1)));
        assertEquals(List.of(Timer.reminder(CART, 1, 1, at(hrs(1)), 5)), timers.claimDue(10));
        assertEquals(1, metrics.get("reminders.published"));
    }

    @Test
    void reminderTimersAreNotGatedByTheWatermark() {
        abandoned(1, 0);
        clock.set(at(min(30)));

        scheduler.onTimer(Timer.reminder(CART, 1, 0, at(min(30)), 0));

        assertEquals(1, intents.size());
    }

    @Test
    void theLastReminderEndsTheSequence() {
        abandoned(1, 0);
        clock.set(at(hrs(24)));
        assertEquals(List.of(CART), openCarts());

        scheduler.onTimer(Timer.reminder(CART, 1, 2, at(hrs(24)), 0));

        assertEquals(at(hrs(24)).plus(min(30)), intents.drain().get(0).sendBy());
        assertEquals(0, timers.size());
        assertEquals(List.of(), openCarts());
    }

    @Test
    void aLateReminderIsStillPublishedBecauseTheDispatcherOwnsLateness() {
        abandoned(1, 0);
        clock.set(at(min(36)));

        scheduler.onTimer(Timer.reminder(CART, 1, 0, at(min(30)), 0));

        assertEquals(at(min(35)), intents.drain().get(0).sendBy());
        assertEquals(1, timers.size());
    }

    @Test
    void aDuplicateReminderTimerPublishesTheSameKeyAgainForTheDispatcherToDedupe() {
        abandoned(1, 0);
        clock.set(at(min(30)));

        scheduler.onTimer(Timer.reminder(CART, 1, 0, at(min(30)), 0));
        scheduler.onTimer(Timer.reminder(CART, 1, 0, at(min(30)), 0));

        List<ReminderIntent> published = intents.drain();
        assertEquals(2, published.size());
        assertEquals(published.get(0), published.get(1));
        assertEquals(1, timers.size());
    }

    @Test
    void reminderForAnOlderCycleIsDropped() {
        CartRecord r = active(1, T0, Arm.TREATMENT, 0);
        store.markAbandoned(r, List.of(T0), true);
        store.applyEvent(new CartEvent.CartPurchased(CART, SHOPPER, 2, at(min(40))), Arm.TREATMENT, 0);
        CartRecord third = active(3, at(min(50)), Arm.TREATMENT, 0);
        store.markAbandoned(third, List.of(T0, at(min(50))), true);
        clock.set(at(min(80)));

        scheduler.onTimer(Timer.reminder(CART, 1, 1, at(hrs(1)), 0));

        assertEquals(0, intents.size());
        assertEquals(1, metrics.get("timers.stale"));
        assertTrue(timers.claimDue(10).isEmpty());
    }

    @Test
    void reminderOnAnActiveCartIsDropped() {
        active(1, T0, Arm.TREATMENT, 0);

        scheduler.onTimer(Timer.reminder(CART, 1, 0, at(min(30)), 0));

        assertEquals(0, intents.size());
        assertEquals(1, metrics.get("timers.wrong_status"));
    }

    @Test
    void aReminderOffsetOutsideTheConfigIsPoisonAndAcked() {
        abandoned(1, 0);

        assertEquals(ACK, scheduler.onTimer(Timer.reminder(CART, 1, 3, at(hrs(48)), 0)));
        assertEquals(ACK, scheduler.onTimer(Timer.reminder(CART, 1, -2, at(hrs(48)), 0)));

        assertEquals(0, intents.size());
        assertEquals(2, metrics.get("timers.poison"));
    }

    @Test
    void transientStoreErrorsPropagateSoTheLeaseRedeliversTheTimer() {
        CartStateStore down = (CartStateStore) Proxy.newProxyInstance(CartStateStore.class.getClassLoader(),
            new Class<?>[] {CartStateStore.class}, (proxy, method, args) -> {
                throw new IllegalStateException("store unavailable");
            });
        caughtUpAt(at(min(31)));

        assertThrows(IllegalStateException.class,
            () -> scheduler(RecoveryConfig.defaults(), down).onTimer(check(1)));
    }
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.core.ReminderSchedulerTest' --console=plain`
Expected: FAIL at `compileTestJava` with `cannot find symbol: class ReminderScheduler`.

- [ ] **Step 7: Write the scheduler**

Create `main/core/ReminderScheduler.java`:

```java
package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.DispatchConfig;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerDecision;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.IntentPublisher;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import com.quince.cartrecovery.ports.TimerStore;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Handles one claimed timer and tells the caller whether to ack or release it. Every fire reloads the cart
 * consistently and compares the version, so a superseded timer is dropped rather than deleted. CHECK_ABANDON
 * waits for the cart's partition watermark; REMINDER i publishes an intent stamped with sendBy and chains i + 1.
 * Exceptions from the ports propagate: the caller leaves the timer un-acked and its lease redelivers it.
 */
public final class ReminderScheduler {
    static final Duration MIN_HOLD = Duration.ofSeconds(1);
    static final Duration MAX_HOLD = Duration.ofSeconds(60);
    private static final TimerDecision ACK = new TimerDecision.Ack();

    private final RecoveryConfig config;
    private final DispatchConfig dispatch;
    private final ReminderPolicy policy;
    private final CartStateStore store;
    private final TimerStore timers;
    private final Watermark watermark;
    private final IntentPublisher intents;
    private final OutcomeRecorder outcomes;
    private final Metrics metrics;

    public ReminderScheduler(RecoveryConfig config, DispatchConfig dispatch, CartStateStore store, TimerStore timers,
                             Watermark watermark, IntentPublisher intents, OutcomeRecorder outcomes, Metrics metrics) {
        this.config = config;
        this.dispatch = dispatch;
        this.policy = new ReminderPolicy(config);
        this.store = store;
        this.timers = timers;
        this.watermark = watermark;
        this.intents = intents;
        this.outcomes = outcomes;
        this.metrics = metrics;
    }

    public TimerDecision onTimer(Timer timer) {
        return switch (timer.kind()) {
            case CHECK_ABANDON -> checkAbandon(timer);
            case REMINDER -> reminder(timer);
        };
    }

    private TimerDecision checkAbandon(Timer timer) {
        Instant needed = timer.dueAt().plus(dispatch.clockSkew());
        Instant w = watermark.current(timer.srcPartition());
        if (w.isBefore(needed)) {
            metrics.increment("timers.held");
            return new TimerDecision.Release(holdFor(w, needed));
        }
        Optional<CartRecord> loaded = current(timer);
        if (loaded.isEmpty()) return ACK;
        CartRecord record = loaded.get();
        Instant now = watermark.now();
        switch (record.status()) {
            case ACTIVE -> {
                List<Instant> starts = record.startsWith(record.lastActivityAt(), now, config.frequencyWindow());
                boolean eligible = policy.eligible(record.abandoned(), now);
                if (!store.markAbandoned(record, starts, eligible)) {
                    metrics.increment("timers.conflict");
                    return ACK;
                }
                metrics.increment("carts.abandoned");
                abandonedOutcome(record, now);
                if (eligible) {
                    armFirstReminder(record);
                } else {
                    metrics.increment(record.arm() == Arm.HOLDOUT ? "carts.holdout" : "carts.cap_reached");
                }
            }
            case ABANDONED -> {
                abandonedOutcome(record, now);
                if (policy.eligible(record, now)) armFirstReminder(record);
            }
            case CLOSED -> metrics.increment("timers.wrong_status");
        }
        return ACK;
    }

    private TimerDecision reminder(Timer timer) {
        int i = timer.offsetIndex();
        if (i < 0 || i >= config.offsets().size()) {
            metrics.increment("timers.poison");
            return ACK;
        }
        Optional<CartRecord> loaded = current(timer);
        if (loaded.isEmpty()) return ACK;
        CartRecord record = loaded.get();
        if (record.status() != CartStatus.ABANDONED) {
            metrics.increment("timers.wrong_status");
            return ACK;
        }
        Instant scheduledFor = timer.dueAt();
        intents.publish(new ReminderIntent(new LedgerKey(record.cartId(), record.version(), i).toString(),
            record.cartId(), record.version(), i, record.srcPartition(), scheduledFor,
            scheduledFor.plus(config.latenessBounds().get(i))));
        metrics.increment("reminders.published");
        if (i + 1 < config.offsets().size()) {
            timers.upsert(Timer.reminder(record.cartId(), record.version(), i + 1,
                record.lastActivityAt().plus(config.offsets().get(i + 1)), record.srcPartition()));
        } else {
            store.endSequence(record.cartId(), record.version());
        }
        return ACK;
    }

    /** The cart at the timer's version, or empty (counted timers.stale) when it is gone or has moved on. */
    private Optional<CartRecord> current(Timer timer) {
        Optional<CartRecord> loaded = store.get(timer.cartId());
        if (loaded.isEmpty() || loaded.get().version() != timer.version()) {
            metrics.increment("timers.stale");
            return Optional.empty();
        }
        return loaded;
    }

    private void armFirstReminder(CartRecord record) {
        timers.upsert(Timer.reminder(record.cartId(), record.version(), 0,
            record.lastActivityAt().plus(config.offsets().get(0)), record.srcPartition()));
    }

    private void abandonedOutcome(CartRecord record, Instant now) {
        outcomes.record(new Outcome(null, record.cartId(), record.version(), record.arm(), OutcomeKind.ABANDONED, now, 0));
    }

    /** clamp(needed - w, 1 s, 60 s); 60 s when the watermark is unknown or stale. */
    static Duration holdFor(Instant w, Instant needed) {
        if (w.equals(Instant.EPOCH)) return MAX_HOLD;
        Duration gap = Duration.between(w, needed);
        if (gap.compareTo(MIN_HOLD) < 0) return MIN_HOLD;
        return gap.compareTo(MAX_HOLD) > 0 ? MAX_HOLD : gap;
    }
}
```

- [ ] **Step 8: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.core.ReminderSchedulerTest' --console=plain`
Expected: PASS (21 tests).

- [ ] **Step 9: Run the full suite**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`, 0 failures.

- [ ] **Step 10: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/core/AbandonmentDetector.java \
        src/main/java/com/quince/cartrecovery/core/ReminderScheduler.java \
        src/test/java/com/quince/cartrecovery/core/AbandonmentDetectorTest.java \
        src/test/java/com/quince/cartrecovery/core/ReminderSchedulerTest.java
git commit -m "$(cat <<'MSG'
Rebuild the detector and scheduler on the production-infra ports

Detector writes the timer first, then the conditional cart update, and
records the source partition. Scheduler gates CHECK_ABANDON on the cart's
partition watermark with a growing release delay, re-arms REMINDER 0 on a
redelivered check, records ABANDONED outcomes on both branches, stamps
sendBy into published intents, ends sequences, and acks poison timers.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task A3: `Dispatcher` (`handle` steps 1 to 7, `retryDue` token before claim, `replay`, lease check, produce before finish)

**Model:** opus (ordering and fencing subtleties). Reviewer: opus.

**Files:**
- Create: `main/core/Dispatcher.java`
- Test: `test/core/DispatcherTest.java`

**Interfaces:**
- Consumes (A1): `SendLedger.claim/markRetry/finish/dueRetries/reopen`, `ClaimResult.Claimed(token, attempts, sendBy, srcPartition, leaseUntil)` / `NotClaimed(reason)`, `Watermark.current/now`, `SendBudget.tryAcquire(Lane)`, `Lane.of(offsetIndex, fastOffsets)`, `NotificationSink.send(ReminderMessage)`, `OutcomeRecorder.record(Outcome)`, `DeadLetterQueue.add(DeadLetter)`, `DeadLetter.REASON_POISON`, `LedgerKey.parse`, `DispatchConfig.gatewayTimeout()/clockSkew()/fastOffsets()`, `RecoveryConfig.maxSendAttempts()/retryBase()/offsets()`, `Clock`, `Metrics`; test adapters `InMemorySendLedger`, `InMemoryWatermark`, `InMemoryOutcomeRecorder`, `InMemoryDeadLetterQueue`, `RecordingNotificationSink`.
- Produces (master §1.3): `Dispatcher(RecoveryConfig, DispatchConfig, CartStateStore, SendLedger, Watermark, SendBudget, NotificationSink, OutcomeRecorder, DeadLetterQueue, Clock, Metrics)` with `HandleResult handle(ReminderIntent intent)`, `void retryDue(int shard, int limit)`, `void replay(List<DeadLetter> letters)`; plus an additive overload taking a trailing `java.util.function.DoubleSupplier jitter` (fraction of the full backoff; the frozen constructor uses `ThreadLocalRandom`). Metrics: `dispatch.skipped_late_precheck`, `dispatch.no_token`, `dispatch.held`, `dispatch.duplicate`, `dispatch.skipped_late`, `dispatch.cancelled`, `dispatch.lease_expiring`, `dispatch.sent`, `dispatch.retry`, `dispatch.dead_lettered`, `dispatch.lease_lost`, `dispatch.retry_held`, `dispatch.retry_no_token`, `dispatch.replayed`, `dispatch.replay_poison_skipped`.

Behaviour pinned by the tests (spec §3, §5.4, §6.2):
1. `now > sendBy` → `SKIPPED_LATE` outcome (attempts 0), `dispatch.skipped_late_precheck`, `DONE`; no token, no ledger row. `now == sendBy` is on time (Review Focus 3).
2. `tryAcquire(lane)` false → `dispatch.no_token`, `HOLD`.
3. `watermark.current(intent.srcPartition) < watermark.now() − CLOCK_SKEW` → `dispatch.held`, `HOLD` (the token is not returned: bounded waste, spec §6.2).
4. `claim(key, sendBy, srcPartition, now)`; `NotClaimed` → `dispatch.duplicate`, `DONE`.
5. `now > sendBy` → `SKIPPED_LATE` outcome, `finish`.
6. Consistent cart read; not `ABANDONED` at the intent's version → `CANCELLED` outcome, `finish`.
7. `now > sendBy` → `SKIPPED_LATE`; `leaseUntil − now < GATEWAY_TIMEOUT` → `dispatch.lease_expiring`, stop, row stays `SENDING` for the retry loop; else send the message built from the cart: `SENT` → outcome, `finish(SENT)`; transient with `attempts < MAX_SEND_ATTEMPTS` → `markRetry(now + jitter × RETRY_BASE × 2^(attempts−1))`; exhausted or permanent → DLQ record, `DEAD` outcome, `finish(DEAD)`. Every outcome and DLQ record precedes its `finish`; a `false` from `markRetry` or `finish` counts `dispatch.lease_lost`.
- `retryDue`: per due `(key, srcPartition)`: gate (skip, `dispatch.retry_held`) → token (skip, `dispatch.retry_no_token`) → `claim` → steps 5 to 7. A skipped row stays due.
- `replay`: `reopen(key)` for every letter except reason `poison`.
- Reminder outcomes carry `Arm.TREATMENT` (only eligible carts get intents; the intent carries no arm).

- [ ] **Step 1: Write the failing dispatcher test**

Create `test/core/DispatcherTest.java`. It keeps every legacy case (send once, cancel on purchase, exponential retry then success, purchase during backoff, exhausted retries, permanent failure and replay, retry landing past the bound, attempt exactly at the bound, replay past the bound; "not yet due" is now the retry row before `nextAttemptAt` / `leaseUntil`) and adds the pre-check without a token, token-order holds, the recorded-partition gate, lanes, the retry loop's skip on lag or no token, the step 7 lease check with takeover at exactly `leaseUntil`, fencing of a stale holder, and produce-before-finish crash points:

```java
package com.quince.cartrecovery.core;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.InMemoryDeadLetterQueue;
import com.quince.cartrecovery.inmemory.InMemoryOutcomeRecorder;
import com.quince.cartrecovery.inmemory.InMemorySendLedger;
import com.quince.cartrecovery.inmemory.InMemoryWatermark;
import com.quince.cartrecovery.inmemory.RecordingNotificationSink;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.model.DispatchConfig;
import com.quince.cartrecovery.model.HandleResult;
import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.DeadLetterQueue;
import com.quince.cartrecovery.ports.NotificationSink;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import com.quince.cartrecovery.ports.SendBudget;
import com.quince.cartrecovery.ports.SendLedger;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.DoubleSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DispatcherTest {
    private static final int SHARDS = 4;
    private static final int SHARD = Shards.of(CART, SHARDS);
    private static final String KEY = "cart-1:1:0";

    private FakeClock clock;
    private InMemoryCartStateStore store;
    private InMemorySendLedger ledger;
    private InMemoryWatermark watermark;
    private RecordingNotificationSink sink;
    private InMemoryOutcomeRecorder outcomes;
    private InMemoryDeadLetterQueue dlq;
    private Metrics metrics;
    private final List<Lane> tokens = new ArrayList<>();
    private boolean tokensAvailable = true;
    private final SendBudget budget = lane -> {
        if (!tokensAvailable) return false;
        tokens.add(lane);
        return true;
    };
    private Dispatcher dispatcher;

    @BeforeEach
    void setUp() {
        clock = new FakeClock(at(min(30)));
        store = new InMemoryCartStateStore(RecoveryConfig.defaults(), SHARDS);
        ledger = new InMemorySendLedger(Duration.ofSeconds(90), SHARDS);
        watermark = new InMemoryWatermark(clock);
        sink = new RecordingNotificationSink(clock);
        outcomes = new InMemoryOutcomeRecorder();
        dlq = new InMemoryDeadLetterQueue();
        metrics = new Metrics();
        dispatcher = dispatcher(RecoveryConfig.defaults().withMaxSendAttempts(3));
        CartRecord active = store.applyEvent(new CartEvent.CartEdited(CART, SHOPPER, 1, T0, ITEMS, "Ada"), Arm.TREATMENT, 0)
            .orElseThrow();
        store.markAbandoned(active, List.of(T0), true);
        moveTo(at(min(30)));
    }

    private Dispatcher dispatcher(RecoveryConfig config) {
        return dispatcher(config, store, ledger, sink, outcomes, dlq, () -> 1.0);
    }

    private Dispatcher dispatcher(RecoveryConfig config, CartStateStore s, SendLedger l, NotificationSink n,
                                  OutcomeRecorder o, DeadLetterQueue d, DoubleSupplier jitter) {
        return new Dispatcher(config, DispatchConfig.defaults(), s, l, watermark, budget, n, o, d, clock, metrics, jitter);
    }

    /** Moves the clock and marks partition 0 caught up to it. */
    private void moveTo(Instant t) {
        clock.set(t);
        watermark.publish(0, 1, t);
    }

    private static ReminderIntent intent(int offsetIndex) {
        Instant scheduled = offsetIndex == 0 ? at(min(30)) : offsetIndex == 1 ? at(hrs(1)) : at(hrs(24));
        Duration bound = offsetIndex == 2 ? min(30) : min(5);
        return new ReminderIntent(new LedgerKey(CART, 1, offsetIndex).toString(), CART, 1, offsetIndex, 0,
            scheduled, scheduled.plus(bound));
    }

    private List<OutcomeKind> outcomeKinds() {
        return outcomes.all().stream().map(Outcome::kind).toList();
    }

    @SuppressWarnings("unchecked")
    private static <T> T around(Class<T> port, T target, List<String> log, String... logged) {
        return (T) Proxy.newProxyInstance(port.getClassLoader(), new Class<?>[] {port}, (proxy, method, args) -> {
            if (List.of(logged).contains(method.getName())) log.add(method.getName());
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }

    @Test
    void sendsOnceWithTheMessageBuiltFromTheCartAndFinishesSent() {
        assertEquals(HandleResult.DONE, dispatcher.handle(intent(0)));
        assertEquals(HandleResult.DONE, dispatcher.handle(intent(0)));

        assertEquals(List.of(new RecordingNotificationSink.Sent(
            new ReminderMessage(KEY, CART, SHOPPER, "Ada", ITEMS), at(min(30)))), sink.sent());
        assertEquals(Optional.of("SENT"), ledger.status(KEY));
        assertEquals(List.of(new Outcome(KEY, CART, 1, Arm.TREATMENT, OutcomeKind.SENT, at(min(30)), 1)), outcomes.all());
        assertEquals(1, metrics.get("dispatch.sent"));
        assertEquals(1, metrics.get("dispatch.duplicate"));
    }

    @Test
    void cancelsWhenTheCartWasPurchasedBeforeTheSend() {
        store.applyEvent(new CartEvent.CartPurchased(CART, SHOPPER, 2, at(min(29))), Arm.TREATMENT, 0);

        dispatcher.handle(intent(0));

        assertEquals(0, sink.attempts());
        assertEquals(Optional.of("CANCELLED"), ledger.status(KEY));
        assertEquals(List.of(OutcomeKind.CANCELLED), outcomeKinds());
        assertEquals(1, metrics.get("dispatch.cancelled"));
    }

    @Test
    void transientFailureRetriesWithExponentialBackoffThenSucceedsOnce() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE);

        dispatcher.handle(intent(0));
        assertEquals(Optional.of(at(min(31))), ledger.nextRetryAt());
        moveTo(at(min(31)));
        dispatcher.retryDue(SHARD, 10);
        assertEquals(Optional.of(at(min(33))), ledger.nextRetryAt());
        moveTo(at(min(33)));
        dispatcher.retryDue(SHARD, 10);

        assertEquals(List.of(at(min(33))), sink.sent().stream().map(RecordingNotificationSink.Sent::sentAt).toList());
        assertEquals(3, sink.attempts());
        assertEquals(Optional.of("SENT"), ledger.status(KEY));
        assertEquals(3, outcomes.all().get(0).attempts());
        assertEquals(2, metrics.get("dispatch.retry"));
    }

    @Test
    void theBackoffIsAJitteredFractionOfTheExponentialCap() {
        dispatcher = dispatcher(RecoveryConfig.defaults(), store, ledger, sink, outcomes, dlq, () -> 0.25);
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);

        dispatcher.handle(intent(0));

        assertEquals(Optional.of(at(min(30)).plusSeconds(15)), ledger.nextRetryAt());
    }

    @Test
    void purchaseDuringBackoffCancelsTheRetry() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);
        dispatcher.handle(intent(0));
        store.applyEvent(new CartEvent.CartPurchased(CART, SHOPPER, 2, at(min(30))), Arm.TREATMENT, 0);
        moveTo(at(min(31)));

        dispatcher.retryDue(SHARD, 10);

        assertEquals(0, sink.sent().size());
        assertEquals(Optional.of("CANCELLED"), ledger.status(KEY));
        assertEquals(1, metrics.get("dispatch.cancelled"));
    }

    @Test
    void exhaustedRetriesAreDeadLetteredWithTheOriginalIntent() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE);

        dispatcher.handle(intent(0));
        moveTo(at(min(31)));
        dispatcher.retryDue(SHARD, 10);
        moveTo(at(min(33)));
        dispatcher.retryDue(SHARD, 10);

        assertEquals(0, sink.sent().size());
        assertEquals(Optional.of("DEAD"), ledger.status(KEY));
        assertEquals(List.of(new DeadLetter(intent(0), "retries_exhausted", at(min(33)))), dlq.drain());
        assertEquals(new Outcome(KEY, CART, 1, Arm.TREATMENT, OutcomeKind.DEAD, at(min(33)), 3), outcomes.all().get(0));
    }

    @Test
    void permanentFailureIsDeadLetteredAndReplaySendsOnce() {
        sink.scriptOutcomes(SendResult.PERMANENT_FAILURE);
        dispatcher.handle(intent(0));
        List<DeadLetter> letters = dlq.drain();
        assertEquals(List.of(new DeadLetter(intent(0), "permanent_failure", at(min(30)))), letters);
        assertEquals(1, metrics.get("dispatch.dead_lettered"));

        moveTo(at(min(32)));
        dispatcher.replay(letters);
        dispatcher.retryDue(SHARD, 10);
        dispatcher.replay(letters);
        dispatcher.retryDue(SHARD, 10);

        assertEquals(1, sink.sent().size());
        assertEquals(at(min(32)), sink.sent().get(0).sentAt());
        assertEquals(1, metrics.get("dispatch.replayed"));
        assertEquals(new Outcome(KEY, CART, 1, Arm.TREATMENT, OutcomeKind.SENT, at(min(32)), 1), outcomes.all().get(1));
    }

    @Test
    void replaySkipsPoisonLetters() {
        sink.scriptOutcomes(SendResult.PERMANENT_FAILURE);
        dispatcher.handle(intent(0));
        dlq.drain();

        dispatcher.replay(List.of(new DeadLetter(intent(0), DeadLetter.REASON_POISON, at(min(30)))));

        assertEquals(Optional.of("DEAD"), ledger.status(KEY));
        assertEquals(1, metrics.get("dispatch.replay_poison_skipped"));
        assertEquals(0, metrics.get("dispatch.replayed"));
    }

    @Test
    void aReplayPastSendByIsSkippedAsLate() {
        sink.scriptOutcomes(SendResult.PERMANENT_FAILURE);
        dispatcher.handle(intent(0));
        moveTo(at(min(36)));

        dispatcher.replay(dlq.drain());
        dispatcher.retryDue(SHARD, 10);

        assertEquals(0, sink.sent().size());
        assertEquals(Optional.of("SKIPPED_LATE"), ledger.status(KEY));
        assertEquals(1, metrics.get("dispatch.replayed"));
        assertEquals(1, metrics.get("dispatch.skipped_late"));
    }

    @Test
    void aRetryLandingAfterSendByIsSkippedNotSent() {
        dispatcher = dispatcher(RecoveryConfig.defaults());
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE);

        dispatcher.handle(intent(0));
        moveTo(at(min(31)));
        dispatcher.retryDue(SHARD, 10);
        moveTo(at(min(33)));
        dispatcher.retryDue(SHARD, 10);
        assertEquals(Optional.of(at(min(37))), ledger.nextRetryAt());
        moveTo(at(min(37)));
        dispatcher.retryDue(SHARD, 10);

        assertEquals(0, sink.sent().size());
        assertEquals(3, sink.attempts());
        assertEquals(0, dlq.size());
        assertEquals(Optional.of("SKIPPED_LATE"), ledger.status(KEY));
        assertEquals(1, metrics.get("dispatch.skipped_late"));
    }

    @Test
    void sendByExactlyNowIsNotLate() {
        moveTo(at(min(35)));

        dispatcher.handle(intent(0));

        assertEquals(1, sink.sent().size());
        assertEquals(0, metrics.get("dispatch.skipped_late_precheck"));
        assertEquals(0, metrics.get("dispatch.skipped_late"));
    }

    @Test
    void aLateIntentIsSkippedAtThePreCheckWithoutATokenOrALedgerRow() {
        moveTo(at(min(35)).plusMillis(1));

        assertEquals(HandleResult.DONE, dispatcher.handle(intent(0)));

        assertEquals(List.of(), tokens);
        assertEquals(0, ledger.size());
        assertEquals(List.of(new Outcome(KEY, CART, 1, Arm.TREATMENT, OutcomeKind.SKIPPED_LATE, clock.now(), 0)),
            outcomes.all());
        assertEquals(1, metrics.get("dispatch.skipped_late_precheck"));
    }

    @Test
    void noTokenHoldsTheIntentWithoutClaiming() {
        tokensAvailable = false;

        assertEquals(HandleResult.HOLD, dispatcher.handle(intent(0)));
        assertEquals(0, ledger.size());
        assertEquals(1, metrics.get("dispatch.no_token"));

        tokensAvailable = true;
        assertEquals(HandleResult.DONE, dispatcher.handle(intent(0)));
        assertEquals(1, sink.sent().size());
    }

    @Test
    void aLaggingPartitionHoldsTheIntentAfterTakingTheTokenAndBeforeTheClaim() {
        clock.set(at(min(30)).plusSeconds(6));

        assertEquals(HandleResult.HOLD, dispatcher.handle(intent(0)));

        assertEquals(List.of(Lane.FAST), tokens);
        assertEquals(0, ledger.size());
        assertEquals(0, sink.attempts());
        assertEquals(1, metrics.get("dispatch.held"));
    }

    @Test
    void theGateReadsTheIntentsRecordedPartition() {
        clock.set(at(min(30)).plusSeconds(6));
        watermark.publish(7, 1, clock.now());
        ReminderIntent onSeven = new ReminderIntent(KEY, CART, 1, 0, 7, at(min(30)), at(min(35)));

        assertEquals(HandleResult.DONE, dispatcher.handle(onSeven));
        assertEquals(1, sink.sent().size());
    }

    @Test
    void theLaneFollowsFastOffsetsOnBothPaths() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);
        dispatcher.handle(intent(1));
        dispatcher.handle(intent(2));
        moveTo(at(min(31)));
        dispatcher.retryDue(SHARD, 10);

        assertEquals(List.of(Lane.FAST, Lane.SLOW, Lane.FAST), tokens);
    }

    @Test
    void theRetryLoopSkipsALaggingPartitionOrAMissingTokenAndLeavesTheRowDue() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);
        dispatcher.handle(intent(0));

        clock.set(at(min(31)));
        dispatcher.retryDue(SHARD, 10);
        assertEquals(1, metrics.get("dispatch.retry_held"));
        assertEquals(Optional.of("RETRYING"), ledger.status(KEY));

        moveTo(at(min(31)));
        tokensAvailable = false;
        dispatcher.retryDue(SHARD, 10);
        assertEquals(1, metrics.get("dispatch.retry_no_token"));
        assertEquals(Optional.of("RETRYING"), ledger.status(KEY));

        tokensAvailable = true;
        dispatcher.retryDue(SHARD, 10);
        assertEquals(1, sink.sent().size());
        assertEquals(2, outcomes.all().get(0).attempts());
    }

    @Test
    void theLeaseCheckStopsASendWithLessThanTheGatewayTimeoutLeftAndTheRetryLoopTakesOverAtExactlyLeaseUntil() {
        CartStateStore slow = (CartStateStore) Proxy.newProxyInstance(CartStateStore.class.getClassLoader(),
            new Class<?>[] {CartStateStore.class}, (proxy, method, args) -> {
                if (method.getName().equals("get")) clock.advance(Duration.ofSeconds(61));
                return method.invoke(store, args);
            });

        dispatcher(RecoveryConfig.defaults(), slow, ledger, sink, outcomes, dlq, () -> 1.0).handle(intent(0));

        assertEquals(0, sink.attempts());
        assertEquals(1, metrics.get("dispatch.lease_expiring"));
        assertEquals(Optional.of("SENDING"), ledger.status(KEY));
        Instant leaseUntil = at(min(30)).plusSeconds(90);
        assertEquals(Optional.of(leaseUntil), ledger.nextRetryAt());

        moveTo(leaseUntil.minusMillis(1));
        dispatcher.retryDue(SHARD, 10);
        assertEquals(0, sink.attempts());

        moveTo(leaseUntil);
        dispatcher.retryDue(SHARD, 10);
        assertEquals(1, sink.sent().size());
        assertEquals(2, outcomes.all().get(0).attempts());
    }

    @Test
    void aHolderWhoseLeaseWasTakenOverDuringTheSendCannotFinish() {
        NotificationSink slowGateway = message -> {
            clock.advance(Duration.ofSeconds(91));
            ledger.claim(message.key(), at(min(35)), 0, clock.now());
            return SendResult.SENT;
        };

        dispatcher(RecoveryConfig.defaults(), store, ledger, slowGateway, outcomes, dlq, () -> 1.0).handle(intent(0));

        assertEquals(1, metrics.get("dispatch.lease_lost"));
        assertEquals(List.of(OutcomeKind.SENT), outcomeKinds());
        assertEquals(Optional.of("SENDING"), ledger.status(KEY));
    }

    @Test
    void outcomesAndDeadLettersAreProducedBeforeTheLedgerFinish() {
        List<String> log = new ArrayList<>();
        SendLedger logged = around(SendLedger.class, ledger, log, "finish");
        OutcomeRecorder recorder = o -> log.add("outcome:" + o.kind());
        DeadLetterQueue letters = l -> log.add("dlq");
        sink.scriptOutcomes(SendResult.SENT, SendResult.PERMANENT_FAILURE);
        Dispatcher d = dispatcher(RecoveryConfig.defaults(), store, logged, sink, recorder, letters, () -> 1.0);

        d.handle(intent(0));
        d.handle(intent(1));

        assertEquals(List.of("outcome:SENT", "finish", "dlq", "outcome:DEAD", "finish"), log);
    }

    @Test
    void aCrashWhileProducingTheSentOutcomeLeavesTheRowForAResendUnderTheSameKey() {
        OutcomeRecorder down = o -> { throw new IllegalStateException("broker unavailable"); };

        assertThrows(IllegalStateException.class, () ->
            dispatcher(RecoveryConfig.defaults(), store, ledger, sink, down, dlq, () -> 1.0).handle(intent(0)));
        assertEquals(Optional.of("SENDING"), ledger.status(KEY));

        moveTo(at(min(30)).plusSeconds(90));
        dispatcher.retryDue(SHARD, 10);

        assertEquals(List.of(KEY, KEY), sink.sent().stream().map(s -> s.message().key()).toList());
        assertEquals(Optional.of("SENT"), ledger.status(KEY));
    }

    @Test
    void aCrashBeforeTheDeadLetterIsProducedNeverLeavesAnUnreplayableDeadRow() {
        sink.scriptOutcomes(SendResult.PERMANENT_FAILURE);
        DeadLetterQueue down = l -> { throw new IllegalStateException("broker unavailable"); };

        assertThrows(IllegalStateException.class, () ->
            dispatcher(RecoveryConfig.defaults(), store, ledger, sink, outcomes, down, () -> 1.0).handle(intent(0)));

        assertEquals(Optional.of("SENDING"), ledger.status(KEY));
        assertEquals(List.of(), outcomes.all());
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.core.DispatcherTest' --console=plain`
Expected: FAIL at `compileTestJava` with `cannot find symbol: class Dispatcher`.

- [ ] **Step 3: Write the dispatcher**

Create `main/core/Dispatcher.java`:

```java
package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.model.DispatchConfig;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.HandleResult;
import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.DeadLetterQueue;
import com.quince.cartrecovery.ports.NotificationSink;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import com.quince.cartrecovery.ports.SendBudget;
import com.quince.cartrecovery.ports.SendLedger;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Sends reminders at most once per key. Every attempt, first or retry, takes a send token, passes the watermark
 * gate for the cart's recorded partition, and holds a fenced ledger lease; sendBy is checked before the token,
 * after the claim, and immediately before the send. Outcome and dead-letter records are produced before the
 * ledger finish, so a crash in between only duplicates records that consumers already resolve.
 */
public final class Dispatcher {
    /** Stands in for sendBy on a retry row that has disappeared: the claim then recreates it already late. */
    private static final Instant GONE = Instant.EPOCH;

    private final RecoveryConfig config;
    private final DispatchConfig dispatch;
    private final CartStateStore store;
    private final SendLedger ledger;
    private final Watermark watermark;
    private final SendBudget budget;
    private final NotificationSink sink;
    private final OutcomeRecorder outcomes;
    private final DeadLetterQueue dlq;
    private final Clock clock;
    private final Metrics metrics;
    private final DoubleSupplier jitter;

    public Dispatcher(RecoveryConfig config, DispatchConfig dispatch, CartStateStore store, SendLedger ledger,
                      Watermark watermark, SendBudget budget, NotificationSink sink, OutcomeRecorder outcomes,
                      DeadLetterQueue dlq, Clock clock, Metrics metrics) {
        this(config, dispatch, store, ledger, watermark, budget, sink, outcomes, dlq, clock, metrics,
            () -> ThreadLocalRandom.current().nextDouble());
    }

    /** jitter returns a fraction in [0, 1] of the full backoff; tests and the fake-clock pipeline pass a constant. */
    public Dispatcher(RecoveryConfig config, DispatchConfig dispatch, CartStateStore store, SendLedger ledger,
                      Watermark watermark, SendBudget budget, NotificationSink sink, OutcomeRecorder outcomes,
                      DeadLetterQueue dlq, Clock clock, Metrics metrics, DoubleSupplier jitter) {
        this.config = config;
        this.dispatch = dispatch;
        this.store = store;
        this.ledger = ledger;
        this.watermark = watermark;
        this.budget = budget;
        this.sink = sink;
        this.outcomes = outcomes;
        this.dlq = dlq;
        this.clock = clock;
        this.metrics = metrics;
        this.jitter = jitter;
    }

    /** Spec §6.2 steps 1 to 7 for one consumed intent. HOLD asks the caller to pause the partition and redeliver. */
    public HandleResult handle(ReminderIntent intent) {
        if (clock.now().isAfter(intent.sendBy())) {
            outcome(intent.key(), OutcomeKind.SKIPPED_LATE, 0);
            metrics.increment("dispatch.skipped_late_precheck");
            return HandleResult.DONE;
        }
        if (!budget.tryAcquire(Lane.of(intent.offsetIndex(), dispatch.fastOffsets()))) {
            metrics.increment("dispatch.no_token");
            return HandleResult.HOLD;
        }
        if (lagging(intent.srcPartition())) {
            metrics.increment("dispatch.held");
            return HandleResult.HOLD;
        }
        ClaimResult claim = ledger.claim(intent.key(), intent.sendBy(), intent.srcPartition(), clock.now());
        if (claim instanceof ClaimResult.Claimed c) {
            attempt(intent.key(), c, intent.scheduledFor());
        } else {
            metrics.increment("dispatch.duplicate");
        }
        return HandleResult.DONE;
    }

    /**
     * One pass of the retry loop over a shard: watermark gate, then a token, then the claim, then steps 5 to 7.
     * The token is taken before the claim, so the loop never holds a lease while waiting for capacity.
     */
    public void retryDue(int shard, int limit) {
        for (DueRetry due : ledger.dueRetries(shard, clock.now(), limit)) {
            if (lagging(due.srcPartition())) {
                metrics.increment("dispatch.retry_held");
                continue;
            }
            LedgerKey key = LedgerKey.parse(due.key());
            if (!budget.tryAcquire(Lane.of(key.offsetIndex(), dispatch.fastOffsets()))) {
                metrics.increment("dispatch.retry_no_token");
                continue;
            }
            ClaimResult claim = ledger.claim(due.key(), GONE, due.srcPartition(), clock.now());
            if (claim instanceof ClaimResult.Claimed c) {
                attempt(due.key(), c, null);
            } else {
                metrics.increment("dispatch.duplicate");
            }
        }
    }

    /** Reopens every dead-lettered key except poison; the retry loop then sends or skips it. Replaying twice is harmless. */
    public void replay(List<DeadLetter> letters) {
        for (DeadLetter letter : letters) {
            if (DeadLetter.REASON_POISON.equals(letter.reason())) {
                metrics.increment("dispatch.replay_poison_skipped");
            } else if (ledger.reopen(letter.intent().key(), clock.now())) {
                metrics.increment("dispatch.replayed");
            }
        }
    }

    /** Steps 5 to 7, holding the lease c. scheduledFor is null on the retry path, where it is rebuilt from the cart. */
    private void attempt(String key, ClaimResult.Claimed c, Instant scheduledFor) {
        if (clock.now().isAfter(c.sendBy())) {
            skipLate(key, c);
            return;
        }
        LedgerKey k = LedgerKey.parse(key);
        Optional<CartRecord> cart = store.get(k.cartId());
        if (cart.isEmpty() || cart.get().version() != k.version() || cart.get().status() != CartStatus.ABANDONED) {
            outcome(key, OutcomeKind.CANCELLED, c.attempts());
            metrics.increment("dispatch.cancelled");
            finish(key, c, OutcomeKind.CANCELLED, "cancelled");
            return;
        }
        Instant now = clock.now();
        if (now.isAfter(c.sendBy())) {
            skipLate(key, c);
            return;
        }
        if (Duration.between(now, c.leaseUntil()).compareTo(dispatch.gatewayTimeout()) < 0) {
            metrics.increment("dispatch.lease_expiring");
            return;
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
    }

    private void skipLate(String key, ClaimResult.Claimed c) {
        outcome(key, OutcomeKind.SKIPPED_LATE, c.attempts());
        metrics.increment("dispatch.skipped_late");
        finish(key, c, OutcomeKind.SKIPPED_LATE, "late");
    }

    private void deadLetter(String key, ClaimResult.Claimed c, CartRecord r, Instant scheduledFor, String reason) {
        LedgerKey k = LedgerKey.parse(key);
        Instant scheduled = scheduledFor != null ? scheduledFor
            : k.offsetIndex() < config.offsets().size() ? r.lastActivityAt().plus(config.offsets().get(k.offsetIndex()))
            : c.sendBy();
        dlq.add(new DeadLetter(new ReminderIntent(key, k.cartId(), k.version(), k.offsetIndex(), c.srcPartition(),
            scheduled, c.sendBy()), reason, clock.now()));
        outcome(key, OutcomeKind.DEAD, c.attempts());
        metrics.increment("dispatch.dead_lettered");
        finish(key, c, OutcomeKind.DEAD, reason);
    }

    private void finish(String key, ClaimResult.Claimed c, OutcomeKind kind, String reason) {
        if (!ledger.finish(key, c.token(), kind, reason)) metrics.increment("dispatch.lease_lost");
    }

    /** Reminder outcomes are TREATMENT by construction: holdout carts never get an intent. */
    private void outcome(String key, OutcomeKind kind, int attempts) {
        LedgerKey k = LedgerKey.parse(key);
        outcomes.record(new Outcome(key, k.cartId(), k.version(), Arm.TREATMENT, kind, clock.now(), attempts));
    }

    /** Behind if the partition's watermark is older than the watermark clock minus CLOCK_SKEW. */
    private boolean lagging(int srcPartition) {
        return watermark.current(srcPartition).isBefore(watermark.now().minus(dispatch.clockSkew()));
    }

    /** Full jitter: a fraction of retryBase x 2^(attempts - 1). */
    private Duration backoff(int attempts) {
        long capMillis = config.retryBase().multipliedBy(1L << (attempts - 1)).toMillis();
        return Duration.ofMillis((long) (capMillis * jitter.getAsDouble()));
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.core.DispatcherTest' --console=plain`
Expected: PASS (22 tests).

- [ ] **Step 5: Run the full suite**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/quince/cartrecovery/core/Dispatcher.java \
        src/test/java/com/quince/cartrecovery/core/DispatcherTest.java
git commit -m "$(cat <<'MSG'
Rebuild the dispatcher around a fenced ledger claim

handle runs the spec's steps 1 to 7: sendBy pre-check before any token,
non-blocking token, per-partition watermark gate, fenced claim, sendBy and
cart re-checks, the gateway-timeout lease check, then send. Outcome and DLQ
records are produced before the ledger finish. The retry loop takes its
token before claiming; replay reopens dead rows except poison.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task A4: `Reconciler` key diff, `Pipeline` (intents, retries, detector lag, dispatch delay), verifier updates and 3 new scenarios, `Main`, `PipelineTest`

**Model:** sonnet (integration with complete code here). Reviewer: sonnet.

**Files:**
- Create: `main/core/Reconciler.java`, `main/Pipeline.java`, `test/core/ReconcilerTest.java`, `test/PipelineTest.java`, `test/FakeClockVerifierTest.java`
- Modify: `main/Main.java`
- Delete: `main/legacy/`, `test/legacy/`

**Interfaces:**
- Consumes (A1 to A3): `CartStateStore.openCartIds/getAll/endSequence`, `TimerStore.existing/upsert`, `SendLedger.highestOffsetIndex`, `AbandonmentDetector.handle(CartEvent, int)`, `ReminderScheduler.onTimer(Timer) → TimerDecision`, `Dispatcher.handle/retryDue/replay` and the jitter overload, every in-memory adapter's extra accessors (`PriorityQueueTimerStore.nextDueAt/size/clear/claimDue`, `InMemorySendLedger.nextRetryAt/size/status`, `InMemoryWatermark.setLagging/clearLag`, `InMemoryIntentQueue.drain`, `InMemoryOutcomeRecorder.all`, `InMemoryDeadLetterQueue.drain/size`, `RecordingNotificationSink.sent/attempts/scriptOutcomes`).
- Produces: `Reconciler(RecoveryConfig, CartStateStore, TimerStore, SendLedger, Clock, Metrics)` with `void reconcileShard(int shard)` (master §1.3; C1c runs it per shard in parallel). `Pipeline(RecoveryConfig, Instant, ArmAssigner)`, `static Pipeline withDefaults(Instant)`, `ingest`, `advanceTo`, `outage`, `restart`, `redeliver`, `replayDeadLetters` (kept), new `setDispatchDelay(Duration)`, `stallDetector()`, `catchUpDetector()`, `tokensTaken()`, `outcomes()`, and accessors `clock()`, `store()`, `timers()`, `ledger()`, `sink()`, `dlq()`, `metrics()` (`outbox()` is gone). `Pipeline.SHARDS = 4`. `Main` keeps its demo output; C1c later adds role dispatch.

Design notes the tests rely on:
- The in-memory detector is synchronous, so partition 0's watermark is the clock and `Pipeline` uses `CLOCK_SKEW = 0` (spec §5.4 "In memory"). With the production 5 s skew every `CHECK_ABANDON` would be held 5 s and the first reminder would move from 30m to 30m05s, breaking the verifier's send times.
- Backoff uses jitter fraction 1.0 (the full cap), so the retry times of scenario 8 stay at 31m and 33m.
- Each `step()` is one detector-loop iteration: publish the watermark, claim and settle every due timer (repeating until none are due, so a `REMINDER 0` armed at its own due time fires in the same step), dispatch ready and held intents, then run the retry loop on every shard. `advanceTo` stops only at future due times (timer scores including lease expiries and release delays, ledger retry times, delayed dispatches), so held work never loops.
- Reconciler: batches of 100 open ids, `existing` per batch, `getAll` only for the missing ids; `upsert` counts `reconcile.timers_rebuilt` only when it wrote; no on-time offset left → `endSequence`, counted `reconcile.sequences_ended`.

- [ ] **Step 1: Write the failing reconciler test**

Create `test/core/ReconcilerTest.java`:

```java
package com.quince.cartrecovery.core;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.InMemorySendLedger;
import com.quince.cartrecovery.inmemory.PriorityQueueTimerStore;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.CartStateStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReconcilerTest {
    private static final int SHARDS = 4;

    private FakeClock clock;
    private InMemoryCartStateStore store;
    private PriorityQueueTimerStore timers;
    private InMemorySendLedger ledger;
    private Metrics metrics;
    private final List<List<String>> loaded = new ArrayList<>();
    private Reconciler reconciler;

    @BeforeEach
    void setUp() {
        clock = new FakeClock(T0);
        store = new InMemoryCartStateStore(RecoveryConfig.defaults(), SHARDS);
        timers = new PriorityQueueTimerStore(clock, Duration.ofSeconds(90));
        ledger = new InMemorySendLedger(Duration.ofSeconds(90), SHARDS);
        metrics = new Metrics();
        CartStateStore recording = (CartStateStore) Proxy.newProxyInstance(CartStateStore.class.getClassLoader(),
            new Class<?>[] {CartStateStore.class}, (proxy, method, args) -> {
                if (method.getName().equals("getAll")) {
                    loaded.add(((Collection<?>) args[0]).stream().map(Object::toString).sorted().toList());
                }
                try {
                    return method.invoke(store, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
        reconciler = new Reconciler(RecoveryConfig.defaults(), recording, timers, ledger, clock, metrics);
    }

    private CartRecord edit(String cartId, int partition) {
        return store.applyEvent(new CartEvent.CartEdited(cartId, SHOPPER, 1, T0, ITEMS), Arm.TREATMENT, partition)
            .orElseThrow();
    }

    private void reconcileAll() {
        for (int s = 0; s < SHARDS; s++) reconciler.reconcileShard(s);
    }

    private void sendRow(String cartId, int offset) {
        String key = new LedgerKey(cartId, 1, offset).toString();
        ClaimResult.Claimed c = (ClaimResult.Claimed) ledger.claim(key, at(hrs(48)), 0, clock.now());
        ledger.finish(key, c.token(), OutcomeKind.SENT, null);
    }

    @Test
    void reloadsOnlyTheOpenCartsWhoseTimerIsMissing() {
        edit("a", 2);
        edit("b", 3);
        timers.upsert(Timer.checkAbandon("a", 1, at(min(30)), 2));

        reconcileAll();

        assertEquals(List.of(List.of("b")), loaded);
        assertEquals(1, metrics.get("reconcile.timers_rebuilt"));
        clock.set(at(min(30)));
        assertEquals(List.of(Timer.checkAbandon("a", 1, at(min(30)), 2), Timer.checkAbandon("b", 1, at(min(30)), 3)),
            timers.claimDue(10));
    }

    @Test
    void nothingIsReloadedWhenEveryOpenCartHasATimer() {
        edit("a", 0);
        timers.upsert(Timer.checkAbandon("a", 1, at(min(30)), 0));

        reconcileAll();

        assertEquals(List.of(), loaded);
    }

    @Test
    void checksExistingTimersInBatchesOfAHundred() {
        List<String> ids = IntStream.range(0, 250).mapToObj(i -> "cart-" + i).toList();
        for (String id : ids) edit(id, 0);
        int shard = Shards.of("cart-0", SHARDS);
        long inShard = ids.stream().filter(id -> Shards.of(id, SHARDS) == shard).count();

        reconciler.reconcileShard(shard);

        assertEquals((inShard + 99) / 100, loaded.size());
        assertEquals(inShard, metrics.get("reconcile.timers_rebuilt"));
    }

    @Test
    void anAbandonedCartResumesAtTheFirstOnTimeOffsetAfterTheLedgersHighest() {
        CartRecord r = edit("a", 1);
        store.markAbandoned(r, List.of(T0), true);
        sendRow("a", 0);
        clock.set(at(hrs(3)));

        reconcileAll();

        clock.set(at(hrs(24)));
        assertEquals(List.of(Timer.reminder("a", 1, 2, at(hrs(24)), 1)), timers.claimDue(10));
    }

    @Test
    void anAbandonedCartWithNoOnTimeOffsetLeftEndsItsSequence() {
        CartRecord r = edit("a", 0);
        store.markAbandoned(r, List.of(T0), true);
        sendRow("a", 1);
        clock.set(at(hrs(24)).plus(min(31)));

        reconcileAll();

        assertEquals(0, timers.size());
        assertEquals(1, metrics.get("reconcile.sequences_ended"));
        assertEquals(List.of(), store.openCartIds(Shards.of("a", SHARDS), clock.now()).toList());
    }

    @Test
    void closedAndIneligibleCartsAreNotListedOrRebuilt() {
        edit("closed", 0);
        store.applyEvent(new CartEvent.CartPurchased("closed", SHOPPER, 2, T0), Arm.TREATMENT, 0);
        CartRecord holdout = store.applyEvent(new CartEvent.CartEdited("holdout", SHOPPER, 1, T0, ITEMS), Arm.HOLDOUT, 0)
            .orElseThrow();
        store.markAbandoned(holdout, List.of(T0), false);

        reconcileAll();

        assertEquals(List.of(), loaded);
        assertEquals(0, timers.size());
    }

    @Test
    void cartsPastTheirOpenUntilAreNotListed() {
        edit("a", 0);
        clock.set(at(hrs(25)).plusMillis(1));

        reconcileAll();

        assertEquals(List.of(), loaded);
        assertEquals(0, timers.size());
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.core.ReconcilerTest' --console=plain`
Expected: FAIL at `compileTestJava` with `cannot find symbol: class Reconciler`.

- [ ] **Step 3: Write the reconciler**

Create `main/core/Reconciler.java`:

```java
package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.Clock;
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
 * lateness bound, or its sequence ended when none remains. Idempotent, so it needs no lock.
 */
public final class Reconciler {
    static final int BATCH = 100;

    private final RecoveryConfig config;
    private final ReminderPolicy policy;
    private final CartStateStore store;
    private final TimerStore timers;
    private final SendLedger ledger;
    private final Clock clock;
    private final Metrics metrics;

    public Reconciler(RecoveryConfig config, CartStateStore store, TimerStore timers,
                      SendLedger ledger, Clock clock, Metrics metrics) {
        this.config = config;
        this.policy = new ReminderPolicy(config);
        this.store = store;
        this.timers = timers;
        this.ledger = ledger;
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
                    int next = nextOnTimeOffset(r, ledger.highestOffsetIndex(r.cartId(), r.version()) + 1, now);
                    if (next < config.offsets().size()) {
                        rebuilt(Timer.reminder(r.cartId(), r.version(), next,
                            r.lastActivityAt().plus(config.offsets().get(next)), r.srcPartition()));
                    } else if (store.endSequence(r.cartId(), r.version())) {
                        metrics.increment("reconcile.sequences_ended");
                    }
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

    private void rebuilt(Timer timer) {
        if (timers.upsert(timer)) metrics.increment("reconcile.timers_rebuilt");
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.core.ReconcilerTest' --console=plain`
Expected: PASS (7 tests).

- [ ] **Step 5: Write the pipeline tests at their original paths**

These are the legacy files with only the changes the spec requires. No send-time assertion changes. Changed assertions:

| Test | Was | Now | Why |
|---|---|---|---|
| Verifier and `PipelineTest` helpers | `s.intent().idempotencyKey()` | `s.message().key()` | The sink records `ReminderMessage` (A1). |
| Scenario 6 | `reminders.duplicate_timer == 1` | `dispatch.duplicate == 1` | Dedupe moved to the dispatcher claim (spec §8.4, metric rename §6.2). |
| Scenario 6 | `timers.wrong_status == 1` | `timers.wrong_status == 0`, 2 raw `ABANDONED` outcomes, 1 after dedupe by `(cartId, version)` | A redelivered `CHECK_ABANDON` on an abandoned cart at the same version is now the re-arm branch (spec §6.2), which records the outcome again. |
| Scenario 9b | `outbox().size() == 0` | ledger status of `cart-1:1:0` is `SKIPPED_LATE` | `Outbox` is deleted; the ledger row holds the result. |
| Scenario 9c | `outbox().size() == 0` | ledger status of `cart-1:1:0` is `CANCELLED` | Same. |
| Scenario 11 | `reminders.skipped_late == 2` | `dispatch.skipped_late_precheck == 2` (ledger still 1 row) | Lateness moved to the dispatcher pre-check, which writes no row (spec §8.4). |
| `PipelineTest` outage cases | `reminders.skipped_late` | `dispatch.skipped_late_precheck` | Same. |

New scenarios 16 to 18 are spec §8.2. Scenario 17 performs the crashed scheduler's first writes by hand (claim the check, `markAbandoned`, record the outcome) and lets the lease redeliver the check 90 s later.

Create `test/PipelineTest.java`:

```java
package com.quince.cartrecovery;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.RecoveryConfig;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PipelineTest {

    private Pipeline treatmentPipeline() {
        return new Pipeline(RecoveryConfig.defaults(), T0, key -> Arm.TREATMENT);
    }

    private List<Instant> sentTimes(Pipeline p) {
        return p.sink().sent().stream().map(s -> s.sentAt()).toList();
    }

    @Test
    void advanceToFiresTimersScheduledDuringAFireAtTheirOwnDueTime() {
        Pipeline p = treatmentPipeline();
        p.ingest(edited(1, min(0)));

        p.advanceTo(at(hrs(1)));

        assertEquals(List.of(at(min(30)), at(hrs(1))), sentTimes(p));
        assertEquals(at(hrs(1)), p.clock().now());
    }

    @Test
    void advanceToEarlierThanNowDoesNothing() {
        Pipeline p = treatmentPipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(10)));

        p.advanceTo(at(min(5)));

        assertEquals(at(min(10)), p.clock().now());
        assertEquals(0, p.sink().sent().size());
        assertEquals(1, p.timers().size());
    }

    @Test
    void ingestFiresTimersDueBeforeTheEvent() {
        Pipeline p = treatmentPipeline();
        p.ingest(edited(1, min(0)));

        p.ingest(purchased(2, min(45)));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30))), sentTimes(p));
    }

    @Test
    void restartWhileActiveRebuildsTheCheckTimer() {
        Pipeline p = treatmentPipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(10)));

        p.restart();
        assertEquals(1, p.timers().size());
        p.advanceTo(at(hrs(24)));

        assertEquals(List.of(at(min(30)), at(hrs(1)), at(hrs(24))), sentTimes(p));
    }

    @Test
    void restartWhileAbandonedResumesFromTheLedger() {
        Pipeline p = treatmentPipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(40)));
        assertEquals(CartStatus.ABANDONED, p.store().get(CART).orElseThrow().status());

        p.restart();
        assertEquals(1, p.metrics().get("reconcile.timers_rebuilt"));
        p.advanceTo(at(hrs(24)));

        assertEquals(List.of(at(min(30)), at(hrs(1)), at(hrs(24))), sentTimes(p));
        assertEquals(3, p.ledger().size());
    }

    @Test
    void restartAfterEveryReminderWasSkippedRebuildsNothing() {
        Pipeline p = treatmentPipeline();
        p.ingest(edited(1, min(0)));
        p.outage(hrs(30));
        p.advanceTo(at(hrs(30)));
        assertEquals(3, p.metrics().get("dispatch.skipped_late_precheck"));
        assertEquals(0, p.sink().sent().size());
        assertEquals(0, p.timers().size());

        p.restart();
        assertEquals(0, p.metrics().get("reconcile.timers_rebuilt"));
        assertEquals(0, p.timers().size());
        p.advanceTo(at(hrs(48)));

        assertEquals(0, p.sink().sent().size());
        assertEquals(3, p.metrics().get("dispatch.skipped_late_precheck"));
    }

    @Test
    void restartAfterPartialOutageSkipsToTheNextOnTimeOffset() {
        Pipeline p = treatmentPipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(40)));
        p.outage(hrs(3).minus(min(40)));

        p.restart();
        assertEquals(1, p.metrics().get("reconcile.timers_rebuilt"));
        assertEquals(1, p.timers().size());
        assertEquals(Optional.of(at(hrs(24))), p.timers().nextDueAt());
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30)), at(hrs(24))), sentTimes(p));
        assertEquals(List.of("cart-1:1:0", "cart-1:1:2"),
            p.sink().sent().stream().map(s -> s.message().key()).toList());
        assertEquals(0, p.metrics().get("dispatch.skipped_late_precheck"));
    }

    @Test
    void restartAfterAllRemindersSchedulesNothing() {
        Pipeline p = treatmentPipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(hrs(25)));

        p.restart();

        assertEquals(0, p.timers().size());
    }
}
```

Create `test/FakeClockVerifierTest.java`:

```java
package com.quince.cartrecovery;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.model.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * End-to-end scenarios from the spec, section 14, plus the production-infra spec §8.2 scenarios (16 to 18).
 * Each drives the pipeline on a fake clock and asserts exactly which reminders would have fired, and when.
 */
class FakeClockVerifierTest {

    private static Pipeline pipeline() {
        return new Pipeline(RecoveryConfig.defaults(), T0, key -> Arm.TREATMENT);
    }

    private static Pipeline pipeline(RecoveryConfig config) {
        return new Pipeline(config, T0, key -> Arm.TREATMENT);
    }

    private static List<Instant> sentTimes(Pipeline p) {
        return p.sink().sent().stream().map(s -> s.sentAt()).toList();
    }

    private static List<Outcome> abandonedOutcomes(Pipeline p) {
        return p.outcomes().all().stream().filter(o -> o.kind() == OutcomeKind.ABANDONED).toList();
    }

    private static List<String> sentKeys(Pipeline p) {
        return p.sink().sent().stream().map(s -> s.message().key()).toList();
    }

    @Test @DisplayName("1. single edit fires at 30m, 1h, 24h, one send each")
    void singleEdit() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30)), at(hrs(1)), at(hrs(24))), sentTimes(p));
        assertEquals(List.of("cart-1:1:0", "cart-1:1:1", "cart-1:1:2"), sentKeys(p));
        assertEquals(0, p.timers().size());
    }

    @Test @DisplayName("2. edit at 20m resets the clock")
    void editResetsClock() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.ingest(edited(2, min(20)));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(50)), at(min(80)), at(hrs(24).plusMinutes(20))), sentTimes(p));
        assertEquals(List.of("cart-1:2:0", "cart-1:2:1", "cart-1:2:2"), sentKeys(p));
    }

    @Test @DisplayName("3a. purchase at 10m: no sends")
    void purchaseBeforeAbandonment() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.ingest(purchased(2, min(10)));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(), sentTimes(p));
        assertEquals(CartStatus.CLOSED, p.store().get(CART).orElseThrow().status());
    }

    @Test @DisplayName("3b. purchase at 45m: first send only")
    void purchaseBetweenReminders() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(45)));
        p.ingest(purchased(2, min(45)));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30))), sentTimes(p));
    }

    @Test @DisplayName("4a. clear cancels pending reminders")
    void clearCancels() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(35)));
        p.ingest(cleared(2, min(35)));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30))), sentTimes(p));
    }

    @Test @DisplayName("4b. resume cancels pending reminders and restarts the clock")
    void resumeCancelsAndRestarts() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(35)));
        p.ingest(resumed(2, min(35)));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30)), at(min(65)), at(min(95)), at(hrs(24).plusMinutes(35))), sentTimes(p));
        assertEquals("cart-1:2:0", sentKeys(p).get(1));
    }

    @Test @DisplayName("5. duplicate delivery of the same edit: same schedule, one send each")
    void duplicateEvent() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(10)));
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30)), at(hrs(1)), at(hrs(24))), sentTimes(p));
        assertEquals(2, p.metrics().get("events.ignored"));
    }

    @Test @DisplayName("6. duplicate delivery of the same timer: one send")
    void duplicateTimer() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(30)));
        assertEquals(1, p.sink().sent().size());

        p.redeliver(Timer.reminder(CART, 1, 0, at(min(30))));
        p.redeliver(Timer.checkAbandon(CART, 1, at(min(30))));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30)), at(hrs(1)), at(hrs(24))), sentTimes(p));
        assertEquals(1, p.metrics().get("dispatch.duplicate"));
        assertEquals(0, p.metrics().get("timers.wrong_status"));
        assertEquals(2, abandonedOutcomes(p).size());
        assertEquals(1, abandonedOutcomes(p).stream().map(o -> o.cartId() + ":" + o.version()).distinct().count());
    }

    @Test @DisplayName("7. out-of-order events: the older version is ignored")
    void outOfOrder() {
        Pipeline p = pipeline();
        p.ingest(edited(2, min(20)));
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(50)), at(min(80)), at(hrs(24).plusMinutes(20))), sentTimes(p));
        assertEquals(1, p.metrics().get("events.ignored"));
    }

    @Test @DisplayName("8. transient failure twice then success: one send, one ledger row")
    void transientFailureRetries() {
        Pipeline p = pipeline();
        p.sink().scriptOutcomes(SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE);
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(40)));

        assertEquals(List.of(at(min(33))), sentTimes(p));
        assertEquals(3, p.sink().attempts());
        assertEquals(1, p.ledger().size());
        assertEquals(0, p.dlq().size());
    }

    @Test @DisplayName("9. permanent failure dead-letters, replay inside the lateness bound sends once")
    void permanentFailureAndReplay() {
        Pipeline p = pipeline();
        p.sink().scriptOutcomes(SendResult.PERMANENT_FAILURE);
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(32)));
        assertEquals(0, p.sink().sent().size());
        assertEquals(1, p.dlq().size());

        p.replayDeadLetters();
        assertEquals(List.of(at(min(32))), sentTimes(p));
        assertEquals(List.of("cart-1:1:0"), sentKeys(p));
        assertEquals(0, p.dlq().size());

        p.replayDeadLetters();
        assertEquals(1, p.sink().sent().size());
        assertEquals(1, p.metrics().get("dispatch.replayed"));
    }

    @Test @DisplayName("9b. replay after the lateness bound is dropped, not sent")
    void replayAfterLatenessBound() {
        Pipeline p = pipeline();
        p.sink().scriptOutcomes(SendResult.PERMANENT_FAILURE);
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(40)));
        assertEquals(1, p.dlq().size());

        p.replayDeadLetters();

        assertEquals(List.of(), sentTimes(p));
        assertEquals(0, p.dlq().size());
        assertEquals(Optional.of("SKIPPED_LATE"), p.ledger().status("cart-1:1:0"));
        assertEquals(1, p.metrics().get("dispatch.skipped_late"));
    }

    @Test @DisplayName("9c. replay after purchase is cancelled, not sent")
    void replayAfterPurchase() {
        Pipeline p = pipeline();
        p.sink().scriptOutcomes(SendResult.PERMANENT_FAILURE);
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(32)));
        p.ingest(purchased(2, min(32)));

        p.replayDeadLetters();

        assertEquals(List.of(), sentTimes(p));
        assertEquals(Optional.of("CANCELLED"), p.ledger().status("cart-1:1:0"));
        assertEquals(1, p.metrics().get("dispatch.cancelled"));
        assertEquals(0, p.metrics().get("dispatch.skipped_late"));
    }

    @Test @DisplayName("10. restart between abandonment and next reminder: reminders still fire on time")
    void restartRebuildsTimers() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(40)));
        p.restart();
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30)), at(hrs(1)), at(hrs(24))), sentTimes(p));
        assertEquals(3, p.ledger().size());
    }

    @Test @DisplayName("10b. restart between abandonment and the first reminder: first reminder rebuilt and fires on time")
    void restartBetweenAbandonmentAndFirstReminder() {
        Pipeline p = pipeline(RecoveryConfig.defaults().withWindow(Duration.ofMinutes(20)));
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(25)));

        assertEquals(CartStatus.ABANDONED, p.store().get(CART).orElseThrow().status());
        assertEquals(List.of(), sentTimes(p));
        assertEquals(0, p.ledger().size());

        p.restart();
        assertEquals(1, p.metrics().get("reconcile.timers_rebuilt"));

        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30)), at(hrs(1)), at(hrs(24))), sentTimes(p));
        assertEquals(List.of("cart-1:1:0","cart-1:1:1","cart-1:1:2"), sentKeys(p));
    }

    @Test @DisplayName("11. timers delivered past the lateness bound are skipped, later offsets still fire")
    void latenessBound() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.outage(Duration.ofHours(3));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(hrs(24))), sentTimes(p));
        assertEquals(2, p.metrics().get("dispatch.skipped_late_precheck"));
        assertEquals(1, p.ledger().size());
    }

    @Test @DisplayName("12. holdout arm: abandoned, no sends")
    void holdout() {
        Pipeline p = new Pipeline(RecoveryConfig.defaults(), T0, key -> Arm.HOLDOUT);
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(hrs(48)));

        assertEquals(CartStatus.ABANDONED, p.store().get(CART).orElseThrow().status());
        assertEquals(List.of(), sentTimes(p));
        assertEquals(1, p.metrics().get("carts.holdout"));
    }

    @Test @DisplayName("13. reopen after purchase: a fresh cycle with its own keys")
    void reopenAfterPurchase() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(45)));
        p.ingest(purchased(2, min(45)));
        p.ingest(edited(3, min(60)));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30)), at(min(90)), at(min(120)), at(hrs(25))), sentTimes(p));
        assertEquals(List.of("cart-1:1:0", "cart-1:3:0", "cart-1:3:1", "cart-1:3:2"), sentKeys(p));
    }

    @Test @DisplayName("14. frequency cap reached: further sequences schedule nothing")
    void frequencyCap() {
        Pipeline p = pipeline(RecoveryConfig.defaults().withFrequencyCap(1));
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(35)));
        p.ingest(resumed(2, min(35)));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30))), sentTimes(p));
        assertEquals(2, p.metrics().get("carts.abandoned"));
        assertEquals(1, p.metrics().get("carts.cap_reached"));
    }

    @Test @DisplayName("15. config rejects a first offset smaller than the window")
    void configValidation() {
        assertThrows(IllegalArgumentException.class, () ->
            RecoveryConfig.defaults().withWindow(Duration.ofMinutes(31)));
    }

    @Test @DisplayName("16. a lagging detector holds the reminder; it is cancelled once the detector catches up")
    void laggingDetectorHoldsTheReminder() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(40)));
        p.stallDetector();
        p.ingest(purchased(2, min(45)));

        p.advanceTo(at(min(62)));
        assertEquals(List.of(at(min(30))), sentTimes(p));
        assertEquals(CartStatus.ABANDONED, p.store().get(CART).orElseThrow().status());
        assertTrue(p.metrics().get("dispatch.held") > 0);
        assertEquals(Optional.empty(), p.ledger().status("cart-1:1:1"));

        p.catchUpDetector();
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30))), sentTimes(p));
        assertEquals(Optional.of("CANCELLED"), p.ledger().status("cart-1:1:1"));
        assertEquals(1, p.metrics().get("dispatch.cancelled"));
        assertEquals(CartStatus.CLOSED, p.store().get(CART).orElseThrow().status());
    }

    @Test @DisplayName("17. a CHECK_ABANDON redelivered after a crash between abandonment and the reminder upsert still sends")
    void redeliveredCheckAfterCrash() {
        Pipeline p = pipeline();
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(29)));

        // The scheduler claims the check at 30m, marks the cart abandoned, records the outcome, and dies before
        // upserting REMINDER 0. The check stays leased and is redelivered when the lease ends (90 s later).
        p.clock().set(at(min(30)));
        assertEquals(1, p.timers().claimDue(10).size());
        CartRecord active = p.store().get(CART).orElseThrow();
        p.store().markAbandoned(active, active.startsWith(active.lastActivityAt(), at(min(30)), Duration.ofDays(7)), true);
        p.outcomes().record(new Outcome(null, CART, 1, Arm.TREATMENT, OutcomeKind.ABANDONED, at(min(30)), 0));

        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(min(30)).plusSeconds(90), at(hrs(1)), at(hrs(24))), sentTimes(p));
        assertEquals(List.of("cart-1:1:0", "cart-1:1:1", "cart-1:1:2"), sentKeys(p));
        assertEquals(2, abandonedOutcomes(p).size());
        assertEquals(1, abandonedOutcomes(p).stream().map(o -> o.cartId() + ":" + o.version()).distinct().count());
    }

    @Test @DisplayName("18. a dispatch delay past the lateness bound skips at the pre-check, spending no token")
    void dispatchDelayPastTheBound() {
        Pipeline p = pipeline();
        p.setDispatchDelay(min(6));
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(hrs(48)));

        assertEquals(List.of(at(hrs(24)).plus(min(6))), sentTimes(p));
        assertEquals(2, p.metrics().get("dispatch.skipped_late_precheck"));
        assertEquals(1, p.tokensTaken());
        assertEquals(1, p.ledger().size());
        assertEquals(1, p.sink().attempts());
    }

    @Test @DisplayName("many carts interleaved keep independent schedules")
    void manyCartsInterleaved() {
        Pipeline p = pipeline();
        for (int i = 0; i < 100; i++) {
            p.ingest(new com.quince.cartrecovery.model.CartEvent.CartEdited(
                "cart-" + i, "shopper-" + i, 1, T0.plusSeconds(i), ITEMS));
        }
        p.ingest(new com.quince.cartrecovery.model.CartEvent.CartPurchased("cart-7", "shopper-7", 2, at(min(5))));
        p.advanceTo(at(hrs(48)));

        assertEquals(99 * 3, p.sink().sent().size());
        assertTrue(sentKeys(p).stream().noneMatch(k -> k.startsWith("cart-7:")));
        assertEquals(at(min(30)).plusSeconds(1), p.sink().sent().get(1).sentAt());
    }
}
```

- [ ] **Step 6: Run them to verify they fail**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.PipelineTest' --tests 'com.quince.cartrecovery.FakeClockVerifierTest' --console=plain`
Expected: FAIL at `compileTestJava` with `cannot find symbol: class Pipeline` (only `legacy.Pipeline` exists).

- [ ] **Step 7: Write the pipeline**

Create `main/Pipeline.java`:

```java
package com.quince.cartrecovery;

import com.quince.cartrecovery.core.AbandonmentDetector;
import com.quince.cartrecovery.core.Dispatcher;
import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.core.Reconciler;
import com.quince.cartrecovery.core.ReminderScheduler;
import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.inmemory.HashArmAssigner;
import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.InMemoryDeadLetterQueue;
import com.quince.cartrecovery.inmemory.InMemoryIntentQueue;
import com.quince.cartrecovery.inmemory.InMemoryOutcomeRecorder;
import com.quince.cartrecovery.inmemory.InMemorySendLedger;
import com.quince.cartrecovery.inmemory.InMemoryWatermark;
import com.quince.cartrecovery.inmemory.PriorityQueueTimerStore;
import com.quince.cartrecovery.inmemory.RecordingNotificationSink;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.DispatchConfig;
import com.quince.cartrecovery.model.HandleResult;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerDecision;
import com.quince.cartrecovery.ports.ArmAssigner;
import com.quince.cartrecovery.ports.SendBudget;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Wires the in-memory adapters to the core stages and drives them from a fake clock. Single-threaded: production
 * ordering per cart comes from partitioning, here from handling one input at a time. One cart-events partition (0),
 * whose watermark is the clock because the detector is synchronous, so CLOCK_SKEW is zero; stallDetector() makes it
 * lag. Backoff uses the full jitter cap, so retry times are deterministic.
 */
public final class Pipeline {
    public static final int SHARDS = 4;
    private static final int PARTITION = 0;
    private static final DispatchConfig DISPATCH =
        new DispatchConfig(Duration.ofSeconds(90), Duration.ofSeconds(30), Duration.ZERO, 2);

    private record Pending(ReminderIntent intent, Instant readyAt) {}

    private final FakeClock clock;
    private final InMemoryCartStateStore store;
    private final PriorityQueueTimerStore timers;
    private final InMemoryWatermark watermark;
    private final InMemoryIntentQueue intents = new InMemoryIntentQueue();
    private final InMemorySendLedger ledger = new InMemorySendLedger(DISPATCH.lease(), SHARDS);
    private final InMemoryOutcomeRecorder outcomes = new InMemoryOutcomeRecorder();
    private final RecordingNotificationSink sink;
    private final InMemoryDeadLetterQueue dlq = new InMemoryDeadLetterQueue();
    private final Metrics metrics = new Metrics();
    private final AbandonmentDetector detector;
    private final ReminderScheduler scheduler;
    private final Dispatcher dispatcher;
    private final Reconciler reconciler;

    private long tokensTaken;
    private Duration dispatchDelay = Duration.ZERO;
    private boolean stalled;
    private final List<CartEvent> buffered = new ArrayList<>();
    private final List<Pending> pending = new ArrayList<>();
    private final List<ReminderIntent> held = new ArrayList<>();

    public Pipeline(RecoveryConfig config, Instant start, ArmAssigner arms) {
        this.clock = new FakeClock(start);
        this.store = new InMemoryCartStateStore(config, SHARDS);
        this.timers = new PriorityQueueTimerStore(clock, DISPATCH.lease());
        this.watermark = new InMemoryWatermark(clock);
        this.sink = new RecordingNotificationSink(clock);
        SendBudget budget = lane -> {
            tokensTaken++;
            return true;
        };
        this.detector = new AbandonmentDetector(config, store, timers, arms, metrics);
        this.scheduler = new ReminderScheduler(config, DISPATCH, store, timers, watermark, intents, outcomes, metrics);
        this.dispatcher = new Dispatcher(config, DISPATCH, store, ledger, watermark, budget, sink, outcomes, dlq,
            clock, metrics, () -> 1.0);
        this.reconciler = new Reconciler(config, store, timers, ledger, clock, metrics);
    }

    public static Pipeline withDefaults(Instant start) {
        RecoveryConfig config = RecoveryConfig.defaults();
        return new Pipeline(config, start, new HashArmAssigner("cart-recovery-v1", config.holdoutPercent()));
    }

    /**
     * Feeds one event at its own time: first advances the clock to the event, running everything due before it,
     * then hands the event to the detector (or buffers it while the detector is stalled). An event older than the
     * clock (a late or duplicate delivery) is handled at the current time without moving the clock.
     */
    public void ingest(CartEvent event) {
        advanceTo(event.occurredAt());
        if (stalled) {
            buffered.add(event);
        } else {
            detector.handle(event, PARTITION);
        }
        step();
    }

    /**
     * Advances the fake clock to target, stopping at every timer due time, lease expiry, retry time, and delayed
     * dispatch in order, so each runs at its own virtual time. Never moves the clock backwards.
     */
    public void advanceTo(Instant target) {
        step();
        while (true) {
            Optional<Instant> next = nextStop();
            if (next.isEmpty() || next.get().isAfter(target)) break;
            clock.set(next.get());
            step();
        }
        clock.set(target);
        step();
    }

    /** Time passes with nothing running, as during an outage. Work due meanwhile runs late on the next advanceTo. */
    public void outage(Duration downFor) {
        clock.advance(downFor);
    }

    /** Simulates losing the timer index (Redis) and the reconciler rebuilding it from durable state. */
    public void restart() {
        timers.clear();
        for (int shard = 0; shard < SHARDS; shard++) reconciler.reconcileShard(shard);
    }

    /** Simulates at-least-once timer delivery by handing a timer to the scheduler again. */
    public void redeliver(Timer timer) {
        settle(timer, scheduler.onTimer(timer));
        step();
    }

    /** Runs the replay role over everything dead-lettered so far; the retry loop then sends or skips each key. */
    public void replayDeadLetters() {
        dispatcher.replay(dlq.drain());
        step();
    }

    /** Intents reach the dispatcher this long after they are published (a consumer backlog). */
    public void setDispatchDelay(Duration delay) {
        this.dispatchDelay = delay;
    }

    /** The detector stops handling events: they are buffered and the watermark stays at the current time. */
    public void stallDetector() {
        stalled = true;
        watermark.setLagging(PARTITION, clock.now());
    }

    /** The detector handles every buffered event in order and the watermark returns to the clock. */
    public void catchUpDetector() {
        stalled = false;
        for (CartEvent e : buffered) detector.handle(e, PARTITION);
        buffered.clear();
        watermark.clearLag();
        step();
    }

    /** Everything due at the current time: one detector loop iteration, timers, dispatch, and the retry loop. */
    private void step() {
        watermark.publish(PARTITION, 1, clock.now());
        List<Timer> due;
        while (!(due = timers.claimDue(Integer.MAX_VALUE)).isEmpty()) {
            for (Timer t : due) settle(t, scheduler.onTimer(t));
        }
        for (ReminderIntent i : intents.drain()) pending.add(new Pending(i, clock.now().plus(dispatchDelay)));
        List<ReminderIntent> ready = new ArrayList<>(held);
        held.clear();
        for (Iterator<Pending> it = pending.iterator(); it.hasNext(); ) {
            Pending p = it.next();
            if (!p.readyAt().isAfter(clock.now())) {
                ready.add(p.intent());
                it.remove();
            }
        }
        for (ReminderIntent i : ready) {
            if (dispatcher.handle(i) == HandleResult.HOLD) held.add(i);
        }
        for (int shard = 0; shard < SHARDS; shard++) dispatcher.retryDue(shard, Integer.MAX_VALUE);
    }

    private void settle(Timer timer, TimerDecision decision) {
        switch (decision) {
            case TimerDecision.Ack a -> timers.ack(timer);
            case TimerDecision.Release r -> timers.release(timer, r.delay());
        }
    }

    /**
     * The earliest future time something becomes due. Work already due but held (a lagging watermark) is retried
     * at every step instead. ponytail: nextRetryAt is only the earliest row, so a held due retry hides later ones
     * until the next other stop; fine while only stallDetector() holds retries.
     */
    private Optional<Instant> nextStop() {
        Instant now = clock.now();
        return Stream.of(timers.nextDueAt(), ledger.nextRetryAt(),
                pending.stream().map(Pending::readyAt).min(Instant::compareTo))
            .flatMap(Optional::stream)
            .filter(t -> t.isAfter(now))
            .min(Instant::compareTo);
    }

    public FakeClock clock() { return clock; }
    public InMemoryCartStateStore store() { return store; }
    public PriorityQueueTimerStore timers() { return timers; }
    public InMemorySendLedger ledger() { return ledger; }
    public InMemoryOutcomeRecorder outcomes() { return outcomes; }
    public RecordingNotificationSink sink() { return sink; }
    public InMemoryDeadLetterQueue dlq() { return dlq; }
    public Metrics metrics() { return metrics; }
    /** Send tokens taken from the (unlimited) budget. */
    public long tokensTaken() { return tokensTaken; }
}
```

- [ ] **Step 8: Run them to verify they pass**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --tests 'com.quince.cartrecovery.PipelineTest' --tests 'com.quince.cartrecovery.FakeClockVerifierTest' --console=plain`
Expected: PASS (`PipelineTest` 8, `FakeClockVerifierTest` 24).

- [ ] **Step 9: Point `Main` at the new pipeline**

Replace `main/Main.java`:

```java
package com.quince.cartrecovery;

import com.quince.cartrecovery.inmemory.RecordingNotificationSink;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.RecoveryConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Plays a scripted scenario through the pipeline on a fake clock and prints which triggers fired. */
public final class Main {
    private static final Instant T0 = Instant.parse("2026-01-01T09:00:00Z");
    private static final List<CartItem> ITEMS = List.of(
        new CartItem("SKU-1", "Linen Shirt", 1, 4990),
        new CartItem("SKU-2", "Cashmere Sweater", 1, 9900));

    public static void main(String[] args) {
        Pipeline p = new Pipeline(RecoveryConfig.defaults(), T0, key -> Arm.TREATMENT);
        int[] seen = {0};

        System.out.println("virtual start " + T0);
        System.out.println();

        step(p, seen, "cart A edited at +0m, cart B edited at +0m, cart C edited at +0m", () -> {
            p.ingest(new CartEvent.CartEdited("A", "user-a", 1, T0, ITEMS));
            p.ingest(new CartEvent.CartEdited("B", "user-b", 1, T0, ITEMS));
            p.ingest(new CartEvent.CartEdited("C", "user-c", 1, T0, ITEMS));
        });
        step(p, seen, "cart B edited again at +20m (clock reset)", () ->
            p.ingest(new CartEvent.CartEdited("B", "user-b", 2, T0.plus(Duration.ofMinutes(20)), ITEMS)));
        step(p, seen, "cart C purchased at +25m (cancels)", () ->
            p.ingest(new CartEvent.CartPurchased("C", "user-c", 2, T0.plus(Duration.ofMinutes(25)))));
        step(p, seen, "advance to +30m", () -> p.advanceTo(T0.plus(Duration.ofMinutes(30))));
        step(p, seen, "duplicate delivery of cart A's first edit (ignored)", () ->
            p.ingest(new CartEvent.CartEdited("A", "user-a", 1, T0, ITEMS)));
        step(p, seen, "advance to +1h", () -> p.advanceTo(T0.plus(Duration.ofHours(1))));
        step(p, seen, "restart: timer index lost and rebuilt from cart records", p::restart);
        step(p, seen, "cart A purchased at +1h10m (stops the 24h reminder)", () ->
            p.ingest(new CartEvent.CartPurchased("A", "user-a", 2, T0.plus(Duration.ofMinutes(70)))));
        step(p, seen, "advance to +25h", () -> p.advanceTo(T0.plus(Duration.ofHours(25))));

        System.out.println("metrics");
        for (Map.Entry<String, Long> e : p.metrics().snapshot().entrySet()) {
            System.out.printf("  %-28s %d%n", e.getKey(), e.getValue());
        }
    }

    private static void step(Pipeline p, int[] seen, String label, Runnable action) {
        System.out.println("== " + label);
        action.run();
        List<RecordingNotificationSink.Sent> sent = p.sink().sent();
        for (int i = seen[0]; i < sent.size(); i++) {
            RecordingNotificationSink.Sent s = sent.get(i);
            System.out.printf("   FIRED  %s  cart=%s offset=%d key=%s%n",
                s.sentAt(), s.message().cartId(), LedgerKey.parse(s.message().key()).offsetIndex(), s.message().key());
        }
        if (seen[0] == sent.size()) System.out.println("   (no sends)");
        seen[0] = sent.size();
        System.out.println("   clock now " + p.clock().now() + ", pending timers " + p.timers().size());
        System.out.println();
    }
}
```

- [ ] **Step 10: Delete the legacy package**

```bash
git rm -rq src/main/java/com/quince/cartrecovery/legacy src/test/java/com/quince/cartrecovery/legacy
```

- [ ] **Step 11: Run the full suite and the demo**

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew test --console=plain`
Expected: `BUILD SUCCESSFUL`, 0 failures, and no test class under `com.quince.cartrecovery.legacy`.

Run: `JAVA_HOME=/Users/vikas/.sdkman/candidates/java/current ./gradlew run --console=plain -q`
Expected output includes, in order: `FIRED  2026-01-01T09:30:00Z  cart=A offset=0 key=A:1:0`, `FIRED  2026-01-01T09:50:00Z  cart=B offset=0 key=B:2:0`, `FIRED  2026-01-01T10:00:00Z  cart=A offset=1 key=A:1:1`, `FIRED  2026-01-01T10:20:00Z  cart=B offset=1 key=B:2:1`, `FIRED  2026-01-02T09:20:00Z  cart=B offset=2 key=B:2:2`; the metrics block shows `dispatch.sent 5`, `reminders.published 5`, `reconcile.timers_rebuilt 2`.

- [ ] **Step 12: Commit**

```bash
git add -A src
git commit -m "$(cat <<'MSG'
Rebuild the reconciler and pipeline on the new core, add verifier scenarios

Reconciler diffs open cart ids against the timer index and reloads only the
missing carts. Pipeline drains published intents into the dispatcher, stops
at retry and lease times, and gains detector-lag and dispatch-delay options.
The verifier keeps every send-time assertion, follows the metric renames,
and adds the lagging-detector, redelivered-check, and dispatch-delay
scenarios. The temporary legacy package is deleted.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

## Contract issues

All resolved; the master plan records the rulings.

1. Deletion timing and the two `Main.java` import lines in A1: resolved by controller ruling R15 (master §3).
2. The extra `Dispatcher` constructor with a trailing `DoubleSupplier jitter` is additive and test-only; C1b uses the frozen constructor. Accepted as written.
3. Contract time hooks: resolved by controller ruling R3. Only `TimerStoreContract` and `WatermarkContract` have time hooks; Redis subclasses sleep in `advance`; millisecond-exact boundaries live only in the in-memory subclasses (`InMemoryWatermarkContractTest.stalenessStartsStrictlyAfterFiveSeconds`); factories return an empty store.
4. `applyEvent` details (null `firstName` keeps the stored name, first-write `shopperKey` and arm, hour-rounded `openUntil`, input-ordered `getAll`): resolved by controller ruling R4; B1 matches.
5. Reminder outcomes use `Arm.TREATMENT`: resolved by controller ruling R18 (master §1.1).
6. Poison classification of deterministic SDK errors in the scheduler role: resolved by controller ruling R13 (C1a `SchedulerRole.fire`).
7. Retry-path dead letters rebuild `scheduledFor` from the cart; `retryDue` passes `Instant.EPOCH` as `sendBy`. Accepted as written.
8. `InMemorySendLedger.status(String)` returns `Optional<String>`: resolved by controller ruling R4 (master §1.4).

## Notes for the controller (other threads)

- B1/B2 contract factories: resolved by controller ruling R3 (B2's Redis subclasses `FLUSHALL` in the factory). `PriorityQueueTimerStore.existing` ignores its shard argument; `RedisTimerStore.existing` looks each id up in its own shard.
- Core metric names and role-level metrics: resolved by controller ruling R12 (listed in thread C1's global notes).
- C1c `Main` starts from A4's `Main.java`: controller ruling R15.
- `ReminderScheduler` has no `Clock`; its time is `Watermark.now()`, so C1a passes a `RedisWatermark`: controller ruling R13.
