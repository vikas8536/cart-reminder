package com.quince.cartrecovery.core;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.InMemoryOutcomeRecorder;
import com.quince.cartrecovery.inmemory.PriorityQueueTimerStore;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
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
    private InMemoryOutcomeRecorder outcomes;
    private AbandonmentDetector detector;

    @BeforeEach
    void setUp() {
        clock = new FakeClock(T0);
        store = new InMemoryCartStateStore(RecoveryConfig.defaults(), 4);
        timers = new PriorityQueueTimerStore(clock, Duration.ofSeconds(90));
        metrics = new Metrics();
        outcomes = new InMemoryOutcomeRecorder();
        detector = new AbandonmentDetector(RecoveryConfig.defaults(), store, timers, key -> Arm.TREATMENT, outcomes, metrics);
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
            RecoveryConfig.defaults(), store, timers, key -> Arm.HOLDOUT, outcomes, metrics);
        holdoutDetector.handle(edited(1, min(0)), 0);
        detector.handle(edited(2, min(1)), 0);

        assertEquals(Arm.HOLDOUT, store.get(CART).orElseThrow().arm());
    }

    @Test
    void anEditWritesTheTimerBeforeTheCart() {
        AbandonmentDetector crashing = new AbandonmentDetector(
            RecoveryConfig.defaults(), failingApply(), timers, key -> Arm.TREATMENT, outcomes, metrics);

        assertThrows(IllegalStateException.class, () -> crashing.handle(edited(1, min(0)), 2));

        assertTrue(store.get(CART).isEmpty());
        assertEquals(List.of(Timer.checkAbandon(CART, 1, at(min(30)), 2)), dueAt(at(min(30))));
    }

    @Test
    void aPurchaseRemovesTheTimerBeforeTheCart() {
        detector.handle(edited(1, min(0)), 0);
        AbandonmentDetector crashing = new AbandonmentDetector(
            RecoveryConfig.defaults(), failingApply(), timers, key -> Arm.TREATMENT, outcomes, metrics);

        assertThrows(IllegalStateException.class, () -> crashing.handle(purchased(2, min(10)), 0));

        assertEquals(CartStatus.ACTIVE, store.get(CART).orElseThrow().status());
        assertEquals(0, timers.size());
    }

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
}
