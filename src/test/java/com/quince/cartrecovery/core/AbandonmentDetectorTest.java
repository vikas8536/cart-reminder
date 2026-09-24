package com.quince.cartrecovery.core;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.PriorityQueueTimerStore;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerKind;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbandonmentDetectorTest {
    private InMemoryCartStateStore store;
    private PriorityQueueTimerStore timers;
    private Metrics metrics;
    private AbandonmentDetector detector;

    @BeforeEach
    void setUp() {
        store = new InMemoryCartStateStore();
        timers = new PriorityQueueTimerStore();
        metrics = new Metrics();
        detector = new AbandonmentDetector(RecoveryConfig.defaults(), store, timers, key -> Arm.TREATMENT, metrics);
    }

    @Test
    void editCreatesActiveRecordAndCheckTimerAtLastActivityPlusWindow() {
        detector.handle(edited(1, min(0)));

        CartRecord r = store.get(CART).orElseThrow();
        assertEquals(CartStatus.ACTIVE, r.status());
        assertEquals(1, r.version());
        assertEquals(T0, r.lastActivityAt());
        assertEquals(ITEMS, r.items());
        List<Timer> due = timers.popDue(at(min(30)));
        assertEquals(List.of(Timer.checkAbandon(CART, 1, at(min(30)))), due);
    }

    @Test
    void laterEditResetsTheClockAndReplacesTheTimer() {
        detector.handle(edited(1, min(0)));
        detector.handle(edited(2, min(20)));

        assertTrue(timers.popDue(at(min(30))).isEmpty());
        List<Timer> due = timers.popDue(at(min(50)));
        assertEquals(1, due.size());
        assertEquals(2, due.get(0).version());
        assertEquals(TimerKind.CHECK_ABANDON, due.get(0).kind());
    }

    @Test
    void duplicateAndOutOfOrderEventsAreIgnored() {
        detector.handle(edited(2, min(20)));
        detector.handle(edited(2, min(20)));
        detector.handle(edited(1, min(0)));

        assertEquals(2, store.get(CART).orElseThrow().version());
        assertEquals(at(min(20)), store.get(CART).orElseThrow().lastActivityAt());
        assertEquals(2, metrics.get("events.ignored"));
        assertEquals(1, timers.size());
    }

    @Test
    void purchaseClosesTheRecordAndRemovesTheTimer() {
        detector.handle(edited(1, min(0)));
        detector.handle(purchased(2, min(10)));

        assertEquals(CartStatus.CLOSED, store.get(CART).orElseThrow().status());
        assertEquals(0, timers.size());
    }

    @Test
    void clearClosesTheRecordAndRemovesTheTimer() {
        detector.handle(edited(1, min(0)));
        detector.handle(cleared(2, min(10)));

        assertEquals(CartStatus.CLOSED, store.get(CART).orElseThrow().status());
        assertEquals(0, timers.size());
    }

    @Test
    void resumeCountsAsActivityAndKeepsItems() {
        detector.handle(edited(1, min(0)));
        detector.handle(resumed(2, min(15)));

        CartRecord r = store.get(CART).orElseThrow();
        assertEquals(CartStatus.ACTIVE, r.status());
        assertEquals(ITEMS, r.items());
        assertEquals(at(min(15)), r.lastActivityAt());
        assertEquals(List.of(Timer.checkAbandon(CART, 2, at(min(45)))), timers.popDue(at(min(45))));
    }

    @Test
    void purchaseForUnknownCartCreatesClosedRecordSoOlderEditsAreIgnored() {
        detector.handle(purchased(5, min(0)));
        detector.handle(edited(3, min(1)));

        CartRecord r = store.get(CART).orElseThrow();
        assertEquals(CartStatus.CLOSED, r.status());
        assertEquals(5, r.version());
        assertEquals(0, timers.size());
        assertEquals(1, metrics.get("events.ignored"));
    }

    @Test
    void redeliveredEditAfterAbandonmentLeavesAbandonedStateAlone() {
        detector.handle(edited(1, min(0)));
        CartRecord abandoned = store.get(CART).orElseThrow().abandoned();
        store.put(abandoned, 1);
        timers.upsert(Timer.reminder(CART, 1, 0, at(min(30))));

        detector.handle(edited(1, min(0)));

        assertEquals(CartStatus.ABANDONED, store.get(CART).orElseThrow().status());
        assertEquals(TimerKind.REMINDER, timers.popDue(at(min(30))).get(0).kind());
    }

    @Test
    void assignsArmOnFirstSightAndKeepsIt() {
        AbandonmentDetector holdoutDetector = new AbandonmentDetector(
            RecoveryConfig.defaults(), store, timers, key -> Arm.HOLDOUT, metrics);
        holdoutDetector.handle(edited(1, min(0)));
        detector.handle(edited(2, min(1)));

        assertEquals(Arm.HOLDOUT, store.get(CART).orElseThrow().arm());
    }
}
