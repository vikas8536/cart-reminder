package com.quince.cartrecovery.core;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.InMemoryOutbox;
import com.quince.cartrecovery.inmemory.InMemorySendLedger;
import com.quince.cartrecovery.inmemory.PriorityQueueTimerStore;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.CartStateStore;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReminderSchedulerTest {
    private FakeClock clock;
    private InMemoryCartStateStore store;
    private PriorityQueueTimerStore timers;
    private InMemorySendLedger ledger;
    private InMemoryOutbox outbox;
    private Metrics metrics;
    private ReminderScheduler scheduler;

    @BeforeEach
    void setUp() {
        clock = new FakeClock(T0);
        store = new InMemoryCartStateStore();
        timers = new PriorityQueueTimerStore();
        ledger = new InMemorySendLedger();
        outbox = new InMemoryOutbox();
        metrics = new Metrics();
        scheduler = new ReminderScheduler(RecoveryConfig.defaults(), store, timers, ledger, outbox, clock, metrics);
    }

    private CartRecord activeRecord(long version, Arm arm) {
        CartRecord r = CartRecord.fresh(CART, SHOPPER, arm).activity(version, T0, ITEMS);
        store.put(r, CartStateStore.ABSENT);
        return r;
    }

    @Test
    void checkAbandonMarksAbandonedAndSchedulesFirstReminder() {
        activeRecord(1, Arm.TREATMENT);
        clock.set(at(min(30)));

        scheduler.onTimer(Timer.checkAbandon(CART, 1, at(min(30))));

        assertEquals(CartStatus.ABANDONED, store.get(CART).orElseThrow().status());
        assertEquals(List.of(Timer.reminder(CART, 1, 0, at(min(30)))), timers.popDue(at(min(30))));
        assertEquals(1, metrics.get("carts.abandoned"));
    }

    @Test
    void staleVersionTimerIsDropped() {
        activeRecord(2, Arm.TREATMENT);
        scheduler.onTimer(Timer.checkAbandon(CART, 1, at(min(30))));

        assertEquals(CartStatus.ACTIVE, store.get(CART).orElseThrow().status());
        assertEquals(0, timers.size());
        assertEquals(1, metrics.get("timers.stale"));
    }

    @Test
    void checkAbandonOnClosedCartIsDropped() {
        CartRecord r = activeRecord(1, Arm.TREATMENT);
        store.put(r.closed(2, at(min(10))), 1);
        scheduler.onTimer(Timer.checkAbandon(CART, 2, at(min(30))));

        assertEquals(0, timers.size());
        assertEquals(1, metrics.get("timers.wrong_status"));
    }

    @Test
    void holdoutCartIsAbandonedButGetsNoReminderTimer() {
        activeRecord(1, Arm.HOLDOUT);
        scheduler.onTimer(Timer.checkAbandon(CART, 1, at(min(30))));

        assertEquals(CartStatus.ABANDONED, store.get(CART).orElseThrow().status());
        assertEquals(0, timers.size());
        assertEquals(1, metrics.get("carts.holdout"));
    }

    @Test
    void frequencyCapStopsFurtherSequences() {
        scheduler = new ReminderScheduler(RecoveryConfig.defaults().withFrequencyCap(1),
            store, timers, ledger, outbox, clock, metrics);
        CartRecord r = activeRecord(1, Arm.TREATMENT);
        store.put(r.abandoned().activity(2, at(min(40)), ITEMS), 1);

        scheduler.onTimer(Timer.checkAbandon(CART, 2, at(min(70))));

        assertEquals(CartStatus.ABANDONED, store.get(CART).orElseThrow().status());
        assertEquals(0, timers.size());
        assertEquals(1, metrics.get("carts.cap_reached"));
    }

    @Test
    void reminderWritesLedgerAndOutboxAndChainsNextOffset() {
        CartRecord r = activeRecord(1, Arm.TREATMENT);
        store.put(r.abandoned(), 1);
        clock.set(at(min(30)));

        scheduler.onTimer(Timer.reminder(CART, 1, 0, at(min(30))));

        assertEquals(1, ledger.size());
        assertEquals(1, outbox.size());
        assertEquals("cart-1:1:0", outbox.due(clock.now()).get(0).key());
        assertEquals(List.of(Timer.reminder(CART, 1, 1, at(hrs(1)))), timers.popDue(at(hrs(1))));
        assertEquals(1, metrics.get("reminders.scheduled"));
    }

    @Test
    void lastReminderDoesNotChainAnotherTimer() {
        CartRecord r = activeRecord(1, Arm.TREATMENT);
        store.put(r.abandoned(), 1);
        clock.set(at(hrs(24)));

        scheduler.onTimer(Timer.reminder(CART, 1, 2, at(hrs(24))));

        assertEquals(1, outbox.size());
        assertEquals(0, timers.size());
    }

    @Test
    void duplicateReminderTimerWritesNothingTwice() {
        CartRecord r = activeRecord(1, Arm.TREATMENT);
        store.put(r.abandoned(), 1);
        clock.set(at(min(30)));

        scheduler.onTimer(Timer.reminder(CART, 1, 0, at(min(30))));
        scheduler.onTimer(Timer.reminder(CART, 1, 0, at(min(30))));

        assertEquals(1, ledger.size());
        assertEquals(1, outbox.size());
        assertEquals(1, metrics.get("reminders.duplicate_timer"));
    }

    @Test
    void reminderPastLatenessBoundIsSkippedButNextIsStillChained() {
        CartRecord r = activeRecord(1, Arm.TREATMENT);
        store.put(r.abandoned(), 1);
        clock.set(at(min(36)));

        scheduler.onTimer(Timer.reminder(CART, 1, 0, at(min(30))));

        assertEquals(0, outbox.size());
        assertEquals(0, ledger.size());
        assertEquals(1, metrics.get("reminders.skipped_late"));
        assertEquals(List.of(Timer.reminder(CART, 1, 1, at(hrs(1)))), timers.popDue(at(hrs(1))));
    }

    @Test
    void reminderForOlderCycleIsDroppedAfterReopenAndReabandon() {
        CartRecord r = activeRecord(1, Arm.TREATMENT);
        CartRecord secondCycle = r.abandoned().closed(2, at(min(40))).activity(3, at(min(50)), ITEMS).abandoned();
        store.put(secondCycle, 1);
        clock.set(at(min(80)));

        scheduler.onTimer(Timer.reminder(CART, 1, 1, at(hrs(1))));

        assertEquals(0, outbox.size());
        assertEquals(1, metrics.get("timers.stale"));
        assertTrue(timers.popDue(at(hrs(2))).isEmpty());
    }

    @Test
    void reminderOnActiveCartIsDropped() {
        activeRecord(1, Arm.TREATMENT);
        scheduler.onTimer(Timer.reminder(CART, 1, 0, at(min(30))));

        assertEquals(0, outbox.size());
        assertEquals(1, metrics.get("timers.wrong_status"));
    }
}
