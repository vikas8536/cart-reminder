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
}
