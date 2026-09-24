package com.quince.cartrecovery;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.model.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * End-to-end scenarios from the spec, section 14. Each drives the pipeline on a fake clock
 * and asserts exactly which reminders would have fired, and when.
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

    private static List<String> sentKeys(Pipeline p) {
        return p.sink().sent().stream().map(s -> s.intent().idempotencyKey()).toList();
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
        assertEquals(1, p.metrics().get("reminders.duplicate_timer"));
        assertEquals(1, p.metrics().get("timers.wrong_status"));
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

    @Test @DisplayName("9. permanent failure dead-letters, replay sends once")
    void permanentFailureAndReplay() {
        Pipeline p = pipeline();
        p.sink().scriptOutcomes(SendResult.PERMANENT_FAILURE);
        p.ingest(edited(1, min(0)));
        p.advanceTo(at(min(40)));
        assertEquals(0, p.sink().sent().size());
        assertEquals(1, p.dlq().size());

        p.replayDeadLetters();
        p.replayDeadLetters();

        assertEquals(List.of("cart-1:1:0"), sentKeys(p));
        assertEquals(0, p.dlq().size());
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
        assertEquals(2, p.metrics().get("reminders.skipped_late"));
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
