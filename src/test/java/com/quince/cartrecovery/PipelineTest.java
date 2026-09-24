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
        assertEquals(3, p.metrics().get("reminders.skipped_late"));
        assertEquals(0, p.sink().sent().size());
        assertEquals(0, p.timers().size());

        p.restart();
        assertEquals(0, p.metrics().get("reconcile.timers_rebuilt"));
        assertEquals(0, p.timers().size());
        p.advanceTo(at(hrs(48)));

        assertEquals(0, p.sink().sent().size());
        assertEquals(3, p.metrics().get("reminders.skipped_late"));
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
            p.sink().sent().stream().map(s -> s.intent().idempotencyKey()).toList());
        assertEquals(0, p.metrics().get("reminders.skipped_late"));
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
