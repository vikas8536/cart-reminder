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
