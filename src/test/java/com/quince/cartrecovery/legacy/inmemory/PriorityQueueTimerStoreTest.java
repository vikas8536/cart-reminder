package com.quince.cartrecovery.legacy.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Timer;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PriorityQueueTimerStoreTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void popsDueTimersInDueOrder() {
        PriorityQueueTimerStore store = new PriorityQueueTimerStore();
        store.upsert(Timer.checkAbandon("b", 1, T0.plusSeconds(20)));
        store.upsert(Timer.checkAbandon("a", 1, T0.plusSeconds(10)));
        store.upsert(Timer.checkAbandon("c", 1, T0.plusSeconds(30)));

        List<Timer> due = store.popDue(T0.plusSeconds(20));

        assertEquals(List.of("a", "b"), due.stream().map(Timer::cartId).toList());
        assertEquals(Optional.of(T0.plusSeconds(30)), store.nextDueAt());
    }

    @Test
    void upsertReplacesTheTimerForTheSameCart() {
        PriorityQueueTimerStore store = new PriorityQueueTimerStore();
        store.upsert(Timer.checkAbandon("a", 1, T0.plusSeconds(10)));
        store.upsert(Timer.checkAbandon("a", 2, T0.plusSeconds(50)));

        assertTrue(store.popDue(T0.plusSeconds(10)).isEmpty());
        List<Timer> due = store.popDue(T0.plusSeconds(50));
        assertEquals(1, due.size());
        assertEquals(2, due.get(0).version());
    }

    @Test
    void removeAndClearDropTimers() {
        PriorityQueueTimerStore store = new PriorityQueueTimerStore();
        store.upsert(Timer.checkAbandon("a", 1, T0));
        store.upsert(Timer.checkAbandon("b", 1, T0));
        store.remove("a");
        assertEquals(1, store.size());
        store.clear();
        assertEquals(0, store.size());
        assertEquals(Optional.empty(), store.nextDueAt());
    }
}
