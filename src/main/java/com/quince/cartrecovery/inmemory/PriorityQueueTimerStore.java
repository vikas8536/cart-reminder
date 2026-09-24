package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;

/** Map by cart id for upsert semantics, priority queue for due ordering. Stale queue entries are skipped on pop. */
public final class PriorityQueueTimerStore implements TimerStore {
    private static final Comparator<Timer> BY_DUE = Comparator.comparing(Timer::dueAt).thenComparing(Timer::cartId);

    private final Map<String, Timer> byCart = new HashMap<>();
    private final PriorityQueue<Timer> queue = new PriorityQueue<>(BY_DUE);

    @Override public void upsert(Timer timer) {
        byCart.put(timer.cartId(), timer);
        queue.add(timer);
    }

    @Override public void remove(String cartId) {
        byCart.remove(cartId);
        compact();
    }

    @Override public List<Timer> popDue(Instant upTo) {
        List<Timer> due = new ArrayList<>();
        compact();
        while (!queue.isEmpty() && !queue.peek().dueAt().isAfter(upTo)) {
            Timer t = queue.poll();
            byCart.remove(t.cartId());
            due.add(t);
            compact();
        }
        return due;
    }

    @Override public Optional<Instant> nextDueAt() {
        compact();
        return Optional.ofNullable(queue.peek()).map(Timer::dueAt);
    }

    @Override public int size() { return byCart.size(); }

    @Override public void clear() {
        byCart.clear();
        queue.clear();
    }

    /** Drops queue heads that no longer match the live timer for their cart. */
    private void compact() {
        while (!queue.isEmpty() && !queue.peek().equals(byCart.get(queue.peek().cartId()))) {
            queue.poll();
        }
    }
}
