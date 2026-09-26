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
