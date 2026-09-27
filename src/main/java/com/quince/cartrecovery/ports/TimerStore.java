package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.Timer;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Derived due-time index, one timer per cart, rebuildable from durable state. Production: Redis scripts
 * on per-shard sorted sets, timed by Redis TIME. Timers compare by (version, offsetIndex), CHECK_ABANDON
 * having offsetIndex -1.
 */
public interface TimerStore {
    /** What an upsert did: whether it wrote, and the stored timer it overwrote (empty on a no-op or a first write). */
    record Upsert(boolean written, Optional<Timer> displaced) {}

    /** Writes only if (version, offsetIndex) is greater than the stored timer's; equal data is a no-op that keeps any lease. */
    Upsert upsert(Timer timer);

    /** Removes the cart's timer only if its version is <= version; returns the removed timer, empty if none was removed. */
    Optional<Timer> remove(String cartId, long version);

    /** Up to limit timers due by the store's time source; each is leased (re-due after the lease) until acked or released. */
    List<Timer> claimDue(int limit);

    /** If the stored timer still equals timer, makes it due again after delay. */
    void release(Timer timer, Duration delay);

    /** Removes the stored timer only if it still equals timer. */
    void ack(Timer timer);

    /** The subset of cartIds (all of this shard) that have a timer. */
    Set<String> existing(int shard, Collection<String> cartIds);
}
