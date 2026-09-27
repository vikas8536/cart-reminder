package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.ArmAssigner;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.TimerStore;

/**
 * Consumes cart events. Writes the timer first (monotonic upsert or conditional remove), then the conditional
 * cart update, so a crash between the two leaves at most a missing or extra timer: a stale timer is rejected by
 * the monotonic upsert or dropped on fire, and a redelivered event repeats both writes.
 */
public final class AbandonmentDetector {
    private final RecoveryConfig config;
    private final CartStateStore store;
    private final TimerStore timers;
    private final ArmAssigner arms;
    private final Metrics metrics;

    public AbandonmentDetector(RecoveryConfig config, CartStateStore store, TimerStore timers,
                               ArmAssigner arms, Metrics metrics) {
        this.config = config;
        this.store = store;
        this.timers = timers;
        this.arms = arms;
        this.metrics = metrics;
    }

    /** srcPartition is the cart-events partition the event was actually consumed from. */
    public void handle(CartEvent event, int srcPartition) {
        switch (event) {
            case CartEvent.CartEdited e -> timers.upsert(checkAbandon(e, srcPartition));
            case CartEvent.CartResumed e -> timers.upsert(checkAbandon(e, srcPartition));
            case CartEvent.CartCleared e -> timers.remove(e.cartId(), e.version());
            case CartEvent.CartPurchased e -> timers.remove(e.cartId(), e.version());
        }
        if (store.applyEvent(event, arms.assign(event.shopperKey()), srcPartition).isEmpty()) {
            metrics.increment("events.ignored");
            return;
        }
        metrics.increment("events.handled");
    }

    private Timer checkAbandon(CartEvent e, int srcPartition) {
        return Timer.checkAbandon(e.cartId(), e.version(), e.occurredAt().plus(config.window()), srcPartition);
    }
}
