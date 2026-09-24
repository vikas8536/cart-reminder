package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.ArmAssigner;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.TimerStore;
import java.util.Optional;

/**
 * Consumes cart events. Keeps one record per cart and one version-tagged timer per cart.
 * Events whose version is at or below the stored version are ignored, which dedupes
 * at-least-once redelivery and drops out-of-order arrivals.
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

    public void handle(CartEvent event) {
        Optional<CartRecord> existing = store.get(event.cartId());
        long storedVersion = existing.map(CartRecord::version).orElse(CartStateStore.ABSENT);
        if (existing.isPresent() && event.version() <= storedVersion) {
            metrics.increment("events.ignored");
            return;
        }
        CartRecord base = existing.orElseGet(() ->
            CartRecord.fresh(event.cartId(), event.shopperKey(), arms.assign(event.shopperKey())));

        CartRecord updated = switch (event) {
            case CartEvent.CartEdited e -> base.activity(e.version(), e.occurredAt(), e.items());
            case CartEvent.CartResumed e -> base.activity(e.version(), e.occurredAt(), base.items());
            case CartEvent.CartCleared e -> base.closed(e.version(), e.occurredAt());
            case CartEvent.CartPurchased e -> base.closed(e.version(), e.occurredAt());
        };

        if (!store.put(updated, storedVersion)) {
            metrics.increment("events.conflict");
            return;
        }
        switch (updated.status()) {
            case ACTIVE -> timers.upsert(Timer.checkAbandon(
                updated.cartId(), updated.version(), updated.lastActivityAt().plus(config.window())));
            case CLOSED -> timers.remove(updated.cartId());
            case ABANDONED -> throw new IllegalStateException("events never produce ABANDONED");
        }
        metrics.increment("events.handled");
    }
}
