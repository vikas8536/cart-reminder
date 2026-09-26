package com.quince.cartrecovery.legacy.core;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.core.ReminderPolicy;

import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.legacy.ports.CartStateStore;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.legacy.ports.SendLedger;
import com.quince.cartrecovery.legacy.ports.TimerStore;

/**
 * Rebuilds the timer index from durable state. The timer store is derived data:
 * an ACTIVE cart needs its abandonment check, an ABANDONED cart needs the first reminder
 * after the highest offset already in the ledger that is still within its lateness bound.
 */
public final class Reconciler {
    private final RecoveryConfig config;
    private final ReminderPolicy policy;
    private final CartStateStore store;
    private final TimerStore timers;
    private final SendLedger ledger;
    private final Clock clock;
    private final Metrics metrics;

    public Reconciler(RecoveryConfig config, CartStateStore store, TimerStore timers,
                      SendLedger ledger, Clock clock, Metrics metrics) {
        this.config = config;
        this.policy = new ReminderPolicy(config);
        this.store = store;
        this.timers = timers;
        this.ledger = ledger;
        this.clock = clock;
        this.metrics = metrics;
    }

    public void rebuildTimers() {
        for (CartRecord r : store.scanOpen()) {
            switch (r.status()) {
                case ACTIVE -> rebuilt(Timer.checkAbandon(
                    r.cartId(), r.version(), r.lastActivityAt().plus(config.window())));
                case ABANDONED -> {
                    if (!policy.eligible(r, clock.now())) continue;
                    int next = nextOnTimeOffset(r, ledger.highestOffsetIndex(r.cartId(), r.version()) + 1);
                    if (next < config.offsets().size()) {
                        rebuilt(Timer.reminder(r.cartId(), r.version(), next,
                            r.lastActivityAt().plus(config.offsets().get(next))));
                    }
                }
                case CLOSED -> { }
            }
        }
    }

    /**
     * First offset from {@code from} whose due time plus lateness bound has not passed. Offsets already
     * past their bound were skipped (or would be), so rebuilding them would only re-run the skip.
     */
    private int nextOnTimeOffset(CartRecord r, int from) {
        int i = from;
        while (i < config.offsets().size()
            && r.lastActivityAt().plus(config.offsets().get(i)).plus(config.latenessBounds().get(i)).isBefore(clock.now())) {
            i++;
        }
        return i;
    }

    private void rebuilt(Timer timer) {
        timers.upsert(timer);
        metrics.increment("reconcile.timers_rebuilt");
    }
}
