package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerKind;
import com.quince.cartrecovery.ports.ArmAssigner;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Instant;
import java.util.Optional;

/**
 * Consumes cart events. Writes the timer first (monotonic upsert or conditional remove), then the conditional
 * cart update, so a crash between the two leaves at most a missing or extra timer: a stale timer is rejected by
 * the monotonic upsert or dropped on fire, and a redelivered event repeats both writes. Review fix 2: a timer write
 * that displaces reminders already owed records SUPERSEDED for them. A redelivered event's write is a no-op and
 * displaces nothing, so it records nothing twice; a crash between the timer write and the outcome loses that outcome.
 */
public final class AbandonmentDetector {
    private final RecoveryConfig config;
    private final ReminderPolicy policy;
    private final CartStateStore store;
    private final TimerStore timers;
    private final ArmAssigner arms;
    private final OutcomeRecorder outcomes;
    private final Metrics metrics;

    public AbandonmentDetector(RecoveryConfig config, CartStateStore store, TimerStore timers,
                               ArmAssigner arms, OutcomeRecorder outcomes, Metrics metrics) {
        this.config = config;
        this.policy = new ReminderPolicy(config);
        this.store = store;
        this.timers = timers;
        this.arms = arms;
        this.outcomes = outcomes;
        this.metrics = metrics;
    }

    /** srcPartition is the cart-events partition the event was actually consumed from. */
    public void handle(CartEvent event, int srcPartition) {
        Optional<Timer> displaced = switch (event) {
            case CartEvent.CartEdited e -> timers.upsert(checkAbandon(e, srcPartition)).displaced();
            case CartEvent.CartResumed e -> timers.upsert(checkAbandon(e, srcPartition)).displaced();
            case CartEvent.CartCleared e -> timers.remove(e.cartId(), e.version());
            case CartEvent.CartPurchased e -> timers.remove(e.cartId(), e.version());
        };
        displaced.ifPresent(t -> superseded(t, event.occurredAt()));
        if (store.applyEvent(event, arms.assign(event.shopperKey()), srcPartition).isEmpty()) {
            metrics.increment("events.ignored");
            return;
        }
        metrics.increment("events.handled");
    }

    /**
     * SUPERSEDED for each reminder of the displaced timer's cycle due at or before occurredAt, offset j being due at
     * base + offset_j. A REMINDER(v, i) owes offsets j >= i (base = its dueAt - offset_i). An overdue CHECK_ABANDON(v)
     * owes offsets from 0 (base = its dueAt - window), but only for a cart the check would have given a sequence; that
     * cart is read here, before the cart update, and only on this rare path. A timer not yet due owes nothing.
     */
    private void superseded(Timer displaced, Instant occurredAt) {
        boolean reminder = displaced.kind() == TimerKind.REMINDER;
        int from = reminder ? displaced.offsetIndex() : 0;
        if (from < 0 || from >= config.offsets().size()) return;
        Instant base = displaced.dueAt().minus(reminder ? config.offsets().get(from) : config.window());
        if (base.plus(config.offsets().get(from)).isAfter(occurredAt)) return;
        if (!reminder && !sequenceOwed(displaced)) return;
        for (int j = from; j < config.offsets().size(); j++) {
            if (base.plus(config.offsets().get(j)).isAfter(occurredAt)) break;   // offsets increase: later ones are later still
            String key = new LedgerKey(displaced.cartId(), displaced.version(), j).toString();
            outcomes.record(new Outcome(key, displaced.cartId(), displaced.version(), Arm.TREATMENT,
                OutcomeKind.SUPERSEDED, occurredAt, 0));
            metrics.increment("reminders.superseded");
        }
    }

    /** As the overdue check would have found the cart: still ACTIVE at its version, treatment, within the cap at its due time. */
    private boolean sequenceOwed(Timer check) {
        Optional<CartRecord> cart = store.get(check.cartId());
        return cart.isPresent() && cart.get().version() == check.version() && cart.get().status() == CartStatus.ACTIVE
            && policy.eligible(cart.get().abandoned(), check.dueAt());
    }

    private Timer checkAbandon(CartEvent e, int srcPartition) {
        return Timer.checkAbandon(e.cartId(), e.version(), e.occurredAt().plus(config.window()), srcPartition);
    }
}
