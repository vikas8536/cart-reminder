package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.DispatchConfig;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerDecision;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.IntentPublisher;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import com.quince.cartrecovery.ports.TimerStore;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Handles one claimed timer and tells the caller whether to ack or release it. Every fire reloads the cart
 * consistently and compares the version, so a superseded timer is dropped rather than deleted. CHECK_ABANDON
 * waits for the cart's partition watermark; REMINDER i publishes an intent stamped with sendBy and chains i + 1.
 * Exceptions from the ports propagate: the caller leaves the timer un-acked and its lease redelivers it.
 */
public final class ReminderScheduler {
    static final Duration MIN_HOLD = Duration.ofSeconds(1);
    static final Duration MAX_HOLD = Duration.ofSeconds(60);
    private static final TimerDecision ACK = new TimerDecision.Ack();

    private final RecoveryConfig config;
    private final DispatchConfig dispatch;
    private final ReminderPolicy policy;
    private final CartStateStore store;
    private final TimerStore timers;
    private final Watermark watermark;
    private final IntentPublisher intents;
    private final OutcomeRecorder outcomes;
    private final Metrics metrics;

    public ReminderScheduler(RecoveryConfig config, DispatchConfig dispatch, CartStateStore store, TimerStore timers,
                             Watermark watermark, IntentPublisher intents, OutcomeRecorder outcomes, Metrics metrics) {
        this.config = config;
        this.dispatch = dispatch;
        this.policy = new ReminderPolicy(config);
        this.store = store;
        this.timers = timers;
        this.watermark = watermark;
        this.intents = intents;
        this.outcomes = outcomes;
        this.metrics = metrics;
    }

    public TimerDecision onTimer(Timer timer) {
        return switch (timer.kind()) {
            case CHECK_ABANDON -> checkAbandon(timer);
            case REMINDER -> reminder(timer);
        };
    }

    private TimerDecision checkAbandon(Timer timer) {
        Instant needed = timer.dueAt().plus(dispatch.clockSkew());
        Instant w = watermark.current(timer.srcPartition());
        if (w.isBefore(needed)) {
            metrics.increment("timers.held");
            return new TimerDecision.Release(holdFor(w, needed));
        }
        Optional<CartRecord> loaded = current(timer);
        if (loaded.isEmpty()) return ACK;
        CartRecord record = loaded.get();
        Instant now = watermark.now();
        switch (record.status()) {
            case ACTIVE -> {
                List<Instant> starts = record.startsWith(record.lastActivityAt(), now, config.frequencyWindow());
                boolean eligible = policy.eligible(record.abandoned(), now);
                if (!store.markAbandoned(record, starts, eligible)) {
                    metrics.increment("timers.conflict");
                    return ACK;
                }
                metrics.increment("carts.abandoned");
                abandonedOutcome(record, now);
                if (eligible) {
                    armFirstReminder(record);
                } else {
                    metrics.increment(record.arm() == Arm.HOLDOUT ? "carts.holdout" : "carts.cap_reached");
                }
            }
            case ABANDONED -> {
                abandonedOutcome(record, now);
                if (policy.eligible(record, now)) armFirstReminder(record);
            }
            case CLOSED -> metrics.increment("timers.wrong_status");
        }
        return ACK;
    }

    private TimerDecision reminder(Timer timer) {
        int i = timer.offsetIndex();
        if (i < 0 || i >= config.offsets().size()) {
            metrics.increment("timers.poison");
            return ACK;
        }
        Optional<CartRecord> loaded = current(timer);
        if (loaded.isEmpty()) return ACK;
        CartRecord record = loaded.get();
        if (record.status() != CartStatus.ABANDONED) {
            metrics.increment("timers.wrong_status");
            return ACK;
        }
        Instant scheduledFor = timer.dueAt();
        intents.publish(new ReminderIntent(new LedgerKey(record.cartId(), record.version(), i).toString(),
            record.cartId(), record.version(), i, record.srcPartition(), scheduledFor,
            scheduledFor.plus(config.latenessBounds().get(i))));
        metrics.increment("reminders.published");
        if (i + 1 < config.offsets().size()) {
            timers.upsert(Timer.reminder(record.cartId(), record.version(), i + 1,
                record.lastActivityAt().plus(config.offsets().get(i + 1)), record.srcPartition()));
        } else {
            store.endSequence(record.cartId(), record.version());
        }
        return ACK;
    }

    /** The cart at the timer's version, or empty (counted timers.stale) when it is gone or has moved on. */
    private Optional<CartRecord> current(Timer timer) {
        Optional<CartRecord> loaded = store.get(timer.cartId());
        if (loaded.isEmpty() || loaded.get().version() != timer.version()) {
            metrics.increment("timers.stale");
            return Optional.empty();
        }
        return loaded;
    }

    private void armFirstReminder(CartRecord record) {
        timers.upsert(Timer.reminder(record.cartId(), record.version(), 0,
            record.lastActivityAt().plus(config.offsets().get(0)), record.srcPartition()));
    }

    private void abandonedOutcome(CartRecord record, Instant now) {
        outcomes.record(new Outcome(null, record.cartId(), record.version(), record.arm(), OutcomeKind.ABANDONED, now, 0));
    }

    /** clamp(needed - w, 1 s, 60 s); 60 s when the watermark is unknown or stale. */
    static Duration holdFor(Instant w, Instant needed) {
        if (w.equals(Instant.EPOCH)) return MAX_HOLD;
        Duration gap = Duration.between(w, needed);
        if (gap.compareTo(MIN_HOLD) < 0) return MIN_HOLD;
        return gap.compareTo(MAX_HOLD) > 0 ? MAX_HOLD : gap;
    }
}
