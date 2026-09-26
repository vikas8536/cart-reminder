package com.quince.cartrecovery.legacy.core;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.core.ReminderPolicy;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.legacy.model.NotificationIntent;
import com.quince.cartrecovery.legacy.model.OutboxEntry;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.legacy.ports.CartStateStore;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.legacy.ports.Outbox;
import com.quince.cartrecovery.legacy.ports.SendLedger;
import com.quince.cartrecovery.legacy.ports.TimerStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Handles timer fires. Every fire reloads the record and compares the version tag,
 * so a cancelled or superseded timer is dropped rather than deleted. Reminder timers
 * chain: handling offset i schedules offset i + 1.
 */
public final class ReminderScheduler {
    private final RecoveryConfig config;
    private final ReminderPolicy policy;
    private final CartStateStore store;
    private final TimerStore timers;
    private final SendLedger ledger;
    private final Outbox outbox;
    private final Clock clock;
    private final Metrics metrics;

    public ReminderScheduler(RecoveryConfig config, CartStateStore store, TimerStore timers,
                             SendLedger ledger, Outbox outbox, Clock clock, Metrics metrics) {
        this.config = config;
        this.policy = new ReminderPolicy(config);
        this.store = store;
        this.timers = timers;
        this.ledger = ledger;
        this.outbox = outbox;
        this.clock = clock;
        this.metrics = metrics;
    }

    public void onTimer(Timer timer) {
        Optional<CartRecord> loaded = store.get(timer.cartId());
        if (loaded.isEmpty() || loaded.get().version() != timer.version()) {
            metrics.increment("timers.stale");
            return;
        }
        CartRecord record = loaded.get();
        switch (timer.kind()) {
            case CHECK_ABANDON -> checkAbandon(record);
            case REMINDER -> reminder(record, timer);
        }
    }

    private void checkAbandon(CartRecord record) {
        if (record.status() != CartStatus.ACTIVE) {
            metrics.increment("timers.wrong_status");
            return;
        }
        CartRecord abandoned = record.abandoned();
        if (!store.put(abandoned, record.version())) {
            metrics.increment("timers.conflict");
            return;
        }
        metrics.increment("carts.abandoned");
        if (!policy.eligible(abandoned, clock.now())) {
            metrics.increment(abandoned.arm() == Arm.HOLDOUT ? "carts.holdout" : "carts.cap_reached");
            return;
        }
        scheduleReminder(abandoned, 0);
    }

    private void reminder(CartRecord record, Timer timer) {
        if (record.status() != CartStatus.ABANDONED) {
            metrics.increment("timers.wrong_status");
            return;
        }
        int i = timer.offsetIndex();
        Instant now = clock.now();
        Duration bound = config.latenessBounds().get(i);
        if (now.isAfter(timer.dueAt().plus(bound))) {
            metrics.increment("reminders.skipped_late");
        } else if (!ledger.recordIfAbsent(record.cartId(), record.version(), i)) {
            metrics.increment("reminders.duplicate_timer");
        } else {
            NotificationIntent intent = new NotificationIntent(
                NotificationIntent.key(record.cartId(), record.version(), i),
                record.cartId(), record.shopperKey(), record.version(), i, timer.dueAt(), record.items());
            outbox.add(new OutboxEntry(intent, 0, now));
            metrics.increment("reminders.scheduled");
        }
        if (i + 1 < config.offsets().size()) {
            scheduleReminder(record, i + 1);
        }
    }

    private void scheduleReminder(CartRecord record, int offsetIndex) {
        Instant due = record.lastActivityAt().plus(config.offsets().get(offsetIndex));
        timers.upsert(Timer.reminder(record.cartId(), record.version(), offsetIndex, due));
    }
}
