package com.quince.cartrecovery.legacy.core;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.core.ReminderPolicy;

import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.legacy.model.DeadLetter;
import com.quince.cartrecovery.legacy.model.NotificationIntent;
import com.quince.cartrecovery.legacy.model.OutboxEntry;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.legacy.ports.CartStateStore;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.legacy.ports.DeadLetterQueue;
import com.quince.cartrecovery.legacy.ports.NotificationSink;
import com.quince.cartrecovery.legacy.ports.Outbox;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Drains the outbox. Re-validates the cart and the reminder's lateness bound immediately
 * before each send attempt, retries transient failures with exponential backoff, dead-letters
 * permanent failures and exhausted retries, and replays dead letters under their original
 * idempotency key.
 */
public final class Dispatcher {
    private final RecoveryConfig config;
    private final CartStateStore store;
    private final Outbox outbox;
    private final NotificationSink sink;
    private final DeadLetterQueue dlq;
    private final Clock clock;
    private final Metrics metrics;

    public Dispatcher(RecoveryConfig config, CartStateStore store, Outbox outbox, NotificationSink sink,
                      DeadLetterQueue dlq, Clock clock, Metrics metrics) {
        this.config = config;
        this.store = store;
        this.outbox = outbox;
        this.sink = sink;
        this.dlq = dlq;
        this.clock = clock;
        this.metrics = metrics;
    }

    public void drain() {
        Instant now = clock.now();
        for (OutboxEntry entry : outbox.due(now)) {
            if (!stillWanted(entry)) {
                outbox.remove(entry.key());
                metrics.increment("dispatch.cancelled");
                continue;
            }
            if (late(entry, now)) {
                outbox.remove(entry.key());
                metrics.increment("dispatch.skipped_late");
                continue;
            }
            SendResult result = sink.send(entry.intent());
            switch (result) {
                case SENT -> {
                    outbox.remove(entry.key());
                    metrics.increment("dispatch.sent");
                }
                case PERMANENT_FAILURE -> deadLetter(entry, "permanent_failure");
                case TRANSIENT_FAILURE -> {
                    int attemptsSoFar = entry.attempts() + 1;
                    if (attemptsSoFar >= config.maxSendAttempts()) {
                        deadLetter(entry, "retries_exhausted");
                    } else {
                        outbox.replace(entry.retryAt(now.plus(backoff(attemptsSoFar))));
                        metrics.increment("dispatch.retry");
                    }
                }
            }
        }
    }

    /**
     * Re-enqueues every dead letter under its original key. The next drain applies the same
     * checks as any attempt, so a replay for a purchased cart or past its lateness bound is dropped.
     */
    public void replayDeadLetters() {
        for (DeadLetter letter : dlq.drain()) {
            outbox.add(new OutboxEntry(letter.intent(), 0, clock.now()));
            metrics.increment("dispatch.replayed");
        }
    }

    /** Last checkpoint before the gateway call: the cart must still be abandoned at the same version. */
    private boolean stillWanted(OutboxEntry entry) {
        Optional<CartRecord> record = store.get(entry.intent().cartId());
        return record.isPresent()
            && record.get().version() == entry.intent().version()
            && record.get().status() == CartStatus.ABANDONED;
    }

    /** Past its scheduled time plus lateness bound, a reminder is dropped, never sent. The bound itself is on time. */
    private boolean late(OutboxEntry entry, Instant now) {
        NotificationIntent intent = entry.intent();
        return now.isAfter(intent.scheduledFor().plus(config.latenessBounds().get(intent.offsetIndex())));
    }

    private void deadLetter(OutboxEntry entry, String reason) {
        outbox.remove(entry.key());
        dlq.add(new DeadLetter(entry.intent(), reason, clock.now()));
        metrics.increment("dispatch.dead_lettered");
    }

    private Duration backoff(int attempt) {
        return config.retryBase().multipliedBy(1L << (attempt - 1));
    }
}
