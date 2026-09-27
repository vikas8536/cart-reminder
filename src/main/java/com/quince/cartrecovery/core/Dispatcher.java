package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.model.DispatchConfig;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.HandleResult;
import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.DeadLetterQueue;
import com.quince.cartrecovery.ports.NotificationSink;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import com.quince.cartrecovery.ports.SendBudget;
import com.quince.cartrecovery.ports.SendLedger;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Sends reminders at most once per key. Every attempt takes a send token, passes the watermark gate for the cart's
 * recorded partition, and holds a fenced ledger lease; sendBy is checked before the token, after the claim, and
 * immediately before the send. Only a send keeps its token (review fix 3): a gate hold, a lost claim, a cancel or late
 * skip after the cart re-read, and a lease too short for the gateway each return it. An exception keeps it, so a refund
 * never follows a send. Outcome and dead-letter records are produced before the ledger finish, so a crash in between
 * only duplicates records that consumers already resolve.
 */
public final class Dispatcher {
    /** Stands in for sendBy on a retry row that has disappeared: the claim then recreates it already late. */
    private static final Instant GONE = Instant.EPOCH;

    private final RecoveryConfig config;
    private final DispatchConfig dispatch;
    private final CartStateStore store;
    private final SendLedger ledger;
    private final Watermark watermark;
    private final SendBudget budget;
    private final NotificationSink sink;
    private final OutcomeRecorder outcomes;
    private final DeadLetterQueue dlq;
    private final Clock clock;
    private final Metrics metrics;
    private final DoubleSupplier jitter;

    public Dispatcher(RecoveryConfig config, DispatchConfig dispatch, CartStateStore store, SendLedger ledger,
                      Watermark watermark, SendBudget budget, NotificationSink sink, OutcomeRecorder outcomes,
                      DeadLetterQueue dlq, Clock clock, Metrics metrics) {
        this(config, dispatch, store, ledger, watermark, budget, sink, outcomes, dlq, clock, metrics,
            () -> ThreadLocalRandom.current().nextDouble());
    }

    /** jitter returns a fraction in [0, 1] of the full backoff; tests and the fake-clock pipeline pass a constant. */
    public Dispatcher(RecoveryConfig config, DispatchConfig dispatch, CartStateStore store, SendLedger ledger,
                      Watermark watermark, SendBudget budget, NotificationSink sink, OutcomeRecorder outcomes,
                      DeadLetterQueue dlq, Clock clock, Metrics metrics, DoubleSupplier jitter) {
        this.config = config;
        this.dispatch = dispatch;
        this.store = store;
        this.ledger = ledger;
        this.watermark = watermark;
        this.budget = budget;
        this.sink = sink;
        this.outcomes = outcomes;
        this.dlq = dlq;
        this.clock = clock;
        this.metrics = metrics;
        this.jitter = jitter;
    }

    /** Spec §6.2 steps 1 to 7 for one consumed intent. HOLD asks the caller to pause the partition and redeliver. */
    public HandleResult handle(ReminderIntent intent) {
        if (clock.now().isAfter(intent.sendBy())) {
            outcome(intent.key(), OutcomeKind.SKIPPED_LATE, 0);
            metrics.increment("dispatch.skipped_late_precheck");
            return HandleResult.DONE;
        }
        Lane lane = Lane.of(intent.offsetIndex(), dispatch.fastOffsets());
        if (!budget.tryAcquire(lane)) {
            metrics.increment("dispatch.no_token");
            return HandleResult.HOLD;
        }
        metrics.increment("dispatch.token_taken");
        if (lagging(intent.srcPartition())) {
            refund(lane);
            metrics.increment("dispatch.held");
            return HandleResult.HOLD;
        }
        ClaimResult claim = ledger.claim(intent.key(), intent.sendBy(), intent.srcPartition(), clock.now());
        if (claim instanceof ClaimResult.Claimed c) {
            if (!attempt(intent.key(), c, intent.scheduledFor())) refund(lane);
        } else {
            refund(lane);
            metrics.increment("dispatch.duplicate");
        }
        return HandleResult.DONE;
    }

    /**
     * One pass of the retry loop over a shard: watermark gate, then a token, then the claim, then steps 5 to 7.
     * The token is taken before the claim, so the loop never holds a lease while waiting for capacity.
     */
    public void retryDue(int shard, int limit) {
        for (DueRetry due : ledger.dueRetries(shard, clock.now(), limit)) {
            if (lagging(due.srcPartition())) {
                metrics.increment("dispatch.retry_held");
                continue;
            }
            Lane lane = Lane.of(LedgerKey.parse(due.key()).offsetIndex(), dispatch.fastOffsets());
            if (!budget.tryAcquire(lane)) {
                metrics.increment("dispatch.retry_no_token");
                continue;
            }
            metrics.increment("dispatch.token_taken");
            ClaimResult claim = ledger.claim(due.key(), GONE, due.srcPartition(), clock.now());
            if (claim instanceof ClaimResult.Claimed c) {
                if (!attempt(due.key(), c, null)) refund(lane);
            } else {
                refund(lane);
                metrics.increment("dispatch.duplicate");
            }
        }
    }

    /** Reopens every dead-lettered key except poison; the retry loop then sends or skips it. Replaying twice is harmless. */
    public void replay(List<DeadLetter> letters) {
        for (DeadLetter letter : letters) {
            if (DeadLetter.REASON_POISON.equals(letter.reason())) {
                metrics.increment("dispatch.replay_poison_skipped");
            } else if (ledger.reopen(letter.intent().key(), clock.now())) {
                metrics.increment("dispatch.replayed");
            }
        }
    }

    /**
     * Steps 5 to 7, holding the lease c; true only if the sink was called. scheduledFor is null on the retry path,
     * where it is rebuilt from the cart.
     */
    private boolean attempt(String key, ClaimResult.Claimed c, Instant scheduledFor) {
        if (clock.now().isAfter(c.sendBy())) {
            skipLate(key, c);
            return false;
        }
        LedgerKey k = LedgerKey.parse(key);
        Optional<CartRecord> cart = store.get(k.cartId());
        if (cart.isEmpty() || cart.get().version() != k.version() || cart.get().status() != CartStatus.ABANDONED) {
            outcome(key, OutcomeKind.CANCELLED, c.attempts());
            metrics.increment("dispatch.cancelled");
            finish(key, c, OutcomeKind.CANCELLED, "cancelled");
            return false;
        }
        Instant now = clock.now();
        if (now.isAfter(c.sendBy())) {
            skipLate(key, c);
            return false;
        }
        if (Duration.between(now, c.leaseUntil()).compareTo(dispatch.gatewayTimeout()) < 0) {
            metrics.increment("dispatch.lease_expiring");
            return false;
        }
        CartRecord r = cart.get();
        SendResult result = sink.send(new ReminderMessage(key, r.cartId(), r.shopperKey(), r.firstName(), r.items()));
        switch (result) {
            case SENT -> {
                outcome(key, OutcomeKind.SENT, c.attempts());
                metrics.increment("dispatch.sent");
                finish(key, c, OutcomeKind.SENT, null);
            }
            case TRANSIENT_FAILURE -> {
                if (c.attempts() < config.maxSendAttempts()) {
                    if (ledger.markRetry(key, c.token(), now.plus(backoff(c.attempts())))) {
                        metrics.increment("dispatch.retry");
                    } else {
                        metrics.increment("dispatch.lease_lost");
                    }
                } else {
                    deadLetter(key, c, r, scheduledFor, "retries_exhausted");
                }
            }
            case PERMANENT_FAILURE -> deadLetter(key, c, r, scheduledFor, "permanent_failure");
        }
        return true;
    }

    /** A token taken for this attempt that no send used goes back to the budget. */
    private void refund(Lane lane) {
        budget.release(lane);
        metrics.increment("dispatch.token_refunded");
    }

    private void skipLate(String key, ClaimResult.Claimed c) {
        outcome(key, OutcomeKind.SKIPPED_LATE, c.attempts());
        metrics.increment("dispatch.skipped_late");
        finish(key, c, OutcomeKind.SKIPPED_LATE, "late");
    }

    private void deadLetter(String key, ClaimResult.Claimed c, CartRecord r, Instant scheduledFor, String reason) {
        LedgerKey k = LedgerKey.parse(key);
        Instant scheduled = scheduledFor != null ? scheduledFor
            : k.offsetIndex() < config.offsets().size() ? r.lastActivityAt().plus(config.offsets().get(k.offsetIndex()))
            : c.sendBy();
        dlq.add(new DeadLetter(new ReminderIntent(key, k.cartId(), k.version(), k.offsetIndex(), c.srcPartition(),
            scheduled, c.sendBy()), reason, clock.now()));
        outcome(key, OutcomeKind.DEAD, c.attempts());
        metrics.increment("dispatch.dead_lettered");
        finish(key, c, OutcomeKind.DEAD, reason);
    }

    private void finish(String key, ClaimResult.Claimed c, OutcomeKind kind, String reason) {
        if (!ledger.finish(key, c.token(), kind, reason)) metrics.increment("dispatch.lease_lost");
    }

    /** Reminder outcomes are TREATMENT by construction: holdout carts never get an intent. */
    private void outcome(String key, OutcomeKind kind, int attempts) {
        LedgerKey k = LedgerKey.parse(key);
        outcomes.record(new Outcome(key, k.cartId(), k.version(), Arm.TREATMENT, kind, clock.now(), attempts));
    }

    /** Behind if the partition's watermark is older than the watermark clock minus CLOCK_SKEW. */
    private boolean lagging(int srcPartition) {
        return watermark.current(srcPartition).isBefore(watermark.now().minus(dispatch.clockSkew()));
    }

    /** Full jitter: a fraction of retryBase x 2^(attempts - 1). */
    private Duration backoff(int attempts) {
        long capMillis = config.retryBase().multipliedBy(1L << (attempts - 1)).toMillis();
        return Duration.ofMillis((long) (capMillis * jitter.getAsDouble()));
    }
}
