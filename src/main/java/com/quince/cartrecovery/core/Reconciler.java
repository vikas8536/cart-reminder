package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import com.quince.cartrecovery.ports.SendLedger;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Rebuilds missing timers for one shard from durable state. Compares keys only: open cart ids against the timer
 * index, in batches, and reloads just the carts whose timer is missing. An ACTIVE cart needs its abandonment
 * check; an eligible ABANDONED cart needs the first offset after the ledger's highest that is still within its
 * lateness bound, or its sequence ended when none remains. Offsets skipped as already late are recorded
 * SKIPPED_LATE (review fix 2), only when the rebuild's upsert wrote or its endSequence succeeded, so once, not on every
 * sweep. Idempotent, so it needs no lock.
 */
public final class Reconciler {
    static final int BATCH = 100;

    private final RecoveryConfig config;
    private final ReminderPolicy policy;
    private final CartStateStore store;
    private final TimerStore timers;
    private final SendLedger ledger;
    private final OutcomeRecorder outcomes;
    private final Clock clock;
    private final Metrics metrics;

    public Reconciler(RecoveryConfig config, CartStateStore store, TimerStore timers,
                      SendLedger ledger, OutcomeRecorder outcomes, Clock clock, Metrics metrics) {
        this.config = config;
        this.policy = new ReminderPolicy(config);
        this.store = store;
        this.timers = timers;
        this.ledger = ledger;
        this.outcomes = outcomes;
        this.clock = clock;
        this.metrics = metrics;
    }

    public void reconcileShard(int shard) {
        Instant now = clock.now();
        try (Stream<String> ids = store.openCartIds(shard, now)) {
            Iterator<String> it = ids.iterator();
            List<String> batch = new ArrayList<>(BATCH);
            while (it.hasNext()) {
                batch.add(it.next());
                if (batch.size() == BATCH || !it.hasNext()) {
                    rebuildMissing(shard, batch, now);
                    batch.clear();
                }
            }
        }
    }

    private void rebuildMissing(int shard, List<String> batch, Instant now) {
        Set<String> present = timers.existing(shard, batch);
        List<String> missing = batch.stream().filter(id -> !present.contains(id)).toList();
        if (missing.isEmpty()) return;
        for (CartRecord r : store.getAll(missing)) {
            switch (r.status()) {
                case ACTIVE -> rebuilt(Timer.checkAbandon(
                    r.cartId(), r.version(), r.lastActivityAt().plus(config.window()), r.srcPartition()));
                case ABANDONED -> {
                    if (!policy.eligible(r, now)) continue;
                    int from = ledger.highestOffsetIndex(r.cartId(), r.version()) + 1;
                    int next = nextOnTimeOffset(r, from, now);
                    boolean settled;
                    if (next < config.offsets().size()) {
                        settled = rebuilt(Timer.reminder(r.cartId(), r.version(), next,
                            r.lastActivityAt().plus(config.offsets().get(next)), r.srcPartition()));
                    } else {
                        settled = store.endSequence(r.cartId(), r.version());
                        if (settled) metrics.increment("reconcile.sequences_ended");
                    }
                    if (settled) skippedLate(r, from, next, now);
                }
                case CLOSED -> { }
            }
        }
    }

    /** First offset from {@code from} whose due time plus lateness bound has not passed. */
    private int nextOnTimeOffset(CartRecord r, int from, Instant now) {
        int i = from;
        while (i < config.offsets().size()
            && r.lastActivityAt().plus(config.offsets().get(i)).plus(config.latenessBounds().get(i)).isBefore(now)) {
            i++;
        }
        return i;
    }

    private boolean rebuilt(Timer timer) {
        boolean written = timers.upsert(timer).written();
        if (written) metrics.increment("reconcile.timers_rebuilt");
        return written;
    }

    /** One SKIPPED_LATE per offset in [from, next): each passed its lateness bound before the rebuild. */
    private void skippedLate(CartRecord r, int from, int next, Instant now) {
        for (int j = from; j < next; j++) {
            outcomes.record(new Outcome(new LedgerKey(r.cartId(), r.version(), j).toString(), r.cartId(), r.version(),
                Arm.TREATMENT, OutcomeKind.SKIPPED_LATE, now, 0));
            metrics.increment("reconcile.skipped_late");
        }
    }
}
