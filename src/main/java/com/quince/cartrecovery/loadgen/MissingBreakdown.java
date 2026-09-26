package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * Fix round 1: splits {@link Expected}'s keys that ended with no recorded outcome into two buckets,
 * using each key's cart script. This is a deliberate deviation from spec 8.5's plain "expected minus
 * every accounted outcome" formula (controller ruling on E2 fix round 1), because a correct pipeline
 * can legitimately produce no outcome at all for an expected key: a resume upserts
 * {@code CHECK_ABANDON(v+1)} over a still-pending {@code CHECK_ABANDON(v)} or {@code REMINDER(v,i)}
 * timer, and a purchase deletes the timer outright (see {@code AbandonmentDetector.handle}), so the
 * superseded cycle's timer is gone before it ever gets a chance to fire — no {@code SENT},
 * {@code SKIPPED_LATE}, {@code CANCELLED} or {@code DEAD} outcome is ever recorded for it.
 *
 * <p>A key is "superseded before send" when the event that superseded its cycle (the next cycle's
 * resume, or this cycle's purchase) happened at or before that offset's {@code sendBy}
 * ({@code scheduledFor + latenessBound}, the same deadline the dispatcher's pre-check enforces): the
 * pipeline was never actually late, the cart just moved on first, and a correct pipeline sending
 * nothing here is exactly right. Otherwise — the superseding event landed after {@code sendBy}, or
 * there was no superseding event at all — the key is "missed while lagging": the reminder should
 * have gone out (or been explicitly skipped-late/dead-lettered) before the cart moved on, and it
 * wasn't, so it stays a real failure.
 */
public final class MissingBreakdown {
    private MissingBreakdown() {}

    public record Result(long supersededBeforeSend, long missedWhileLagging) {}

    /**
     * @param keysWithOutcome every key that resolved to a terminal outcome (SENT, SKIPPED_LATE, CANCELLED
     *                        or DEAD) — e.g. {@code Accounting.resolveOutcomes(outcomes).keySet()}.
     */
    public static Result compute(List<CartScript> scripts, RecoveryConfig config, ArmAssigner assigner,
                                  Set<String> keysWithOutcome) {
        long superseded = 0;
        long missed = 0;
        List<Duration> offsets = config.offsets();
        List<Duration> latenessBounds = config.latenessBounds();
        for (CartScript script : scripts) {
            if (assigner.assign(script.shopperKey()) == Arm.HOLDOUT) continue;
            List<Cycle> cycles = script.cycles();
            for (int cycleIndex = 0; cycleIndex < cycles.size(); cycleIndex++) {
                if (cycleIndex + 1 > config.frequencyCap()) break;
                Cycle cycle = cycles.get(cycleIndex);
                for (int i = 0; i < offsets.size(); i++) {
                    Instant dueAt = cycle.lastActivityAt().plus(offsets.get(i));
                    // Not expected at all (Expected.keysForCycle): cancelled at or before its own due time.
                    if (cycle.cancelledAt() != null && !cycle.cancelledAt().isAfter(dueAt)) break;
                    String key = script.cartId() + ":" + cycle.version() + ":" + i;
                    if (keysWithOutcome.contains(key)) continue;   // has an outcome: not missing at all
                    Instant sendBy = dueAt.plus(latenessBounds.get(i));
                    if (cycle.cancelledAt() != null && !cycle.cancelledAt().isAfter(sendBy)) {
                        superseded++;
                    } else {
                        missed++;
                    }
                }
            }
        }
        return new Result(superseded, missed);
    }
}
