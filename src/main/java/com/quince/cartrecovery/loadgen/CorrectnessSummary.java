package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fix round 2: composes {@link Accounting} and {@link MissingBreakdown} into the numbers the load test
 * report needs. The four outcome-kind counts (sent, skippedLate, cancelled, dead) are restricted to
 * {@code expectedKeys} first — an outcome recorded for a key {@link Expected} never counted (a
 * real-vs-nominal timing edge at a cycle boundary, say) would otherwise silently cancel out a real miss
 * elsewhere in the unexplained-missing subtraction, with no trace of it in the report. With that
 * restriction the identity always holds, key for key:
 * {@code expected = sent + skippedLate + cancelled + dead + supersededBeforeSend + unexplainedMissing},
 * and {@code unexplainedMissing} is exactly {@code supersededAfterSendBy + neverSupersededNoOutcome}. Outcomes that did land on a
 * non-expected key are counted separately and reported, not silently dropped.
 */
public final class CorrectnessSummary {
    private CorrectnessSummary() {}

    public record Result(long sent, long skippedLate, long cancelled, long dead, long supersededBeforeSend,
                          long supersededAfterSendBy, long neverSupersededNoOutcome, long unexplainedMissing, double unexplainedMissingRatio,
                          long outcomesOnNonExpectedKeys) {}

    public static Result compute(Set<String> expectedKeys, List<OutcomeRow> outcomes, List<CartScript> scripts,
                                  RecoveryConfig config, ArmAssigner assigner) {
        Map<String, String> resolved = Accounting.resolveOutcomes(outcomes);
        Map<String, String> onExpected = Accounting.restrictToKeys(resolved, expectedKeys);

        long sent = Accounting.countByKind(onExpected, "SENT");
        long skippedLate = Accounting.countByKind(onExpected, "SKIPPED_LATE");
        long cancelled = Accounting.countByKind(onExpected, "CANCELLED");
        long dead = Accounting.countByKind(onExpected, "DEAD");
        long outcomesOnNonExpectedKeys = resolved.size() - onExpected.size();

        MissingBreakdown.Result missing = MissingBreakdown.compute(scripts, config, assigner, onExpected.keySet());
        long unexplainedMissing = Accounting.unexplainedMissing(expectedKeys.size(), sent, skippedLate, cancelled, dead,
            missing.supersededBeforeSend());
        double ratio = Accounting.unexplainedMissingRatio(unexplainedMissing, expectedKeys.size());

        return new Result(sent, skippedLate, cancelled, dead, missing.supersededBeforeSend(),
            missing.supersededAfterSendBy(), missing.neverSupersededNoOutcome(), unexplainedMissing, ratio, outcomesOnNonExpectedKeys);
    }
}
