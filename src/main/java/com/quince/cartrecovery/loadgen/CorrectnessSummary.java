package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Composes {@link Accounting} into the numbers the load test report needs, from outcomes (review fix 2). Every kind
 * count is restricted to {@code expectedKeys} first (fix round 2), so an outcome on a key {@link Expected} never counted
 * cannot cancel out a real miss. A key resolved SUPERSEDED at or before its sendBy is a correct non-send; after its
 * sendBy it is a lateness miss. The identity holds key for key:
 * {@code expected = sent + skippedLate + cancelled + dead + supersededBeforeSend + unexplainedMissing}, and
 * {@code unexplainedMissing = supersededAfterSendBy + neverSupersededNoOutcome}. {@link MissingBreakdown}'s script
 * inference, over the keys with no SENT, SKIPPED_LATE, CANCELLED or DEAD outcome, is reported beside it as a cross-check.
 */
public final class CorrectnessSummary {
    private CorrectnessSummary() {}

    public record Result(long sent, long skippedLate, long cancelled, long dead, long supersededBeforeSend,
                          long supersededAfterSendBy, long neverSupersededNoOutcome, long unexplainedMissing,
                          double unexplainedMissingRatio, long outcomesOnNonExpectedKeys,
                          MissingBreakdown.Result scriptInference) {}

    /**
     * @param expectedKeys {@code Expected.keys(scripts, config, assigner)}
     * @param shift        real minus nominal time: the loadgen replays the script shifted so its first event lands at the
     *                     test start, and a SUPERSEDED outcome carries the superseding event's real time
     */
    public static Result compute(Set<String> expectedKeys, List<OutcomeRow> outcomes, List<CartScript> scripts,
                                  RecoveryConfig config, ArmAssigner assigner, Duration shift) {
        Map<String, String> resolved = Accounting.resolveOutcomes(outcomes);
        Map<String, String> onExpected = Accounting.restrictToKeys(resolved, expectedKeys);

        long sent = Accounting.countByKind(onExpected, "SENT");
        long skippedLate = Accounting.countByKind(onExpected, "SKIPPED_LATE");
        long cancelled = Accounting.countByKind(onExpected, "CANCELLED");
        long dead = Accounting.countByKind(onExpected, "DEAD");
        long outcomesOnNonExpectedKeys = resolved.size() - onExpected.size();

        Map<String, Instant> sendBy = Expected.sendBy(scripts, config, assigner);
        Map<String, Instant> supersededAt = Accounting.supersededAt(outcomes);
        long before = 0;
        long after = 0;
        for (Map.Entry<String, String> e : onExpected.entrySet()) {
            if (!"SUPERSEDED".equals(e.getValue())) continue;
            if (supersededAt.get(e.getKey()).isAfter(sendBy.get(e.getKey()).plus(shift))) after++;
            else before++;
        }
        long neverSupersededNoOutcome = expectedKeys.size() - onExpected.size();
        long unexplainedMissing = Accounting.unexplainedMissing(expectedKeys.size(), sent, skippedLate, cancelled, dead, before);
        double ratio = Accounting.unexplainedMissingRatio(unexplainedMissing, expectedKeys.size());

        Set<String> otherOutcome = onExpected.entrySet().stream().filter(e -> !"SUPERSEDED".equals(e.getValue()))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
        MissingBreakdown.Result inferred = MissingBreakdown.compute(scripts, config, assigner, otherOutcome);

        return new Result(sent, skippedLate, cancelled, dead, before, after, neverSupersededNoOutcome,
            unexplainedMissing, ratio, outcomesOnNonExpectedKeys, inferred);
    }
}
