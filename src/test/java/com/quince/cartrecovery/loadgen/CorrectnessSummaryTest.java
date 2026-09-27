package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Review fix 2: the superseded and missing buckets come from reminder-outcomes, not script inference. Outcome counts
// stay restricted to expected keys (fix round 2), the identity holds, and the script inference is a cross-check.
class CorrectnessSummaryTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final RecoveryConfig CONFIG = RecoveryConfig.defaults();   // offsets 30m, 1h, 24h
    private static final ArmAssigner ALL_TREATMENT = shopperKey -> Arm.TREATMENT;
    private static final Instant DUE0 = T0.plus(CONFIG.offsets().get(0));
    private static final Instant SEND_BY0 = DUE0.plus(CONFIG.latenessBounds().get(0));

    private static OutcomeRow row(String key, String kind, Instant at) {
        return new OutcomeRow(key, key.substring(0, key.indexOf(':')), 1, "TREATMENT", kind, at, 0);
    }

    private static CorrectnessSummary.Result compute(List<CartScript> scripts, List<OutcomeRow> outcomes, Duration shift) {
        return CorrectnessSummary.compute(Expected.keys(scripts, CONFIG, ALL_TREATMENT), outcomes, scripts, CONFIG,
            ALL_TREATMENT, shift);
    }

    @Test void aNonExpectedOutcomeDoesNotCancelOutARealMissAndTheIdentityHolds() {
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(new Cycle(1L, T0, null)));
        Set<String> expectedKeys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);
        assertEquals(Set.of("cart-1:1:0", "cart-1:1:1", "cart-1:1:2"), expectedKeys);

        CorrectnessSummary.Result result = compute(List.of(script), List.of(row("cart-1:1:99", "SENT", T0)), Duration.ZERO);

        assertEquals(0, result.sent(), "the stray outcome must not be counted as a real sent key");
        assertEquals(1, result.outcomesOnNonExpectedKeys());
        assertEquals(0, result.supersededBeforeSend());
        assertEquals(0, result.supersededAfterSendBy());
        assertEquals(3, result.neverSupersededNoOutcome(), "all three expected keys have no outcome");
        assertEquals(3, result.unexplainedMissing());
        assertIdentity(expectedKeys.size(), result);
    }

    @Test void theBuggyBehaviorWouldHaveMadeUnexplainedSmallerThanTheRealMisses() {
        List<OutcomeRow> outcomes = List.of(row("cart-1:1:99", "SENT", T0));
        long buggySent = Accounting.countByKind(Accounting.resolveOutcomes(outcomes), "SENT");
        assertEquals(1, buggySent);
        assertEquals(2, Accounting.unexplainedMissing(3, buggySent, 0, 0, 0, 0));
    }

    @Test void aRealisticMixIsClassifiedFromOutcomesAndTheIdentityHolds() {
        CartScript sentScript = new CartScript("cart-sent", "s1", List.of(new Cycle(1L, T0, null)));
        CartScript supersededScript = new CartScript("cart-superseded", "s2", List.of(new Cycle(1L, T0, DUE0.plusSeconds(1))));
        CartScript missedScript = new CartScript("cart-missed", "s3", List.of(new Cycle(1L, T0, null)));
        CartScript lateScript = new CartScript("cart-late", "s4", List.of(new Cycle(1L, T0, SEND_BY0.plusSeconds(1))));
        List<CartScript> scripts = List.of(sentScript, supersededScript, missedScript, lateScript);

        List<OutcomeRow> outcomes = List.of(
            row("cart-sent:1:0", "SENT", T0), row("cart-sent:1:1", "SENT", T0), row("cart-sent:1:2", "SENT", T0),
            row("cart-superseded:1:0", "SUPERSEDED", DUE0.plusSeconds(1)),
            row("cart-late:1:0", "SUPERSEDED", SEND_BY0.plusSeconds(1)),
            row("some-other-cart:1:0", "SENT", T0));

        CorrectnessSummary.Result result = compute(scripts, outcomes, Duration.ZERO);

        assertEquals(3, result.sent());
        assertEquals(1, result.outcomesOnNonExpectedKeys());
        assertEquals(1, result.supersededBeforeSend());
        assertEquals(1, result.supersededAfterSendBy());
        assertEquals(3, result.neverSupersededNoOutcome());   // cart-missed's three offsets
        assertEquals(new MissingBreakdown.Result(1, 1, 3), result.scriptInference(), "the cross-check agrees here");
        assertIdentity(Expected.keys(scripts, CONFIG, ALL_TREATMENT).size(), result);
    }

    @Test void supersededExactlyAtTheShiftedSendByCountsAsBefore() {
        Duration shift = Duration.ofHours(1);
        CartScript script = new CartScript("cart-1", "s1", List.of(new Cycle(1L, T0, SEND_BY0)));

        CorrectnessSummary.Result onTime = compute(List.of(script),
            List.of(row("cart-1:1:0", "SUPERSEDED", SEND_BY0.plus(shift))), shift);
        CorrectnessSummary.Result late = compute(List.of(script),
            List.of(row("cart-1:1:0", "SUPERSEDED", SEND_BY0.plus(shift).plusMillis(1))), shift);

        assertEquals(1, onTime.supersededBeforeSend());
        assertEquals(0, onTime.unexplainedMissing());
        assertEquals(1, late.supersededAfterSendBy());
        assertEquals(1, late.unexplainedMissing());
    }

    @Test void aKeyThatWasSupersededAndThenCancelledCountsAsCancelled() {
        CartScript script = new CartScript("cart-1", "s1", List.of(new Cycle(1L, T0, DUE0.plusSeconds(1))));

        CorrectnessSummary.Result result = compute(List.of(script), List.of(
            row("cart-1:1:0", "SUPERSEDED", DUE0.plusSeconds(1)), row("cart-1:1:0", "CANCELLED", DUE0.plusSeconds(2))),
            Duration.ZERO);

        assertEquals(1, result.cancelled());
        assertEquals(0, result.supersededBeforeSend());
        assertIdentity(1, result);
    }

    private static void assertIdentity(long expected, CorrectnessSummary.Result r) {
        assertEquals(expected,
            r.sent() + r.skippedLate() + r.cancelled() + r.dead() + r.supersededBeforeSend() + r.unexplainedMissing(),
            "expected = sent + skipped + cancelled + dead + superseded-before-sendBy + unexplained");
        assertEquals(r.unexplainedMissing(), r.supersededAfterSendBy() + r.neverSupersededNoOutcome(),
            "unexplained = superseded-after-sendBy + never-superseded-no-outcome");
    }
}
