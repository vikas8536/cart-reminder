package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Fix round 2: LoadgenRole used to count sent/skippedLate/cancelled/dead over every resolved outcome,
// including ones on keys Expected never counted. That let a non-expected outcome silently cancel out a
// real miss in the unexplainedMissing subtraction, making it smaller than the missing buckets (then "missed while lagging") even though
// the report claimed the latter "counts in unexplained". These tests pin the fix: outcome counts are
// restricted to expectedKeys, non-expected outcomes are reported on their own, and the full identity holds.
class CorrectnessSummaryTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final RecoveryConfig CONFIG = RecoveryConfig.defaults();   // offsets 30m, 1h, 24h
    private static final ArmAssigner ALL_TREATMENT = shopperKey -> Arm.TREATMENT;

    @Test void aNonExpectedOutcomeDoesNotCancelOutARealMissAndTheIdentityHolds() {
        // One cart, one cycle, never resumes or purchases: three expected keys, none of which ever fire.
        Cycle cycle = new Cycle(1L, T0, null);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));
        Set<String> expectedKeys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);
        assertEquals(Set.of("cart-1:1:0", "cart-1:1:1", "cart-1:1:2"), expectedKeys);

        // A SENT outcome on a key Expected never produced (offset index 99 is out of range for this
        // script) — the kind of stray outcome a real-vs-nominal timing edge, or a stale replay, can leave.
        List<OutcomeRow> outcomes = List.of(
            new OutcomeRow("cart-1:1:99", "cart-1", 1, "TREATMENT", "SENT", T0, 1));

        CorrectnessSummary.Result result = CorrectnessSummary.compute(expectedKeys, outcomes, List.of(script), CONFIG, ALL_TREATMENT);

        assertEquals(0, result.sent(), "the stray outcome must not be counted as a real sent key");
        assertEquals(1, result.outcomesOnNonExpectedKeys());
        assertEquals(0, result.supersededBeforeSend());
        assertEquals(0, result.supersededAfterSendBy());
        assertEquals(3, result.neverSupersededNoOutcome(), "all three expected keys are genuinely missing");
        assertEquals(3, result.unexplainedMissing(), "unexplainedMissing must equal the two missing buckets' sum exactly");
        assertIdentity(expectedKeys.size(), result);
    }

    @Test void theBuggyBehaviorWouldHaveMadeUnexplainedSmallerThanMissedWhileLagging() {
        // Same script as above, but demonstrating what fix round 1's LoadgenRole did wrong: counting the
        // stray outcome against the WHOLE resolved map (not restricted to expectedKeys) would have counted
        // it as "sent", reducing unexplainedMissing below the true missing-bucket count.
        Cycle cycle = new Cycle(1L, T0, null);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));
        List<OutcomeRow> outcomes = List.of(new OutcomeRow("cart-1:1:99", "cart-1", 1, "TREATMENT", "SENT", T0, 1));

        var resolvedUnrestricted = Accounting.resolveOutcomes(outcomes);
        long buggySent = Accounting.countByKind(resolvedUnrestricted, "SENT");
        long buggyUnexplained = Accounting.unexplainedMissing(3, buggySent, 0, 0, 0, 0);

        assertEquals(1, buggySent);
        assertEquals(2, buggyUnexplained, "the bug: unexplained (2) ends up smaller than the real miss count (3)");
    }

    @Test void aRealisticMixStillSatisfiesTheIdentity() {
        Cycle sentCycle = new Cycle(1L, T0, null);
        CartScript sentScript = new CartScript("cart-sent", "shopper-sent", List.of(sentCycle));

        Instant dueAt = T0.plus(CONFIG.offsets().get(0));
        Instant sendBy = dueAt.plus(CONFIG.latenessBounds().get(0));
        Cycle supersededCycle = new Cycle(1L, T0, dueAt.plusSeconds(1));   // between due and sendBy
        CartScript supersededScript = new CartScript("cart-superseded", "shopper-superseded", List.of(supersededCycle));
        assert !supersededCycle.cancelledAt().isAfter(sendBy);

        Cycle missedCycle = new Cycle(1L, T0, null);
        CartScript missedScript = new CartScript("cart-missed", "shopper-missed", List.of(missedCycle));

        Cycle lateCycle = new Cycle(1L, T0, sendBy.plusSeconds(1));      // resumed only after offset 0's sendBy
        CartScript lateScript = new CartScript("cart-late", "shopper-late", List.of(lateCycle));

        List<CartScript> scripts = List.of(sentScript, supersededScript, missedScript, lateScript);
        Set<String> expectedKeys = Expected.keys(scripts, CONFIG, ALL_TREATMENT);

        List<OutcomeRow> outcomes = List.of(
            new OutcomeRow("cart-sent:1:0", "cart-sent", 1, "TREATMENT", "SENT", T0, 1),
            new OutcomeRow("cart-sent:1:1", "cart-sent", 1, "TREATMENT", "SENT", T0, 1),
            new OutcomeRow("cart-sent:1:2", "cart-sent", 1, "TREATMENT", "SENT", T0, 1),
            new OutcomeRow("some-other-cart:1:0", "some-other-cart", 1, "TREATMENT", "SENT", T0, 1));   // non-expected

        CorrectnessSummary.Result result = CorrectnessSummary.compute(expectedKeys, outcomes, scripts, CONFIG, ALL_TREATMENT);

        assertEquals(3, result.sent());
        assertEquals(1, result.outcomesOnNonExpectedKeys());
        assertEquals(1, result.supersededBeforeSend());       // cart-superseded's single expected key (offset 0)
        assertEquals(1, result.supersededAfterSendBy());      // cart-late's offset 0, resumed after its sendBy
        assertEquals(3, result.neverSupersededNoOutcome());   // cart-missed's three offsets
        assertIdentity(expectedKeys.size(), result);
    }

    private static void assertIdentity(long expected, CorrectnessSummary.Result r) {
        assertEquals(expected,
            r.sent() + r.skippedLate() + r.cancelled() + r.dead() + r.supersededBeforeSend() + r.unexplainedMissing(),
            "expected = sent + skipped + cancelled + dead + superseded-before-send + unexplained");
        assertEquals(r.unexplainedMissing(), r.supersededAfterSendBy() + r.neverSupersededNoOutcome(),
            "unexplained = superseded-after-sendBy + never-superseded-no-outcome");
    }
}
