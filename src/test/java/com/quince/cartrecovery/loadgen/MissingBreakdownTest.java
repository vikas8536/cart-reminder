package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Fix round 1, finding 1: an expected key with no recorded outcome isn't necessarily a miss — a resume
// or purchase can correctly supersede (and so cancel, with no outcome ever recorded) a still-pending
// timer. These cases pin the boundary between "superseded before send" (a correct non-send) and "missed
// while lagging" (a real failure), using offset 0 of the defaults (offset 30 min, lateness bound 5 min,
// so sendBy = lastActivityAt + 35 min).
class MissingBreakdownTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final RecoveryConfig CONFIG = RecoveryConfig.defaults();
    private static final ArmAssigner ALL_TREATMENT = shopperKey -> Arm.TREATMENT;

    @Test void resumeBeforeTheDueTimeIsNotExpectedAtAllSoNeitherBucketCountsIt() {
        Instant resumeAt = T0.plus(CONFIG.offsets().get(0)).minusSeconds(1);   // before dueAt(0)
        Cycle cycle = new Cycle(1L, T0, resumeAt);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        MissingBreakdown.Result result = MissingBreakdown.compute(List.of(script), CONFIG, ALL_TREATMENT, Set.of());

        assertEquals(new MissingBreakdown.Result(0, 0, 0), result);
    }

    @Test void resumeBetweenDueAndSendByIsSupersededBeforeSend() {
        Instant dueAt = T0.plus(CONFIG.offsets().get(0));
        Instant sendBy = dueAt.plus(CONFIG.latenessBounds().get(0));
        Instant resumeAt = dueAt.plusSeconds(1);   // after due, still at/before sendBy
        Cycle cycle = new Cycle(1L, T0, resumeAt);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));
        assert !resumeAt.isAfter(sendBy);

        MissingBreakdown.Result result = MissingBreakdown.compute(List.of(script), CONFIG, ALL_TREATMENT, Set.of());

        assertEquals(new MissingBreakdown.Result(1, 0, 0), result);
    }

    // Final review: "missed while lagging" is split into pure lateness and possible silent loss.
    @Test void resumeAfterSendByIsSupersededAfterSendBy() {
        Instant dueAt = T0.plus(CONFIG.offsets().get(0));
        Instant sendBy = dueAt.plus(CONFIG.latenessBounds().get(0));
        Instant resumeAt = sendBy.plusSeconds(1);   // after the pre-check's own deadline
        Cycle cycle = new Cycle(1L, T0, resumeAt);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        MissingBreakdown.Result result = MissingBreakdown.compute(List.of(script), CONFIG, ALL_TREATMENT, Set.of());

        assertEquals(new MissingBreakdown.Result(0, 1, 0), result);   // pure lateness
    }

    @Test void aKeyWithNoSupersedingEventThatNeverGotAnOutcomeIsNeverSupersededNoOutcome() {
        Cycle cycle = new Cycle(1L, T0, null);   // never resumes or purchases
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        MissingBreakdown.Result result = MissingBreakdown.compute(List.of(script), CONFIG, ALL_TREATMENT, Set.of());

        assertEquals(new MissingBreakdown.Result(0, 0, 3), result);   // possible silent loss: all three offsets, none superseded
    }

    @Test void aKeyThatDidGetAnOutcomeIsNotCountedInEitherBucket() {
        Instant resumeAt = T0.plus(CONFIG.offsets().get(0)).plusSeconds(1);   // would otherwise be superseded-before-send
        Cycle cycle = new Cycle(1L, T0, resumeAt);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        MissingBreakdown.Result result = MissingBreakdown.compute(List.of(script), CONFIG, ALL_TREATMENT, Set.of("cart-1:1:0"));

        assertEquals(new MissingBreakdown.Result(0, 0, 0), result);
    }

    @Test void holdoutCartsAreNeverCountedInEitherBucket() {
        Cycle cycle = new Cycle(1L, T0, null);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));
        ArmAssigner allHoldout = shopperKey -> Arm.HOLDOUT;

        MissingBreakdown.Result result = MissingBreakdown.compute(List.of(script), CONFIG, allHoldout, Set.of());

        assertEquals(new MissingBreakdown.Result(0, 0, 0), result);
    }
}
