package com.quince.cartrecovery.loadgen;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AccountingTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @Test void eachKeyResolvesByPrecedenceSentBeatsEverythingElse() {
        List<OutcomeRow> rows = List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SKIPPED_LATE", T0, 1),
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SENT", T0.plusSeconds(1), 1));

        Map<String, String> resolved = Accounting.resolveOutcomes(rows);

        assertEquals("SENT", resolved.get("k1"));
    }

    @Test void deadBeatsCancelledBeatsSkippedLate() {
        assertEquals("DEAD", Accounting.resolveOutcomes(List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "CANCELLED", T0, 1),
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "DEAD", T0, 1))).get("k1"));
        assertEquals("CANCELLED", Accounting.resolveOutcomes(List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SKIPPED_LATE", T0, 1),
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "CANCELLED", T0, 1))).get("k1"));
    }

    @Test void abandonedRowsWithNoKeyAreDropped() {
        List<OutcomeRow> rows = List.of(new OutcomeRow(null, "cart-1", 1, "TREATMENT", "ABANDONED", T0, 0));
        assertEquals(Map.of(), Accounting.resolveOutcomes(rows));
    }

    @Test void countByKindCountsResolvedKinds() {
        Map<String, String> resolved = Map.of("k1", "SENT", "k2", "SENT", "k3", "DEAD");
        assertEquals(2, Accounting.countByKind(resolved, "SENT"));
        assertEquals(1, Accounting.countByKind(resolved, "DEAD"));
        assertEquals(0, Accounting.countByKind(resolved, "CANCELLED"));
    }

    @Test void duplicateSendsCountsSendsBeyondTheFirstPerKey() {
        List<SinkSend> sends = List.of(
            new SinkSend("k1", "cart-1", T0, true, 1),
            new SinkSend("k1", "cart-1", T0.plusSeconds(1), true, 1),
            new SinkSend("k2", "cart-2", T0, true, 1));

        assertEquals(1, Accounting.duplicateSends(sends));
    }

    @Test void noDuplicatesWhenEveryKeySendsOnce() {
        List<SinkSend> sends = List.of(
            new SinkSend("k1", "cart-1", T0, true, 1),
            new SinkSend("k2", "cart-2", T0, true, 1));

        assertEquals(0, Accounting.duplicateSends(sends));
    }

    @Test void postPurchaseSendsCountsOnlySendsWellAfterThePurchase() {
        Duration clockSkew = Duration.ofSeconds(5);
        Instant purchaseAt = T0;
        Map<String, Instant> purchaseAtByCart = Map.of("cart-1", purchaseAt);
        List<SinkSend> sends = List.of(
            new SinkSend("k1", "cart-1", purchaseAt.plusSeconds(3), true, 1),   // within clockSkew + 1s: not counted
            new SinkSend("k2", "cart-1", purchaseAt.plusSeconds(10), true, 1),  // well after: counted
            new SinkSend("k3", "cart-2", purchaseAt.plusSeconds(100), true, 1)); // no purchase on record: not counted

        assertEquals(1, Accounting.postPurchaseSends(sends, purchaseAtByCart, clockSkew));
    }

    @Test void unexplainedMissingIsExpectedMinusEveryAccountedOutcome() {
        assertEquals(2, Accounting.unexplainedMissing(100, 80, 10, 5, 3, 0));
    }

    // Fix round 1, finding 1: a key superseded before its own sendBy is a correct non-send, not a miss.
    @Test void unexplainedMissingAlsoExcludesKeysSupersededBeforeSend() {
        assertEquals(1, Accounting.unexplainedMissing(100, 80, 10, 5, 3, 1));
    }

    @Test void unexplainedMissingRatioGuardsAgainstZeroExpected() {
        assertEquals(0.0, Accounting.unexplainedMissingRatio(0, 0));
        assertEquals(0.02, Accounting.unexplainedMissingRatio(2, 100));
    }

    @Test void supersededRanksBelowEveryOtherKind() {
        assertEquals("SKIPPED_LATE", Accounting.resolveOutcomes(List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SUPERSEDED", T0, 0),
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SKIPPED_LATE", T0, 1))).get("k1"));
        assertEquals("CANCELLED", Accounting.resolveOutcomes(List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "CANCELLED", T0, 1),
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SUPERSEDED", T0, 0))).get("k1"));
        assertEquals("SUPERSEDED", Accounting.resolveOutcomes(List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SUPERSEDED", T0, 0))).get("k1"));
    }

    @Test void supersededAtIsTheEarliestSupersededTimePerKey() {
        Map<String, Instant> at = Accounting.supersededAt(List.of(
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SUPERSEDED", T0.plusSeconds(5), 0),
            new OutcomeRow("k1", "cart-1", 1, "TREATMENT", "SUPERSEDED", T0, 0),
            new OutcomeRow("k2", "cart-2", 1, "TREATMENT", "SENT", T0, 1)));
        assertEquals(Map.of("k1", T0), at);
    }
}
