package com.quince.cartrecovery.loadgen;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves observed {@code sink-sends} and {@code reminder-outcomes} rows into the correctness
 * counts the load test reports. Every method is a pure function over the rows passed in; the
 * caller (the loadgen role) is responsible for reading those rows from Kafka, filtered to its run.
 */
public final class Accounting {
    private static final List<String> PRECEDENCE = List.of("SENT", "DEAD", "CANCELLED", "SKIPPED_LATE");

    private Accounting() {}

    /** Each key resolves to exactly one kind, by precedence SENT > DEAD > CANCELLED > SKIPPED_LATE. Rows with a null key ({@code ABANDONED}) are dropped. */
    public static Map<String, String> resolveOutcomes(List<OutcomeRow> outcomes) {
        Map<String, String> best = new HashMap<>();
        for (OutcomeRow row : outcomes) {
            if (row.key() == null) continue;
            String current = best.get(row.key());
            if (current == null || precedenceRank(row.kind()) < precedenceRank(current)) {
                best.put(row.key(), row.kind());
            }
        }
        return best;
    }

    private static int precedenceRank(String kind) {
        int i = PRECEDENCE.indexOf(kind);
        return i < 0 ? PRECEDENCE.size() : i;
    }

    public static long countByKind(Map<String, String> resolved, String kind) {
        return resolved.values().stream().filter(kind::equals).count();
    }

    /** Sends beyond the first per key: a healthy pipeline has zero. */
    public static long duplicateSends(List<SinkSend> sends) {
        Map<String, Long> perKey = new HashMap<>();
        for (SinkSend send : sends) {
            perKey.merge(send.key(), 1L, Long::sum);
        }
        return perKey.values().stream().mapToLong(count -> Math.max(0, count - 1)).sum();
    }

    /** A send whose cart had a purchase more than {@code clockSkew + 1s} before the send: a healthy pipeline has zero. */
    public static long postPurchaseSends(List<SinkSend> sends, Map<String, Instant> purchaseAtByCart, Duration clockSkew) {
        Duration threshold = clockSkew.plusSeconds(1);
        long count = 0;
        for (SinkSend send : sends) {
            Instant purchaseAt = purchaseAtByCart.get(send.cartId());
            if (purchaseAt == null) continue;
            if (Duration.between(purchaseAt, send.at()).compareTo(threshold) > 0) count++;
        }
        return count;
    }

    /**
     * expected - sent - skippedLate - cancelled - dead - supersededBeforeSend. A deliberate deviation
     * from spec §8.5's plain formula (controller ruling, E2 fix round 1): a key superseded by a resume
     * or purchase at or before its own sendBy is a correct non-send with no outcome ever recorded for
     * it (see {@link MissingBreakdown}), so it is excluded here rather than counted as a failure. A key
     * superseded only after its sendBy stays inside this total as "missed while lagging" — it should
     * have gone out, or been explicitly resolved, before the cart moved on, and wasn't.
     */
    public static long unexplainedMissing(long expected, long sent, long skippedLate, long cancelled, long dead,
                                           long supersededBeforeSend) {
        return expected - sent - skippedLate - cancelled - dead - supersededBeforeSend;
    }

    public static double unexplainedMissingRatio(long unexplainedMissing, long expected) {
        return expected == 0 ? 0.0 : unexplainedMissing / (double) expected;
    }
}
