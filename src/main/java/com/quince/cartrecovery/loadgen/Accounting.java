package com.quince.cartrecovery.loadgen;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolves observed {@code sink-sends} and {@code reminder-outcomes} rows into the correctness
 * counts the load test reports. Every method is a pure function over the rows passed in; the
 * caller (the loadgen role) is responsible for reading those rows from Kafka, filtered to its run.
 */
public final class Accounting {
    private static final List<String> PRECEDENCE = List.of("SENT", "DEAD", "CANCELLED", "SKIPPED_LATE", "SUPERSEDED");

    private Accounting() {}

    /** Each key resolves to exactly one kind, by precedence SENT > DEAD > CANCELLED > SKIPPED_LATE > SUPERSEDED. Rows with a null key ({@code ABANDONED}) are dropped. */
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

    /**
     * Fix round 2: restricts a resolved-outcomes map to only the keys in {@code keys} (typically
     * {@code Expected.keys(...)}). An outcome on a key that isn't expected — a real-vs-nominal timing
     * edge at a cycle boundary, say — must never be counted alongside sent/skippedLate/cancelled/dead,
     * or it silently cancels out a real miss elsewhere in {@link #unexplainedMissing}'s subtraction
     * without ever being visible in the report.
     */
    public static Map<String, String> restrictToKeys(Map<String, String> resolved, Set<String> keys) {
        Map<String, String> restricted = new HashMap<>();
        for (Map.Entry<String, String> e : resolved.entrySet()) {
            if (keys.contains(e.getKey())) restricted.put(e.getKey(), e.getValue());
        }
        return restricted;
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
     * expected - sent - skippedLate - cancelled - dead - supersededBeforeSend. A deliberate deviation from spec §8.5's
     * plain formula: a key resolved SUPERSEDED at or before its own sendBy is a correct non-send (the cart moved on
     * first), so it is excluded. A key superseded only after its sendBy, or with no outcome at all, stays inside.
     */
    public static long unexplainedMissing(long expected, long sent, long skippedLate, long cancelled, long dead,
                                           long supersededBeforeSend) {
        return expected - sent - skippedLate - cancelled - dead - supersededBeforeSend;
    }

    public static double unexplainedMissingRatio(long unexplainedMissing, long expected) {
        return expected == 0 ? 0.0 : unexplainedMissing / (double) expected;
    }

    /** Earliest SUPERSEDED time per key (review fix 2: the detector records one per displaced, owed reminder). */
    public static Map<String, Instant> supersededAt(List<OutcomeRow> outcomes) {
        Map<String, Instant> at = new HashMap<>();
        for (OutcomeRow row : outcomes) {
            if (row.key() != null && "SUPERSEDED".equals(row.kind())) {
                at.merge(row.key(), row.at(), (a, b) -> a.isBefore(b) ? a : b);
            }
        }
        return at;
    }
}
