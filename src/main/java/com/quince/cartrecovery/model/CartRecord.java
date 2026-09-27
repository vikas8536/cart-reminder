package com.quince.cartrecovery.model;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * One record per cart. sequenceStarts holds the last-activity time of each reminder sequence started.
 * firstName is optional (null). srcPartition is the cart-events partition the detector consumed the
 * cart's latest event from, or -1 when unknown (records written before the field existed).
 */
public record CartRecord(String cartId, String shopperKey, CartStatus status, long version,
                         Instant lastActivityAt, List<CartItem> items, Arm arm,
                         List<Instant> sequenceStarts, String firstName, int srcPartition) {

    public static final int MAX_ITEMS = 50;

    public CartRecord {
        items = List.copyOf(items);
        sequenceStarts = List.copyOf(sequenceStarts);
    }

    public CartRecord(String cartId, String shopperKey, CartStatus status, long version,
                      Instant lastActivityAt, List<CartItem> items, Arm arm, List<Instant> sequenceStarts) {
        this(cartId, shopperKey, status, version, lastActivityAt, items, arm, sequenceStarts, null, -1);
    }

    public static CartRecord fresh(String cartId, String shopperKey, Arm arm) {
        return new CartRecord(cartId, shopperKey, CartStatus.ACTIVE, 0L, Instant.EPOCH, List.of(), arm, List.of());
    }

    /** Items are capped at MAX_ITEMS. */
    public CartRecord activity(long newVersion, Instant at, List<CartItem> newItems) {
        List<CartItem> capped = newItems.size() > MAX_ITEMS ? newItems.subList(0, MAX_ITEMS) : newItems;
        return new CartRecord(cartId, shopperKey, CartStatus.ACTIVE, newVersion, at, capped, arm, sequenceStarts,
            firstName, srcPartition);
    }

    public CartRecord closed(long newVersion, Instant at) {
        return new CartRecord(cartId, shopperKey, CartStatus.CLOSED, newVersion, at, items, arm, sequenceStarts,
            firstName, srcPartition);
    }

    public CartRecord abandoned() {
        List<Instant> starts = new ArrayList<>(sequenceStarts);
        starts.add(lastActivityAt);
        return new CartRecord(cartId, shopperKey, CartStatus.ABANDONED, version, lastActivityAt, items, arm, starts,
            firstName, srcPartition);
    }

    /** sequenceStarts pruned to [now - frequencyWindow, now], plus start. */
    public List<Instant> startsWith(Instant start, Instant now, Duration frequencyWindow) {
        Instant from = now.minus(frequencyWindow);
        List<Instant> kept = new ArrayList<>();
        for (Instant s : sequenceStarts) {
            if (!s.isBefore(from) && !s.isAfter(now)) kept.add(s);
        }
        kept.add(start);
        return List.copyOf(kept);
    }
}
