package com.quince.cartrecovery.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** One record per cart. sequenceStarts holds the last-activity time of each reminder sequence started. */
public record CartRecord(String cartId, String shopperKey, CartStatus status, long version,
                         Instant lastActivityAt, List<CartItem> items, Arm arm,
                         List<Instant> sequenceStarts) {

    public CartRecord {
        items = List.copyOf(items);
        sequenceStarts = List.copyOf(sequenceStarts);
    }

    public static CartRecord fresh(String cartId, String shopperKey, Arm arm) {
        return new CartRecord(cartId, shopperKey, CartStatus.ACTIVE, 0L, Instant.EPOCH, List.of(), arm, List.of());
    }

    public CartRecord activity(long newVersion, Instant at, List<CartItem> newItems) {
        return new CartRecord(cartId, shopperKey, CartStatus.ACTIVE, newVersion, at, newItems, arm, sequenceStarts);
    }

    public CartRecord closed(long newVersion, Instant at) {
        return new CartRecord(cartId, shopperKey, CartStatus.CLOSED, newVersion, at, items, arm, sequenceStarts);
    }

    public CartRecord abandoned() {
        List<Instant> starts = new ArrayList<>(sequenceStarts);
        starts.add(lastActivityAt);
        return new CartRecord(cartId, shopperKey, CartStatus.ABANDONED, version, lastActivityAt, items, arm, starts);
    }
}
