package com.quince.cartrecovery.model;

import java.time.Instant;
import java.util.List;

public record NotificationIntent(String idempotencyKey, String cartId, String shopperKey, long version,
                                 int offsetIndex, Instant scheduledFor, List<CartItem> items) {

    public static String key(String cartId, long version, int offsetIndex) {
        return cartId + ":" + version + ":" + offsetIndex;
    }
}
