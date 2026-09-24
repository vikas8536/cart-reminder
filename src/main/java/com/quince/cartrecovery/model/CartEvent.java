package com.quince.cartrecovery.model;

import java.time.Instant;
import java.util.List;

/** Events published by the Cart Service. Version is per cart and strictly increasing. */
public sealed interface CartEvent permits CartEvent.CartEdited, CartEvent.CartResumed,
        CartEvent.CartCleared, CartEvent.CartPurchased {

    String cartId();
    String shopperKey();
    long version();
    Instant occurredAt();

    record CartEdited(String cartId, String shopperKey, long version, Instant occurredAt,
                      List<CartItem> items) implements CartEvent {}

    record CartResumed(String cartId, String shopperKey, long version, Instant occurredAt)
            implements CartEvent {}

    record CartCleared(String cartId, String shopperKey, long version, Instant occurredAt)
            implements CartEvent {}

    record CartPurchased(String cartId, String shopperKey, long version, Instant occurredAt)
            implements CartEvent {}
}
