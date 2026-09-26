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

    /** firstName is optional (null when the shopper is anonymous or the name is unknown). */
    record CartEdited(String cartId, String shopperKey, long version, Instant occurredAt,
                      List<CartItem> items, String firstName) implements CartEvent {
        public CartEdited(String cartId, String shopperKey, long version, Instant occurredAt, List<CartItem> items) {
            this(cartId, shopperKey, version, occurredAt, items, null);
        }
    }

    record CartResumed(String cartId, String shopperKey, long version, Instant occurredAt)
            implements CartEvent {}

    record CartCleared(String cartId, String shopperKey, long version, Instant occurredAt)
            implements CartEvent {}

    record CartPurchased(String cartId, String shopperKey, long version, Instant occurredAt)
            implements CartEvent {}
}
