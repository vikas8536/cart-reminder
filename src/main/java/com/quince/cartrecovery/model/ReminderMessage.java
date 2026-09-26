package com.quince.cartrecovery.model;

import java.util.List;

/** What the gateway receives, built at send time from a consistent cart read. firstName may be null. */
public record ReminderMessage(String key, String cartId, String shopperKey, String firstName, List<CartItem> items) {
    public ReminderMessage {
        items = List.copyOf(items);
    }
}
