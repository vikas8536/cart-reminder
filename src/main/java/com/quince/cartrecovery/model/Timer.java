package com.quince.cartrecovery.model;

import java.time.Instant;

/** One pending timer per cart. offsetIndex is -1 for CHECK_ABANDON. */
public record Timer(String cartId, TimerKind kind, long version, int offsetIndex, Instant dueAt) {

    public static Timer checkAbandon(String cartId, long version, Instant dueAt) {
        return new Timer(cartId, TimerKind.CHECK_ABANDON, version, -1, dueAt);
    }

    public static Timer reminder(String cartId, long version, int offsetIndex, Instant dueAt) {
        return new Timer(cartId, TimerKind.REMINDER, version, offsetIndex, dueAt);
    }
}
