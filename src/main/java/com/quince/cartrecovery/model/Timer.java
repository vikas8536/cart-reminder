package com.quince.cartrecovery.model;

import java.time.Instant;

/** One pending timer per cart. offsetIndex is -1 for CHECK_ABANDON. srcPartition is -1 when unknown. */
public record Timer(String cartId, TimerKind kind, long version, int offsetIndex, Instant dueAt, int srcPartition) {

    public Timer(String cartId, TimerKind kind, long version, int offsetIndex, Instant dueAt) {
        this(cartId, kind, version, offsetIndex, dueAt, -1);
    }

    public static Timer checkAbandon(String cartId, long version, Instant dueAt, int srcPartition) {
        return new Timer(cartId, TimerKind.CHECK_ABANDON, version, -1, dueAt, srcPartition);
    }

    public static Timer reminder(String cartId, long version, int offsetIndex, Instant dueAt, int srcPartition) {
        return new Timer(cartId, TimerKind.REMINDER, version, offsetIndex, dueAt, srcPartition);
    }

    public static Timer checkAbandon(String cartId, long version, Instant dueAt) {
        return checkAbandon(cartId, version, dueAt, -1);
    }

    public static Timer reminder(String cartId, long version, int offsetIndex, Instant dueAt) {
        return reminder(cartId, version, offsetIndex, dueAt, -1);
    }
}
