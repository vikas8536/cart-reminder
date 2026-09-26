package com.quince.cartrecovery.model;

public enum Lane {
    FAST, SLOW;

    /** Early offsets (offsetIndex < fastOffsets) go to the fast lane. */
    public static Lane of(int offsetIndex, int fastOffsets) {
        return offsetIndex < fastOffsets ? FAST : SLOW;
    }
}
