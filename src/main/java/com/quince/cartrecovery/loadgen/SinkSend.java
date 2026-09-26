package com.quince.cartrecovery.loadgen;

import java.time.Instant;

/** Mirrors one {@code sink-sends} record: one row per {@code NotificationSink.send()} call. */
public record SinkSend(String key, String cartId, Instant at, boolean hasFirstName, int itemCount) {}
