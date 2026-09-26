package com.quince.cartrecovery;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

/** Polls a condition every 50 ms until it is true, or throws after the timeout elapses. */
public final class Await {
    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    private Await() {}

    public static void until(BooleanSupplier condition, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (!condition.getAsBoolean()) {
            if (Instant.now().isAfter(deadline)) {
                throw new AssertionError("condition not met within " + timeout);
            }
            try {
                Thread.sleep(POLL_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for condition", e);
            }
        }
    }
}
