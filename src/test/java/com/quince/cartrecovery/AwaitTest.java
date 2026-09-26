package com.quince.cartrecovery;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitTest {
    @Test void returnsAsSoonAsConditionIsTrue() {
        AtomicInteger calls = new AtomicInteger();
        Await.until(() -> calls.incrementAndGet() >= 3, Duration.ofSeconds(1));
        assertTrue(calls.get() >= 3);
    }

    @Test void throwsAssertionErrorOnTimeout() {
        AssertionError e = assertThrows(AssertionError.class,
            () -> Await.until(() -> false, Duration.ofMillis(120)));
        assertTrue(e.getMessage().contains("condition not met"));
    }
}
