package com.quince.cartrecovery.model;

import java.time.Duration;

/** What the caller does with a claimed timer after the scheduler handled it. */
public sealed interface TimerDecision {
    record Ack() implements TimerDecision {}
    record Release(Duration delay) implements TimerDecision {}
}
