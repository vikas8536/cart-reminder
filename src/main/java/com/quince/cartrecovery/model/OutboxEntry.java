package com.quince.cartrecovery.model;

import java.time.Instant;

public record OutboxEntry(NotificationIntent intent, int attempts, Instant nextAttemptAt) {

    public String key() { return intent.idempotencyKey(); }

    public OutboxEntry retryAt(Instant at) {
        return new OutboxEntry(intent, attempts + 1, at);
    }
}
