package com.quince.cartrecovery.core;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.RecoveryConfig;
import java.time.Instant;

/** Decides whether an abandoned cart may receive a reminder sequence. Shared by scheduler and reconciler. */
public final class ReminderPolicy {
    private final RecoveryConfig config;

    public ReminderPolicy(RecoveryConfig config) { this.config = config; }

    /** Holdout carts never get reminders. Others are capped on sequences started within the frequency window. */
    public boolean eligible(CartRecord record, Instant now) {
        if (record.arm() == Arm.HOLDOUT) return false;
        Instant windowStart = now.minus(config.frequencyWindow());
        long recent = record.sequenceStarts().stream().filter(s -> !s.isBefore(windowStart)).count();
        return recent <= config.frequencyCap();
    }
}
