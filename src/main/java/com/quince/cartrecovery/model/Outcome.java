package com.quince.cartrecovery.model;

import java.time.Instant;

/**
 * One accounting record. key is null for ABANDONED. Reminder outcomes always carry Arm.TREATMENT,
 * because only eligible (never holdout) carts get reminders.
 */
public record Outcome(String key, String cartId, long version, Arm arm, OutcomeKind kind, Instant at, int attempts) {}
