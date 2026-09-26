package com.quince.cartrecovery.model;

import java.time.Instant;

public record DeadLetter(ReminderIntent intent, String reason, Instant at) {
    /** Reason for an intent that could not be deserialized; replay skips it. */
    public static final String REASON_POISON = "poison";
}
