package com.quince.cartrecovery.model;

import java.time.Instant;

/** What the scheduler hands the dispatcher. sendBy = scheduledFor + the offset's lateness bound. */
public record ReminderIntent(String key, String cartId, long version, int offsetIndex, int srcPartition,
                             Instant scheduledFor, Instant sendBy) {}
