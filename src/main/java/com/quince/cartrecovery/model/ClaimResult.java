package com.quince.cartrecovery.model;

import java.time.Instant;

public sealed interface ClaimResult {
    /** sendBy and srcPartition are the row's stored values, which a takeover never changes. */
    record Claimed(String token, int attempts, Instant sendBy, int srcPartition, Instant leaseUntil)
            implements ClaimResult {}

    /** reason is "final" (the row reached a final status) or "leased" (held, or a retry not yet due). */
    record NotClaimed(String reason) implements ClaimResult {}
}
