package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.OutcomeKind;
import java.time.Instant;
import java.util.List;

/**
 * One row per idempotency key with a fenced lease. Statuses: SENDING (leaseToken, leaseUntil), RETRYING,
 * and the final SENT, SKIPPED_LATE, CANCELLED, DEAD. Every non-final row is in the retry index with
 * nextAttemptAt (for SENDING equal to leaseUntil). Production: DynamoDB "send-ledger".
 */
public interface SendLedger {
    /**
     * Creates the row as SENDING with attempts 1 if absent; or takes it over, incrementing attempts, if
     * RETRYING with nextAttemptAt <= now or SENDING with leaseUntil <= now. Every claim gets a fresh token
     * and leaseUntil = now + lease. Otherwise NotClaimed("final") or NotClaimed("leased").
     */
    ClaimResult claim(String key, Instant sendBy, int srcPartition, Instant now);

    /** SENDING with this token → RETRYING due at nextAt. False if the lease was lost. */
    boolean markRetry(String key, String token, Instant nextAt);

    /** SENDING with this token → the final outcome (SENT, SKIPPED_LATE, CANCELLED, DEAD). False if the lease was lost. */
    boolean finish(String key, String token, OutcomeKind outcome, String reason);

    /** Non-final rows of this shard with nextAttemptAt <= now, earliest first. */
    List<DueRetry> dueRetries(int shard, Instant now, int limit);

    /** DEAD → RETRYING due now with attempts reset to 0. False for any other status. */
    boolean reopen(String key, Instant now);

    /** Highest offsetIndex with a row for this cart and version, any status; -1 if none. */
    int highestOffsetIndex(String cartId, long version);
}
