package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.SendLedger;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Same state machine as the DynamoDB send-ledger: fenced leases, tokens, and a retry index of non-final rows. */
public final class InMemorySendLedger implements SendLedger {
    private static final String SENDING = "SENDING";
    private static final String RETRYING = "RETRYING";

    private record Row(String status, String token, Instant leaseUntil, Instant sendBy, int srcPartition,
                       int attempts, Instant nextAttemptAt, String reason) {
        boolean isFinal() { return !status.equals(SENDING) && !status.equals(RETRYING); }
    }

    private final Duration lease;
    private final int shards;
    private final Map<String, Row> rows = new HashMap<>();

    public InMemorySendLedger(Duration lease, int shards) {
        this.lease = lease;
        this.shards = shards;
    }

    @Override public synchronized ClaimResult claim(String key, Instant sendBy, int srcPartition, Instant now) {
        Row row = rows.get(key);
        if (row != null && row.isFinal()) return new ClaimResult.NotClaimed("final");
        boolean takeover = row != null && (
            (row.status().equals(RETRYING) && !row.nextAttemptAt().isAfter(now))
            || (row.status().equals(SENDING) && !row.leaseUntil().isAfter(now)));
        if (row != null && !takeover) return new ClaimResult.NotClaimed("leased");
        Instant until = now.plus(lease);
        String token = UUID.randomUUID().toString();
        Row claimed = row == null
            ? new Row(SENDING, token, until, sendBy, srcPartition, 1, until, null)
            : new Row(SENDING, token, until, row.sendBy(), row.srcPartition(), row.attempts() + 1, until, row.reason());
        rows.put(key, claimed);
        return new ClaimResult.Claimed(token, claimed.attempts(), claimed.sendBy(), claimed.srcPartition(), until);
    }

    @Override public synchronized boolean markRetry(String key, String token, Instant nextAt) {
        Row row = held(key, token);
        if (row == null) return false;
        rows.put(key, new Row(RETRYING, null, null, row.sendBy(), row.srcPartition(), row.attempts(), nextAt, row.reason()));
        return true;
    }

    @Override public synchronized boolean finish(String key, String token, OutcomeKind outcome, String reason) {
        if (outcome == OutcomeKind.ABANDONED) throw new IllegalArgumentException("ABANDONED is not a ledger outcome");
        Row row = held(key, token);
        if (row == null) return false;
        rows.put(key, new Row(outcome.name(), null, null, row.sendBy(), row.srcPartition(), row.attempts(), null, reason));
        return true;
    }

    @Override public synchronized List<DueRetry> dueRetries(int shard, Instant now, int limit) {
        return rows.entrySet().stream()
            .filter(e -> !e.getValue().isFinal()
                && !e.getValue().nextAttemptAt().isAfter(now)
                && Shards.of(LedgerKey.parse(e.getKey()).cartId(), shards) == shard)
            .sorted(Comparator.comparing((Map.Entry<String, Row> e) -> e.getValue().nextAttemptAt())
                .thenComparing(Map.Entry::getKey))
            .limit(limit)
            .map(e -> new DueRetry(e.getKey(), e.getValue().srcPartition()))
            .toList();
    }

    @Override public synchronized boolean reopen(String key, Instant now) {
        Row row = rows.get(key);
        if (row == null || !row.status().equals(OutcomeKind.DEAD.name())) return false;
        rows.put(key, new Row(RETRYING, null, null, row.sendBy(), row.srcPartition(), 0, now, null));
        return true;
    }

    @Override public synchronized int highestOffsetIndex(String cartId, long version) {
        int highest = -1;
        for (String key : rows.keySet()) {
            LedgerKey k = LedgerKey.parse(key);
            if (k.cartId().equals(cartId) && k.version() == version) highest = Math.max(highest, k.offsetIndex());
        }
        return highest;
    }

    /** Earliest nextAttemptAt over non-final rows (a SENDING row's is its lease expiry). */
    public synchronized Optional<Instant> nextRetryAt() {
        return rows.values().stream().filter(r -> !r.isFinal()).map(Row::nextAttemptAt).min(Instant::compareTo);
    }

    public synchronized int size() { return rows.size(); }

    /** The row's status name (SENDING, RETRYING, SENT, SKIPPED_LATE, CANCELLED, DEAD), empty if absent. */
    public synchronized Optional<String> status(String key) {
        return Optional.ofNullable(rows.get(key)).map(Row::status);
    }

    private Row held(String key, String token) {
        Row row = rows.get(key);
        return row != null && row.status().equals(SENDING) && row.token().equals(token) ? row : null;
    }
}
