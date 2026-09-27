package com.quince.cartrecovery.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.ClaimResult.Claimed;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.SendLedger;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour every SendLedger must share. Time is an explicit argument of this port, so boundaries are tested exactly
 * and no time hook is needed. Keys carry a per-test prefix, so a subclass may share one table across tests.
 */
public abstract class SendLedgerContract {
    protected static final Duration LEASE = Duration.ofSeconds(90);
    protected static final int SHARDS = 4;
    protected static final Instant NOW = Instant.parse("2026-01-01T09:30:00Z");
    protected static final Instant SEND_BY = NOW.plus(Duration.ofMinutes(5));

    protected final String prefix = "c" + UUID.randomUUID().toString().substring(0, 8) + "-";
    protected SendLedger ledger;

    /** A ledger holding none of this test's keys. */
    protected abstract SendLedger newLedger(Duration lease, int shards);

    @BeforeEach
    void createLedger() {
        ledger = newLedger(LEASE, SHARDS);
    }

    protected String key(String cart, long version, int offset) {
        return new LedgerKey(prefix + cart, version, offset).toString();
    }

    private Claimed claimed(String key, Instant now) {
        return assertInstanceOf(Claimed.class, ledger.claim(key, SEND_BY, 3, now));
    }

    private List<DueRetry> due(String key, Instant now) {
        int shard = Shards.of(LedgerKey.parse(key).cartId(), SHARDS);
        return ledger.dueRetries(shard, now, 100).stream().filter(d -> d.key().startsWith(prefix)).toList();
    }

    @Test
    void theFirstClaimCreatesASendingRowWithOneAttempt() {
        Claimed c = claimed(key("a", 1, 0), NOW);

        assertEquals(1, c.attempts());
        assertEquals(SEND_BY, c.sendBy());
        assertEquals(3, c.srcPartition());
        assertEquals(NOW.plus(LEASE), c.leaseUntil());
    }

    @Test
    void aHeldLeaseIsNotClaimedAgainUntilExactlyLeaseUntil() {
        String k = key("a", 1, 0);
        Claimed first = claimed(k, NOW);

        assertEquals(new ClaimResult.NotClaimed("leased"), ledger.claim(k, SEND_BY, 3, NOW));
        assertEquals(new ClaimResult.NotClaimed("leased"), ledger.claim(k, SEND_BY, 3, first.leaseUntil().minusMillis(1)));

        Claimed takeover = claimed(k, first.leaseUntil());
        assertEquals(2, takeover.attempts());
        assertNotEquals(first.token(), takeover.token());
        assertEquals(first.leaseUntil().plus(LEASE), takeover.leaseUntil());
    }

    @Test
    void aTakeoverKeepsTheStoredSendByAndPartition() {
        String k = key("a", 1, 0);
        Claimed first = claimed(k, NOW);

        ClaimResult again = ledger.claim(k, SEND_BY.plusSeconds(999), 7, first.leaseUntil());

        Claimed c = assertInstanceOf(Claimed.class, again);
        assertEquals(SEND_BY, c.sendBy());
        assertEquals(3, c.srcPartition());
    }

    @Test
    void aStaleTokenCanNeitherFinishNorMarkRetry() {
        String k = key("a", 1, 0);
        Claimed stale = claimed(k, NOW);
        Claimed current = claimed(k, stale.leaseUntil());

        assertFalse(ledger.finish(k, stale.token(), OutcomeKind.SENT, null));
        assertFalse(ledger.markRetry(k, stale.token(), NOW.plusSeconds(60)));
        assertTrue(ledger.finish(k, current.token(), OutcomeKind.SENT, null));
    }

    @Test
    void aFinishedRowIsFinal() {
        String k = key("a", 1, 0);
        Claimed c = claimed(k, NOW);
        assertTrue(ledger.finish(k, c.token(), OutcomeKind.CANCELLED, "cancelled"));

        assertEquals(new ClaimResult.NotClaimed("final"), ledger.claim(k, SEND_BY, 3, NOW.plus(Duration.ofDays(1))));
        assertFalse(ledger.finish(k, c.token(), OutcomeKind.SENT, null));
        assertEquals(List.of(), due(k, NOW.plus(Duration.ofDays(1))));
    }

    @Test
    void markRetryReleasesTheLeaseUntilExactlyNextAttemptAt() {
        String k = key("a", 1, 0);
        Claimed c = claimed(k, NOW);
        Instant next = NOW.plusSeconds(60);

        assertTrue(ledger.markRetry(k, c.token(), next));
        assertFalse(ledger.finish(k, c.token(), OutcomeKind.SENT, null));
        assertEquals(new ClaimResult.NotClaimed("leased"), ledger.claim(k, SEND_BY, 3, next.minusMillis(1)));

        Claimed retry = claimed(k, next);
        assertEquals(2, retry.attempts());
        assertEquals(SEND_BY, retry.sendBy());
    }

    @Test
    void dueRetriesListsSendingRowsFromLeaseUntilAndRetryingRowsFromNextAttemptAt() {
        String sending = key("s", 1, 0);
        String retrying = key("r", 1, 1);
        String done = key("d", 1, 0);
        Claimed s = claimed(sending, NOW);
        Claimed r = claimed(retrying, NOW);
        ledger.markRetry(retrying, r.token(), NOW.plusSeconds(30));
        Claimed d = claimed(done, NOW);
        ledger.finish(done, d.token(), OutcomeKind.SENT, null);

        assertEquals(List.of(), due(sending, s.leaseUntil().minusMillis(1)));
        assertEquals(List.of(new DueRetry(sending, 3, SEND_BY)), due(sending, s.leaseUntil()));
        assertEquals(List.of(), due(retrying, NOW.plusSeconds(29)));
        assertEquals(List.of(new DueRetry(retrying, 3, SEND_BY)), due(retrying, NOW.plusSeconds(30)));
        assertEquals(List.of(), due(done, NOW.plus(Duration.ofDays(1))));
    }

    @Test
    void dueRetriesIsPerShardEarliestFirstAndLimited() {
        String first = null;
        String second = null;
        String otherShard = null;
        int shard = Shards.of(prefix + "x0", SHARDS);
        for (int i = 0; first == null || second == null || otherShard == null; i++) {
            String cart = "x" + i;
            boolean same = Shards.of(prefix + cart, SHARDS) == shard;
            if (same && first == null) first = key(cart, 1, 0);
            else if (same && second == null) second = key(cart, 1, 0);
            else if (!same && otherShard == null) otherShard = key(cart, 1, 0);
        }
        ledger.markRetry(second, claimed(second, NOW).token(), NOW.plusSeconds(20));
        ledger.markRetry(first, claimed(first, NOW).token(), NOW.plusSeconds(10));
        ledger.markRetry(otherShard, claimed(otherShard, NOW).token(), NOW.plusSeconds(5));

        List<DueRetry> all = ledger.dueRetries(shard, NOW.plusSeconds(60), 100).stream()
            .filter(d -> d.key().startsWith(prefix)).toList();
        assertEquals(List.of(new DueRetry(first, 3, SEND_BY), new DueRetry(second, 3, SEND_BY)), all);
        assertEquals(1, ledger.dueRetries(shard, NOW.plusSeconds(60), 1).size());
    }

    @Test
    void reopenMovesOnlyADeadRowBackToRetryingWithAttemptsReset() {
        String k = key("a", 1, 0);
        Claimed c = claimed(k, NOW);
        assertFalse(ledger.reopen(k, NOW));
        ledger.finish(k, c.token(), OutcomeKind.DEAD, "permanent_failure");
        Instant later = NOW.plusSeconds(120);

        assertTrue(ledger.reopen(k, later));
        assertFalse(ledger.reopen(k, later));
        assertEquals(List.of(new DueRetry(k, 3, SEND_BY)), due(k, later));
        assertEquals(1, claimed(k, later).attempts());
        assertFalse(ledger.reopen(key("missing", 1, 0), later));
    }

    @Test
    void reopenLeavesOtherFinalRowsAlone() {
        String k = key("a", 1, 0);
        ledger.finish(k, claimed(k, NOW).token(), OutcomeKind.SENT, null);

        assertFalse(ledger.reopen(k, NOW));
        assertEquals(new ClaimResult.NotClaimed("final"), ledger.claim(k, SEND_BY, 3, NOW));
    }

    @Test
    void highestOffsetIndexIsPerCartAndVersionOverAnyStatus() {
        assertEquals(-1, ledger.highestOffsetIndex(prefix + "a", 1));
        ledger.finish(key("a", 1, 0), claimed(key("a", 1, 0), NOW).token(), OutcomeKind.SKIPPED_LATE, "late");
        claimed(key("a", 1, 2), NOW);
        claimed(key("a", 2, 1), NOW);
        claimed(key("ab", 1, 3), NOW);

        assertEquals(2, ledger.highestOffsetIndex(prefix + "a", 1));
        assertEquals(1, ledger.highestOffsetIndex(prefix + "a", 2));
    }

    @Test
    void aCartIdContainingColonsWorksEverywhere() {
        String k = key("x:1:2", 7, 1);
        Claimed c = claimed(k, NOW);
        ledger.markRetry(k, c.token(), NOW);

        assertEquals(List.of(new DueRetry(k, 3, SEND_BY)), due(k, NOW));
        assertEquals(1, ledger.highestOffsetIndex(prefix + "x:1:2", 7));
        assertEquals(-1, ledger.highestOffsetIndex(prefix + "x", 1));
    }

    @Test
    void dueRetriesCarryTheStoredSendBy() {
        String k = key("a", 1, 0);
        ledger.markRetry(k, claimed(k, NOW).token(), NOW);

        assertEquals(SEND_BY, due(k, NOW).get(0).sendBy());
    }
}
