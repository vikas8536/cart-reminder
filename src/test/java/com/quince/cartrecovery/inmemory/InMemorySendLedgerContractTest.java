package com.quince.cartrecovery.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.quince.cartrecovery.contract.SendLedgerContract;
import com.quince.cartrecovery.model.ClaimResult.Claimed;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.ports.SendLedger;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class InMemorySendLedgerContractTest extends SendLedgerContract {
    @Override protected SendLedger newLedger(Duration lease, int shards) { return new InMemorySendLedger(lease, shards); }

    @Test
    void statusSizeAndNextRetryAtTrackTheRows() {
        InMemorySendLedger l = (InMemorySendLedger) ledger;
        String k = key("a", 1, 0);
        assertEquals(Optional.empty(), l.status(k));
        assertEquals(Optional.empty(), l.nextRetryAt());

        Claimed c = (Claimed) l.claim(k, SEND_BY, 0, NOW);
        assertEquals(Optional.of("SENDING"), l.status(k));
        assertEquals(Optional.of(c.leaseUntil()), l.nextRetryAt());

        l.markRetry(k, c.token(), NOW.plusSeconds(60));
        assertEquals(Optional.of("RETRYING"), l.status(k));
        assertEquals(Optional.of(NOW.plusSeconds(60)), l.nextRetryAt());

        Claimed again = (Claimed) l.claim(k, SEND_BY, 0, NOW.plusSeconds(60));
        l.finish(k, again.token(), OutcomeKind.SENT, null);
        assertEquals(Optional.of("SENT"), l.status(k));
        assertEquals(Optional.empty(), l.nextRetryAt());
        assertEquals(1, l.size());
    }

    @Test
    void abandonedIsNotALedgerOutcome() {
        String k = key("a", 1, 0);
        Claimed c = (Claimed) ledger.claim(k, SEND_BY, 0, NOW);
        assertThrows(IllegalArgumentException.class, () -> ledger.finish(k, c.token(), OutcomeKind.ABANDONED, null));
    }
}
