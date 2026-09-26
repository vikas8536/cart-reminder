package com.quince.cartrecovery.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour every TimerStore must share. The store keeps its own time (Redis TIME in production), so a subclass
 * supplies now() and advance(); a real-time subclass may sleep in advance(). Durations stay at a few seconds.
 * The factory's store must use SHARDS shards and hold no due timers at all (a Redis subclass flushes first):
 * claimDue would otherwise lease another test's timers.
 * Every assertion here keeps at least 100 ms of margin, so it holds on a real clock. Millisecond-exact time
 * boundaries are tested only in the in-memory subclass (controller ruling R3); infra subclasses add none.
 */
public abstract class TimerStoreContract {
    protected static final int SHARDS = 8;
    protected static final Duration LEASE = Duration.ofSeconds(1);
    private static final Duration PAST_LEASE = LEASE.plusMillis(100);

    protected final String prefix = "c" + UUID.randomUUID().toString().substring(0, 8) + "-";
    protected TimerStore store;

    protected abstract TimerStore newStore(Duration lease);

    /** The store's current time. */
    protected abstract Instant now();

    /** Moves the store's time forward by at least d. */
    protected abstract void advance(Duration d);

    @BeforeEach
    void createStore() {
        store = newStore(LEASE);
    }

    protected String cart(String name) { return prefix + name; }

    /** Store time plus offset, at millisecond precision (what Redis keeps). */
    protected Instant at(Duration offset) {
        return now().plus(offset).truncatedTo(ChronoUnit.MILLIS);
    }

    private List<Timer> claimMine(int limit) {
        return store.claimDue(limit).stream().filter(t -> t.cartId().startsWith(prefix)).toList();
    }

    private boolean exists(String cartId) {
        return store.existing(Shards.of(cartId, SHARDS), List.of(cartId)).contains(cartId);
    }

    @Test
    void claimDueReturnsOnlyDueTimersEarliestFirst() {
        Timer a = Timer.checkAbandon(cart("a"), 1, at(Duration.ofSeconds(-2)), 0);
        Timer b = Timer.checkAbandon(cart("b"), 1, at(Duration.ofSeconds(-1)), 0);
        Timer c = Timer.checkAbandon(cart("c"), 1, at(Duration.ofHours(1)), 0);
        store.upsert(c);
        store.upsert(b);
        store.upsert(a);

        assertEquals(List.of(a, b), claimMine(10));
    }

    @Test
    void claimDueRespectsTheLimit() {
        store.upsert(Timer.checkAbandon(cart("a"), 1, at(Duration.ofSeconds(-3)), 0));
        store.upsert(Timer.checkAbandon(cart("b"), 1, at(Duration.ofSeconds(-2)), 0));
        store.upsert(Timer.checkAbandon(cart("c"), 1, at(Duration.ofSeconds(-1)), 0));

        assertEquals(2, claimMine(2).size());
        assertEquals(1, claimMine(10).size());
    }

    @Test
    void aClaimedTimerIsLeasedAndRedeliveredAfterTheLease() {
        Timer a = Timer.reminder(cart("a"), 1, 0, at(Duration.ofSeconds(-1)), 4);
        store.upsert(a);

        assertEquals(List.of(a), claimMine(10));
        assertEquals(List.of(), claimMine(10));
        advance(PAST_LEASE);
        assertEquals(List.of(a), claimMine(10));
    }

    @Test
    void ackRemovesTheTimerOnlyIfUnchanged() {
        Timer v1 = Timer.checkAbandon(cart("a"), 1, at(Duration.ofSeconds(-1)), 0);
        store.upsert(v1);
        claimMine(10);
        Timer v2 = Timer.checkAbandon(cart("a"), 2, at(Duration.ofHours(1)), 0);
        store.upsert(v2);

        store.ack(v1);
        assertTrue(exists(cart("a")));

        store.ack(v2);
        assertFalse(exists(cart("a")));
    }

    @Test
    void releaseMakesAnUnchangedTimerDueAgainAfterTheDelay() {
        Timer a = Timer.checkAbandon(cart("a"), 1, at(Duration.ofSeconds(-1)), 0);
        store.upsert(a);
        claimMine(10);

        store.release(a, Duration.ofMillis(500));

        assertEquals(List.of(), claimMine(10));
        advance(Duration.ofMillis(600));
        assertEquals(List.of(a), claimMine(10));
    }

    @Test
    void releaseOfAReplacedTimerChangesNothing() {
        Timer v1 = Timer.checkAbandon(cart("a"), 1, at(Duration.ofSeconds(-1)), 0);
        store.upsert(v1);
        claimMine(10);
        Timer v2 = Timer.checkAbandon(cart("a"), 2, at(Duration.ofHours(1)), 0);
        store.upsert(v2);

        store.release(v1, Duration.ZERO);

        assertEquals(List.of(), claimMine(10));
        assertTrue(exists(cart("a")));
    }

    @Test
    void upsertOnlyMovesForwardByVersionThenOffset() {
        String id = cart("a");
        Instant due = at(Duration.ofHours(1));
        assertTrue(store.upsert(Timer.checkAbandon(id, 2, due, 0)));
        assertFalse(store.upsert(Timer.checkAbandon(id, 1, due, 0)));
        assertFalse(store.upsert(Timer.reminder(id, 1, 2, due, 0)));
        assertTrue(store.upsert(Timer.reminder(id, 2, 0, due, 0)));
        assertFalse(store.upsert(Timer.checkAbandon(id, 2, due, 0)));
        assertTrue(store.upsert(Timer.reminder(id, 2, 1, due, 0)));
        assertTrue(store.upsert(Timer.checkAbandon(id, 3, due, 0)));
    }

    @Test
    void anEqualUpsertIsANoOpThatKeepsTheLease() {
        Timer a = Timer.checkAbandon(cart("a"), 1, at(Duration.ofSeconds(-1)), 0);
        store.upsert(a);
        claimMine(10);

        assertFalse(store.upsert(a));
        assertEquals(List.of(), claimMine(10));
    }

    @Test
    void theSameVersionAndOffsetWithDifferentDataIsRejected() {
        Timer first = Timer.reminder(cart("a"), 1, 0, at(Duration.ofSeconds(-2)), 0);
        store.upsert(first);

        assertFalse(store.upsert(Timer.reminder(cart("a"), 1, 0, at(Duration.ofSeconds(-1)), 7)));
        assertEquals(List.of(first), claimMine(10));
    }

    @Test
    void removeOnlyIfTheStoredVersionIsAtMostTheGivenOne() {
        String id = cart("a");
        store.upsert(Timer.checkAbandon(id, 3, at(Duration.ofHours(1)), 0));

        store.remove(id, 2);
        assertTrue(exists(id));
        store.remove(id, 3);
        assertFalse(exists(id));

        store.upsert(Timer.reminder(id, 5, 1, at(Duration.ofHours(1)), 0));
        store.remove(id, 9);
        assertFalse(exists(id));
    }

    @Test
    void aRemovedTimerCanBeRecreatedByALateOlderUpsert() {
        String id = cart("a");
        store.upsert(Timer.checkAbandon(id, 2, at(Duration.ofHours(1)), 0));
        store.remove(id, 2);

        assertTrue(store.upsert(Timer.checkAbandon(id, 1, at(Duration.ofHours(1)), 0)));
    }

    @Test
    void existingReturnsOnlyCartsWithATimer() {
        String with = cart("with");
        String without = cart("without");
        store.upsert(Timer.checkAbandon(with, 1, at(Duration.ofHours(1)), 0));

        assertEquals(Set.of(with), store.existing(Shards.of(with, SHARDS), List.of(with)));
        assertEquals(Set.of(), store.existing(Shards.of(without, SHARDS), List.of(without)));
    }

    @Test
    void everyFieldRoundTripsIncludingDelimitersInTheCartId() {
        Timer odd = Timer.reminder(cart("a:b|c"), 12, 2, at(Duration.ofSeconds(-1)), 6);
        store.upsert(odd);

        assertEquals(List.of(odd), claimMine(10));
    }
}
