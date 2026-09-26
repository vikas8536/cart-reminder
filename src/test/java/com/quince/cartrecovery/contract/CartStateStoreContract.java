package com.quince.cartrecovery.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.CartStateStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour every CartStateStore must share. Time is an explicit argument of this port, so no time hook is needed.
 * Cart ids carry a per-test prefix, so a subclass may share one table across tests.
 */
public abstract class CartStateStoreContract {
    protected static final int SHARDS = 8;
    protected static final Instant T0 = Instant.parse("2026-01-01T09:00:00Z");
    protected static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));

    protected final String prefix = "c" + UUID.randomUUID().toString().substring(0, 8) + "-";
    protected CartStateStore store;

    /** A store holding none of this test's carts, using config for openUntil and shards for the open index. */
    protected abstract CartStateStore newStore(RecoveryConfig config, int shards);

    @BeforeEach
    void createStore() {
        store = newStore(RecoveryConfig.defaults(), SHARDS);
    }

    protected String cart(String name) { return prefix + name; }

    private static CartEvent.CartEdited edited(String id, long v, Instant at, String firstName) {
        return new CartEvent.CartEdited(id, "shopper-" + id, v, at, ITEMS, firstName);
    }

    private Set<String> open(String id, Instant now) {
        try (Stream<String> ids = store.openCartIds(Shards.of(id, SHARDS), now)) {
            return ids.filter(x -> x.startsWith(prefix)).collect(Collectors.toSet());
        }
    }

    private CartRecord stored(String id) { return store.get(id).orElseThrow(); }

    @Test
    void applyEventCreatesAnActiveRecordWithArmNameAndPartition() {
        String id = cart("a");
        CartRecord r = store.applyEvent(edited(id, 1, T0, "Ada"), Arm.HOLDOUT, 3).orElseThrow();

        assertEquals(new CartRecord(id, "shopper-" + id, CartStatus.ACTIVE, 1, T0, ITEMS, Arm.HOLDOUT, List.of(), "Ada", 3), r);
        assertEquals(r, stored(id));
    }

    @Test
    void staleAndDuplicateEventsAreRejected() {
        String id = cart("a");
        store.applyEvent(edited(id, 2, T0.plusSeconds(20), null), Arm.TREATMENT, 0);

        assertEquals(Optional.empty(), store.applyEvent(edited(id, 2, T0.plusSeconds(20), null), Arm.TREATMENT, 0));
        assertEquals(Optional.empty(), store.applyEvent(edited(id, 1, T0, null), Arm.TREATMENT, 0));
        assertEquals(Optional.empty(), store.applyEvent(new CartEvent.CartPurchased(id, "s", 1, T0), Arm.TREATMENT, 0));
        assertEquals(2, stored(id).version());
        assertEquals(CartStatus.ACTIVE, stored(id).status());
    }

    @Test
    void shopperKeyAndArmAreFixedOnFirstWrite() {
        String id = cart("a");
        store.applyEvent(new CartEvent.CartEdited(id, "guest-1", 1, T0, ITEMS), Arm.TREATMENT, 0);
        store.applyEvent(new CartEvent.CartEdited(id, "user-9", 2, T0.plusSeconds(1), ITEMS), Arm.HOLDOUT, 0);

        assertEquals("guest-1", stored(id).shopperKey());
        assertEquals(Arm.TREATMENT, stored(id).arm());
    }

    @Test
    void editWithoutANameKeepsTheStoredNameAndResumeKeepsItems() {
        String id = cart("a");
        store.applyEvent(edited(id, 1, T0, "Ada"), Arm.TREATMENT, 0);
        store.applyEvent(new CartEvent.CartEdited(id, "s", 2, T0.plusSeconds(1), List.of()), Arm.TREATMENT, 5);
        assertEquals("Ada", stored(id).firstName());
        assertEquals(List.of(), stored(id).items());
        assertEquals(5, stored(id).srcPartition());

        store.applyEvent(edited(id, 3, T0.plusSeconds(2), null), Arm.TREATMENT, 5);
        CartRecord resumed = store.applyEvent(new CartEvent.CartResumed(id, "s", 4, T0.plusSeconds(3)), Arm.TREATMENT, 6)
            .orElseThrow();

        assertEquals(ITEMS, resumed.items());
        assertEquals("Ada", resumed.firstName());
        assertEquals(T0.plusSeconds(3), resumed.lastActivityAt());
        assertEquals(6, resumed.srcPartition());
    }

    @Test
    void absentNameAndEmptyItemsReadBackAsNullAndEmpty() {
        String id = cart("a");
        store.applyEvent(new CartEvent.CartEdited(id, "s", 1, T0, List.of(), null), Arm.TREATMENT, 0);

        assertNull(stored(id).firstName());
        assertEquals(List.of(), stored(id).items());
    }

    @Test
    void itemsAreCappedAtFifty() {
        String id = cart("a");
        List<CartItem> many = IntStream.range(0, 60).mapToObj(i -> new CartItem("SKU-" + i, "Item " + i, 1, 100)).toList();
        store.applyEvent(new CartEvent.CartEdited(id, "s", 1, T0, many), Arm.TREATMENT, 0);

        assertEquals(many.subList(0, 50), stored(id).items());
    }

    @Test
    void purchaseOfAnUnknownCartCreatesAClosedRecordThatBlocksOlderEvents() {
        String id = cart("a");
        CartRecord closed = store.applyEvent(new CartEvent.CartPurchased(id, "s", 5, T0), Arm.TREATMENT, 1).orElseThrow();

        assertEquals(CartStatus.CLOSED, closed.status());
        assertEquals(5, closed.version());
        assertEquals(Optional.empty(), store.applyEvent(edited(id, 3, T0.plusSeconds(1), null), Arm.TREATMENT, 1));
    }

    @Test
    void getAllReturnsPresentRecordsOnly() {
        String a = cart("a");
        String b = cart("b");
        store.applyEvent(edited(a, 1, T0, null), Arm.TREATMENT, 0);
        store.applyEvent(edited(b, 1, T0, null), Arm.TREATMENT, 0);

        List<CartRecord> all = store.getAll(List.of(a, b, cart("missing")));

        assertEquals(Set.of(a, b), all.stream().map(CartRecord::cartId).collect(Collectors.toSet()));
    }

    @Test
    void markAbandonedSetsStatusAndStartsAndKeepsDetectorFields() {
        String id = cart("a");
        CartRecord r = store.applyEvent(edited(id, 1, T0, "Ada"), Arm.TREATMENT, 2).orElseThrow();

        assertTrue(store.markAbandoned(r, List.of(T0), true));

        CartRecord a = stored(id);
        assertEquals(CartStatus.ABANDONED, a.status());
        assertEquals(List.of(T0), a.sequenceStarts());
        assertEquals(ITEMS, a.items());
        assertEquals("Ada", a.firstName());
        assertEquals(2, a.srcPartition());
        assertEquals(1, a.version());
    }

    @Test
    void markAbandonedFailsWhenNotActiveOrWhenANewerEventWon() {
        String id = cart("a");
        CartRecord v1 = store.applyEvent(edited(id, 1, T0, null), Arm.TREATMENT, 0).orElseThrow();
        assertTrue(store.markAbandoned(v1, List.of(T0), true));
        assertFalse(store.markAbandoned(v1, List.of(T0, T0), true));
        assertEquals(List.of(T0), stored(id).sequenceStarts());

        String other = cart("b");
        CartRecord b1 = store.applyEvent(edited(other, 1, T0, null), Arm.TREATMENT, 0).orElseThrow();
        store.applyEvent(edited(other, 2, T0.plusSeconds(1), null), Arm.TREATMENT, 0);

        assertFalse(store.markAbandoned(b1, List.of(T0), true));
        assertEquals(CartStatus.ACTIVE, stored(other).status());
        assertEquals(2, stored(other).version());
        assertEquals(List.of(), stored(other).sequenceStarts());
    }

    @Test
    void endSequenceNeedsTheSameVersionAndAbandonedStatus() {
        String id = cart("a");
        CartRecord r = store.applyEvent(edited(id, 1, T0, null), Arm.TREATMENT, 0).orElseThrow();
        assertFalse(store.endSequence(id, 1));

        store.markAbandoned(r, List.of(T0), true);

        assertFalse(store.endSequence(id, 2));
        assertTrue(store.endSequence(id, 1));
        assertEquals(CartStatus.ABANDONED, stored(id).status());
        assertFalse(store.endSequence(cart("missing"), 1));
    }

    @Test
    void openUntilIsLastActivityPlusLastOffsetAndBoundRoundedUpToTheHour() {
        String id = cart("a");
        store.applyEvent(edited(id, 1, T0, null), Arm.TREATMENT, 0);
        // 09:00 + 24h + 30m = 09:30 next day, rounded up to 10:00 (T0 + 25h), inclusive.
        Instant until = T0.plus(Duration.ofHours(25));

        assertEquals(Set.of(id), open(id, T0));
        assertEquals(Set.of(id), open(id, until.minusSeconds(1)));
        assertEquals(Set.of(id), open(id, until));
        assertEquals(Set.of(), open(id, until.plusMillis(1)));
    }

    @Test
    void aLaterEditMovesOpenUntil() {
        String id = cart("a");
        store.applyEvent(edited(id, 1, T0, null), Arm.TREATMENT, 0);
        store.applyEvent(edited(id, 2, T0.plus(Duration.ofHours(2)), null), Arm.TREATMENT, 0);

        assertEquals(Set.of(id), open(id, T0.plus(Duration.ofHours(26))));
        assertEquals(Set.of(), open(id, T0.plus(Duration.ofHours(27)).plusMillis(1)));
    }

    @Test
    void openCartIdsListsOnlyTheGivenShard() {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 40; i++) ids.add(cart("s" + i));
        for (String id : ids) store.applyEvent(edited(id, 1, T0, null), Arm.TREATMENT, 0);

        for (int shard = 0; shard < SHARDS; shard++) {
            int s = shard;
            Set<String> expected = ids.stream().filter(id -> Shards.of(id, SHARDS) == s).collect(Collectors.toSet());
            try (Stream<String> listed = store.openCartIds(shard, T0)) {
                assertEquals(expected, listed.filter(x -> x.startsWith(prefix)).collect(Collectors.toSet()));
            }
        }
    }

    @Test
    void closingIneligibleAbandonmentAndEndOfSequenceRemoveTheCartFromTheOpenIndex() {
        String purchased = cart("p");
        store.applyEvent(edited(purchased, 1, T0, null), Arm.TREATMENT, 0);
        store.applyEvent(new CartEvent.CartPurchased(purchased, "s", 2, T0.plusSeconds(1)), Arm.TREATMENT, 0);
        assertEquals(Set.of(), open(purchased, T0));

        String ineligible = cart("i");
        CartRecord i = store.applyEvent(edited(ineligible, 1, T0, null), Arm.HOLDOUT, 0).orElseThrow();
        store.markAbandoned(i, List.of(T0), false);
        assertEquals(Set.of(), open(ineligible, T0));

        String eligible = cart("e");
        CartRecord e = store.applyEvent(edited(eligible, 1, T0, null), Arm.TREATMENT, 0).orElseThrow();
        store.markAbandoned(e, List.of(T0), true);
        assertEquals(Set.of(eligible), open(eligible, T0));
        store.endSequence(eligible, 1);
        assertEquals(Set.of(), open(eligible, T0));

        store.applyEvent(edited(purchased, 3, T0.plusSeconds(2), null), Arm.TREATMENT, 0);
        assertEquals(Set.of(purchased), open(purchased, T0));
    }
}
