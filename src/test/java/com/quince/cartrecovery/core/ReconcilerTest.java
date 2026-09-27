package com.quince.cartrecovery.core;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.InMemorySendLedger;
import com.quince.cartrecovery.inmemory.PriorityQueueTimerStore;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.CartStateStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReconcilerTest {
    private static final int SHARDS = 4;

    private FakeClock clock;
    private InMemoryCartStateStore store;
    private PriorityQueueTimerStore timers;
    private InMemorySendLedger ledger;
    private Metrics metrics;
    private final List<List<String>> loaded = new ArrayList<>();
    private Reconciler reconciler;

    @BeforeEach
    void setUp() {
        clock = new FakeClock(T0);
        store = new InMemoryCartStateStore(RecoveryConfig.defaults(), SHARDS);
        timers = new PriorityQueueTimerStore(clock, Duration.ofSeconds(90));
        ledger = new InMemorySendLedger(Duration.ofSeconds(90), SHARDS);
        metrics = new Metrics();
        CartStateStore recording = (CartStateStore) Proxy.newProxyInstance(CartStateStore.class.getClassLoader(),
            new Class<?>[] {CartStateStore.class}, (proxy, method, args) -> {
                if (method.getName().equals("getAll")) {
                    loaded.add(((Collection<?>) args[0]).stream().map(Object::toString).sorted().toList());
                }
                try {
                    return method.invoke(store, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
        reconciler = new Reconciler(RecoveryConfig.defaults(), recording, timers, ledger, clock, metrics);
    }

    private CartRecord edit(String cartId, int partition) {
        return store.applyEvent(new CartEvent.CartEdited(cartId, SHOPPER, 1, T0, ITEMS), Arm.TREATMENT, partition)
            .orElseThrow();
    }

    private void reconcileAll() {
        for (int s = 0; s < SHARDS; s++) reconciler.reconcileShard(s);
    }

    private void sendRow(String cartId, int offset) {
        String key = new LedgerKey(cartId, 1, offset).toString();
        ClaimResult.Claimed c = (ClaimResult.Claimed) ledger.claim(key, at(hrs(48)), 0, clock.now());
        ledger.finish(key, c.token(), OutcomeKind.SENT, null);
    }

    @Test
    void reloadsOnlyTheOpenCartsWhoseTimerIsMissing() {
        edit("a", 2);
        edit("b", 3);
        timers.upsert(Timer.checkAbandon("a", 1, at(min(30)), 2));

        reconcileAll();

        assertEquals(List.of(List.of("b")), loaded);
        assertEquals(1, metrics.get("reconcile.timers_rebuilt"));
        clock.set(at(min(30)));
        assertEquals(List.of(Timer.checkAbandon("a", 1, at(min(30)), 2), Timer.checkAbandon("b", 1, at(min(30)), 3)),
            timers.claimDue(10));
    }

    @Test
    void nothingIsReloadedWhenEveryOpenCartHasATimer() {
        edit("a", 0);
        timers.upsert(Timer.checkAbandon("a", 1, at(min(30)), 0));

        reconcileAll();

        assertEquals(List.of(), loaded);
    }

    @Test
    void checksExistingTimersInBatchesOfAHundred() {
        List<String> ids = IntStream.range(0, 250).mapToObj(i -> "cart-" + i).toList();
        for (String id : ids) edit(id, 0);
        int shard = Shards.of("cart-0", SHARDS);
        long inShard = ids.stream().filter(id -> Shards.of(id, SHARDS) == shard).count();

        reconciler.reconcileShard(shard);

        assertEquals((inShard + 99) / 100, loaded.size());
        assertEquals(inShard, metrics.get("reconcile.timers_rebuilt"));
    }

    @Test
    void anAbandonedCartResumesAtTheFirstOnTimeOffsetAfterTheLedgersHighest() {
        CartRecord r = edit("a", 1);
        store.markAbandoned(r, List.of(T0), true);
        sendRow("a", 0);
        clock.set(at(hrs(3)));

        reconcileAll();

        clock.set(at(hrs(24)));
        assertEquals(List.of(Timer.reminder("a", 1, 2, at(hrs(24)), 1)), timers.claimDue(10));
    }

    @Test
    void anAbandonedCartWithNoOnTimeOffsetLeftEndsItsSequence() {
        CartRecord r = edit("a", 0);
        store.markAbandoned(r, List.of(T0), true);
        sendRow("a", 1);
        clock.set(at(hrs(24)).plus(min(31)));

        reconcileAll();

        assertEquals(0, timers.size());
        assertEquals(1, metrics.get("reconcile.sequences_ended"));
        assertEquals(List.of(), store.openCartIds(Shards.of("a", SHARDS), clock.now()).toList());
    }

    @Test
    void closedAndIneligibleCartsAreNotListedOrRebuilt() {
        edit("closed", 0);
        store.applyEvent(new CartEvent.CartPurchased("closed", SHOPPER, 2, T0), Arm.TREATMENT, 0);
        CartRecord holdout = store.applyEvent(new CartEvent.CartEdited("holdout", SHOPPER, 1, T0, ITEMS), Arm.HOLDOUT, 0)
            .orElseThrow();
        store.markAbandoned(holdout, List.of(T0), false);

        reconcileAll();

        assertEquals(List.of(), loaded);
        assertEquals(0, timers.size());
    }

    @Test
    void cartsPastTheirOpenUntilAreNotListed() {
        edit("a", 0);
        clock.set(at(hrs(25)).plusMillis(1));

        reconcileAll();

        assertEquals(List.of(), loaded);
        assertEquals(0, timers.size());
    }
}
