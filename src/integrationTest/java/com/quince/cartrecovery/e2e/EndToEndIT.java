package com.quince.cartrecovery.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.app.DetectorRole;
import com.quince.cartrecovery.app.DispatcherRole;
import com.quince.cartrecovery.app.InfraConfig;
import com.quince.cartrecovery.app.ReconcilerRole;
import com.quince.cartrecovery.app.ReplayRole;
import com.quince.cartrecovery.app.Role;
import com.quince.cartrecovery.app.RoleInfra;
import com.quince.cartrecovery.app.RoleThread;
import com.quince.cartrecovery.app.SchedulerRole;
import com.quince.cartrecovery.app.TopicTail;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Spec §8.3: roles as in-process threads on shared containers, demo-scale timings (offsets +3 s, +6 s, +9 s). */
@Testcontainers(disabledWithoutDocker = true)
class EndToEndIT {
    static final Duration WAIT = Duration.ofSeconds(25);
    static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));
    static final List<OutcomeKind> PRECEDENCE =
        List.of(OutcomeKind.SENT, OutcomeKind.DEAD, OutcomeKind.CANCELLED, OutcomeKind.SKIPPED_LATE);

    static TopicTail sends;
    static TopicTail outcomes;
    private final List<RoleThread> roles = new ArrayList<>();

    @BeforeAll
    static void infra() {
        RoleInfra.start();
        new RecoveryMetaStore(RoleInfra.ctx().dynamo()).setPaused(false);   // never depend on another class's guardrail test
        sends = new TopicTail(RoleInfra.bootstrap(), Topics.SINK_SENDS);
        outcomes = new TopicTail(RoleInfra.bootstrap(), Topics.OUTCOMES);
    }

    @AfterAll
    static void closeTails() {
        sends.close();
        outcomes.close();
    }

    @AfterEach
    void stopRoles() {
        List<Throwable> failures = new ArrayList<>();
        for (int i = roles.size() - 1; i >= 0; i--) {
            if (roles.get(i).failure() != null) failures.add(roles.get(i).failure());
            roles.get(i).close();
        }
        roles.clear();
        assertEquals(List.of(), failures, "roles failed while running");
    }

    // 1. Happy path, one cart over 8 partitions: three sends in order; idle partitions do not pause the gate.
    @Test
    void happyPathSendsThreeRemindersInOrder() {
        String cart = RoleInfra.prefix("e2e1") + "cart";
        pipeline(RoleInfra.config(Map.of()));
        edit(cart, 1, Instant.now());
        Await.until(() -> sentKeys(cart).size() >= 3, WAIT);
        assertEquals(List.of(key(cart, 1, 0), key(cart, 1, 1), key(cart, 1, 2)), sentKeys(cart));
        Await.until(() -> resolved(cart).size() == 3, WAIT);
        assertTrue(resolved(cart).values().stream().allMatch(o -> o.kind() == OutcomeKind.SENT));
    }

    // 2. Purchase mid-sequence: no sends after the purchase.
    @Test
    void purchaseMidSequenceStopsTheRemainingReminders() throws Exception {
        String cart = RoleInfra.prefix("e2e2") + "cart";
        pipeline(RoleInfra.config(Map.of()));
        Instant t0 = Instant.now();
        edit(cart, 1, t0);
        Await.until(() -> sentKeys(cart).contains(key(cart, 1, 0)), WAIT);
        RoleInfra.produce(new CartEvent.CartPurchased(cart, "shopper-" + cart, 2, Instant.now()));
        sleepUntil(t0.plusSeconds(12));   // past reminders 1 and 2 (due +6 s, +9 s)
        assertEquals(List.of(key(cart, 1, 0)), sentKeys(cart));
    }

    // 3. Duplicate and out-of-order events: exactly one send per key, all for the newest version.
    @Test
    void duplicateAndOutOfOrderEventsSendEachKeyOnce() throws Exception {
        String cart = RoleInfra.prefix("e2e3") + "cart";
        pipeline(RoleInfra.config(Map.of()));
        Instant t0 = Instant.now();
        edit(cart, 2, t0);
        edit(cart, 1, t0.minusSeconds(1));   // older version arriving later
        edit(cart, 2, t0);                   // duplicate delivery
        Await.until(() -> sentKeys(cart).size() >= 3, WAIT);
        sleepUntil(Instant.now().plusSeconds(2));
        assertEquals(List.of(key(cart, 2, 0), key(cart, 2, 1), key(cart, 2, 2)), sentKeys(cart));
    }

    // 4. Two schedulers and two dispatchers on shared shards: no duplicate sends across 200 carts.
    @Test
    void twoSchedulersAndTwoDispatchersNeverSendAKeyTwice() throws Exception {
        String prefix = RoleInfra.prefix("e2e4");
        InfraConfig c = RoleInfra.config(Map.of());
        start(new DetectorRole(), c);
        start(new SchedulerRole(), c);
        start(new SchedulerRole(), c);
        start(new DispatcherRole(), c);
        start(new DispatcherRole(), c);
        Set<String> expected = new HashSet<>();
        Instant t0 = Instant.now();
        for (int i = 0; i < 200; i++) {
            edit(prefix + i, 1, t0);
            for (int offset = 0; offset < 3; offset++) expected.add(key(prefix + i, 1, offset));
        }
        Await.until(() -> sentKeys(prefix).size() >= 600, WAIT);
        sleepUntil(Instant.now().plusSeconds(2));
        List<String> keys = sentKeys(prefix);
        assertEquals(600, keys.size(), "one send per key");
        assertEquals(expected, new HashSet<>(keys));
    }

    // 5. SEND_FAILURE_RATE=0.3 with 2 attempts: some sends after a retry, some dead-lettered; replay sends each dead key once.
    @Test
    void transientFailuresRetryThenDeadLetterAndReplaySendsEachDeadKeyOnce() throws Exception {
        String prefix = RoleInfra.prefix("e2e5");
        InfraConfig failing = RoleInfra.config(Map.of("SEND_FAILURE_RATE", "0.3", "MAX_SEND_ATTEMPTS", "2",
            "LATENESS_BOUNDS", "PT60S,PT60S,PT60S"));   // a dispatcher restart plus replay outlasts 20 s
        start(new DetectorRole(), failing);
        start(new SchedulerRole(), failing);
        RoleThread failingDispatcher = start(new DispatcherRole(), failing);
        Set<String> expected = new HashSet<>();
        Instant t0 = Instant.now();
        for (int i = 0; i < 40; i++) {
            edit(prefix + i, 1, t0);
            for (int offset = 0; offset < 3; offset++) expected.add(key(prefix + i, 1, offset));
        }
        Await.until(() -> resolved(prefix).keySet().containsAll(expected), WAIT);
        Map<String, Outcome> first = resolved(prefix);
        List<String> dead = first.values().stream().filter(o -> o.kind() == OutcomeKind.DEAD).map(Outcome::key).toList();
        assertFalse(dead.isEmpty(), "some keys are dead-lettered at 30% failure with 2 attempts");
        assertTrue(first.values().stream().anyMatch(o -> o.kind() == OutcomeKind.SENT && o.attempts() >= 2),
            "some sends succeed after a retry");

        stop(failingDispatcher);
        InfraConfig healthy = RoleInfra.config(Map.of("MAX_SEND_ATTEMPTS", "2", "LATENESS_BOUNDS", "PT60S,PT60S,PT60S"));
        start(new DispatcherRole(), healthy);
        RoleThread replay = new RoleThread(new ReplayRole(), healthy);
        assertTrue(replay.join(WAIT), "replay returns when caught up");
        assertNull(replay.failure());

        Await.until(() -> {
            Map<String, Outcome> now = resolved(prefix);
            return dead.stream().allMatch(k -> now.get(k).kind() == OutcomeKind.SENT);
        }, WAIT);
        List<String> keys = sentKeys(prefix);
        for (String k : dead) assertEquals(1, Collections.frequency(keys, k), k);
        assertEquals(keys.size(), new HashSet<>(keys).size(), "no key sent twice");
        assertEquals(expected, new HashSet<>(keys));
    }

    // 6. Redis FLUSHALL mid-sequence: brief pause, the reconciler rebuilds, the remaining sends happen once.
    @Test
    void flushAllMidSequenceIsRebuiltWithoutDuplicates() throws Exception {
        String prefix = RoleInfra.prefix("e2e6");
        InfraConfig c = RoleInfra.config(Map.of());
        pipeline(c);
        start(new ReconcilerRole(), c);
        Set<String> expected = new HashSet<>();
        Instant t0 = Instant.now();
        for (int i = 0; i < 5; i++) {
            edit(prefix + i, 1, t0);
            for (int offset = 0; offset < 3; offset++) expected.add(key(prefix + i, 1, offset));
        }
        Await.until(() -> {
            List<String> keys = sentKeys(prefix);
            for (int i = 0; i < 5; i++) if (!keys.contains(key(prefix + i, 1, 0))) return false;
            return true;
        }, WAIT);
        RoleInfra.ctx().redis().sync().flushall();
        Await.until(() -> sentKeys(prefix).size() >= 15, WAIT);
        sleepUntil(Instant.now().plusSeconds(2));
        List<String> keys = sentKeys(prefix);
        assertEquals(15, keys.size(), "no duplicates after the rebuild");
        assertEquals(expected, new HashSet<>(keys));
    }

    // 7. Detector stopped, purchase produced, reminder due: no send; detector restarted: the reminder is cancelled.
    @Test
    void stoppedDetectorHoldsTheReminderUntilThePurchaseCancelsIt() throws Exception {
        String cart = RoleInfra.prefix("e2e7") + "cart";
        // The gate holds reminder 0 only if the last watermark (about when the detector stopped) is older than
        // due − CLOCK_SKEW (2 s): due at +10 s leaves the detector until +8 s to stop.
        InfraConfig c = RoleInfra.config(Map.of("OFFSETS", "PT10S,PT13S,PT16S"));
        RoleThread detector = start(new DetectorRole(), c);
        start(new SchedulerRole(), c);
        start(new DispatcherRole(), c);
        Instant t0 = Instant.now();
        edit(cart, 1, t0);
        Await.until(() -> abandoned(cart), WAIT);
        stop(detector);
        assertTrue(Instant.now().isBefore(t0.plusSeconds(8)), "detector stopped too late for the gate to hold reminder 0");
        RoleInfra.produce(new CartEvent.CartPurchased(cart, "shopper-" + cart, 2, Instant.now()));
        sleepUntil(t0.plusSeconds(14));   // reminder 0 due at +10 s: held by the stale watermark
        assertEquals(List.of(), sentKeys(cart));
        start(new DetectorRole(), c);
        Await.until(() -> {
            Outcome o = resolved(cart).get(key(cart, 1, 0));
            return o != null && o.kind() == OutcomeKind.CANCELLED;
        }, WAIT);
        assertEquals(List.of(), sentKeys(cart));
    }

    private RoleThread start(Role role, InfraConfig config) {
        RoleThread t = new RoleThread(role, config);
        roles.add(t);
        return t;
    }

    private void stop(RoleThread role) {
        role.close();
        roles.remove(role);
    }

    private void pipeline(InfraConfig c) {
        start(new DetectorRole(), c);
        start(new SchedulerRole(), c);
        start(new DispatcherRole(), c);
    }

    private static void edit(String cartId, long version, Instant at) {
        RoleInfra.produce(new CartEvent.CartEdited(cartId, "shopper-" + cartId, version, at, ITEMS, "Ada"));
    }

    private static String key(String cartId, long version, int offset) {
        return new LedgerKey(cartId, version, offset).toString();
    }

    /** Keys at the sink in the order sink-sends holds them (per cart, one partition, so in send order). */
    private static List<String> sentKeys(String prefix) {
        return sends.records(prefix).stream().map(r -> JsonCodec.decodeSinkSend(r.value()).key()).toList();
    }

    /** One outcome per key by precedence SENT > DEAD > CANCELLED > SKIPPED_LATE; ABANDONED has no key. */
    private static Map<String, Outcome> resolved(String prefix) {
        Map<String, Outcome> best = new HashMap<>();
        for (var r : outcomes.records(prefix)) {
            Outcome o = JsonCodec.decodeOutcome(r.value());
            if (o.key() == null) continue;
            best.merge(o.key(), o, (a, b) -> PRECEDENCE.indexOf(b.kind()) < PRECEDENCE.indexOf(a.kind()) ? b : a);
        }
        return best;
    }

    private static boolean abandoned(String cartId) {
        return outcomes.records(cartId).stream().map(r -> JsonCodec.decodeOutcome(r.value()))
            .anyMatch(o -> o.kind() == OutcomeKind.ABANDONED && o.cartId().equals(cartId));
    }

    private static void sleepUntil(Instant t) throws InterruptedException {
        long ms = Duration.between(Instant.now(), t).toMillis();
        if (ms > 0) Thread.sleep(ms);
    }
}
