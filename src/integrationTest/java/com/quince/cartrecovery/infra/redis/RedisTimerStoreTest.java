package com.quince.cartrecovery.infra.redis;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.model.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class RedisTimerStoreTest {
    private static final int SHARDS = 8;
    private static final Instant LONG_AGO = Instant.ofEpochMilli(1_000);
    private static final Instant FAR_FUTURE = Instant.ofEpochMilli(9_999_999_999_999L);

    private RedisTimerStore store;

    @BeforeEach
    void setUp() {
        TestRedis.flushAll();
        store = new RedisTimerStore(TestRedis.connection(), SHARDS, Duration.ofSeconds(30));
    }

    @Test
    void packedValueNeverContainsTheCartId() {
        Timer reminder = Timer.reminder("a:b|c", 12, 1, LONG_AGO, 5);
        assertEquals("REMINDER|12|1|5|1000", RedisTimerStore.pack(reminder));
        assertEquals(reminder, RedisTimerStore.unpack("a:b|c", RedisTimerStore.pack(reminder)));

        Timer check = Timer.checkAbandon("x", 3, Instant.ofEpochMilli(7), -1);
        assertEquals("CHECK_ABANDON|3|-1|-1|7", RedisTimerStore.pack(check));
        assertEquals(check, RedisTimerStore.unpack("x", RedisTimerStore.pack(check)));
    }

    @Test
    void cartIdWithDelimitersRoundTripsThroughClaimAndAck() {
        String id = "x|1:2";
        Timer t = Timer.checkAbandon(id, 3, LONG_AGO, 0);
        assertTrue(store.upsert(t).written());
        assertEquals(Set.of(id), store.existing(Shards.of(id, SHARDS), List.of(id)));
        assertEquals(List.of(t), store.claimDue(10));
        store.ack(t);
        assertEquals(Set.of(), store.existing(Shards.of(id, SHARDS), List.of(id)));
    }

    @Test
    void upsertIsMonotonicAndEqualDataIsANoOp() {
        Timer r0 = Timer.reminder("c", 2, 0, LONG_AGO, 1);
        assertTrue(store.upsert(r0).written());
        assertFalse(store.upsert(r0).written());
        assertFalse(store.upsert(Timer.checkAbandon("c", 2, LONG_AGO, 1)).written());
        assertTrue(store.upsert(Timer.checkAbandon("c", 3, LONG_AGO, 1)).written());
    }

    @Test
    void upsertRejectsANegativeVersion() {
        assertThrows(IllegalArgumentException.class, () -> store.upsert(Timer.checkAbandon("c", -1, LONG_AGO, 0)));
    }

    @Test
    void claimDueWalksAllShardsUpToTheLimitAndLeases() {
        Set<String> ids = IntStream.range(0, 20).mapToObj(i -> "cart-" + i).collect(toSet());
        ids.forEach(id -> store.upsert(Timer.checkAbandon(id, 1, LONG_AGO, 0)));
        assertTrue(ids.stream().map(id -> Shards.of(id, SHARDS)).distinct().count() > 1, "ids spread over shards");

        List<Timer> first = store.claimDue(15);
        List<Timer> second = store.claimDue(15);
        assertEquals(15, first.size());
        assertEquals(5, second.size());
        assertEquals(ids, Stream.concat(first.stream(), second.stream()).map(Timer::cartId).collect(toSet()));
        assertEquals(List.of(), store.claimDue(15), "everything is leased");
    }

    @Test
    void claimDueAcrossShardsIsSortedByDueAtEarliestFirst() {
        // Same due time would land in shard-order, which is not due order. Use spread-out due times so
        // only a real merge-sort across shards produces the right order.
        List<String> ids = IntStream.range(0, 12).mapToObj(i -> "sort-" + i).toList();
        // Reverse insert order relative to due time and relative to shard, so shard-arrival order != due order.
        for (int i = 0; i < ids.size(); i++) {
            store.upsert(Timer.checkAbandon(ids.get(ids.size() - 1 - i), 1, LONG_AGO.plusMillis(i), 0));
        }
        List<Timer> claimed = store.claimDue(100);
        List<Instant> dueAts = claimed.stream().map(Timer::dueAt).toList();
        List<Instant> sorted = new ArrayList<>(dueAts);
        sorted.sort(Instant::compareTo);
        assertEquals(sorted, dueAts, "claimDue must return timers sorted by dueAt, earliest first");
    }

    @Test
    void releaseAckAndRemoveApplyOnlyToUnchangedOrOlderTimers() {
        Timer t = Timer.checkAbandon("c", 1, LONG_AGO, 0);
        store.upsert(t);
        Timer claimed = store.claimDue(1).get(0);
        store.release(claimed, Duration.ZERO);
        assertEquals(List.of(t), store.claimDue(1), "released with no delay: due again");

        store.upsert(Timer.checkAbandon("c", 2, FAR_FUTURE, 0));
        store.ack(t);
        assertEquals(Set.of("c"), store.existing(0, List.of("c")), "stale ack leaves the newer timer");
        store.remove("c", 1);
        assertEquals(Set.of("c"), store.existing(0, List.of("c")), "stored version 2 is greater than 1");
        store.remove("c", 2);
        assertEquals(Set.of(), store.existing(0, List.of("c")));
    }

    @Test
    void existingChecksEachIdInItsOwnShardAcrossChunks() {
        List<String> ids = IntStream.range(0, 1_200).mapToObj(i -> "e-" + i).toList();
        ids.forEach(id -> store.upsert(Timer.checkAbandon(id, 1, FAR_FUTURE, 0)));
        List<String> query = new ArrayList<>(ids);
        query.add("absent-1");
        query.add("absent-2");
        assertEquals(new HashSet<>(ids), store.existing(0, query));
        assertEquals(Set.of(), store.existing(0, List.of()));
    }

    @Test
    void pastDueSumsOnlyTimersDueAtOrBeforeNowAcrossShards() {
        store.upsert(Timer.checkAbandon("past-a", 1, LONG_AGO, 0));
        store.upsert(Timer.checkAbandon("past-b", 1, LONG_AGO.plusMillis(1), 0));
        store.upsert(Timer.checkAbandon("future", 1, FAR_FUTURE, 0));
        assertEquals(2, store.pastDue());
    }

    @Test
    void reloadsScriptsAfterScriptFlush() {
        TestRedis.sync().scriptFlush();
        assertTrue(store.upsert(Timer.checkAbandon("c", 1, LONG_AGO, 0)).written());
        assertEquals(1, store.claimDue(10).size());
    }
}
