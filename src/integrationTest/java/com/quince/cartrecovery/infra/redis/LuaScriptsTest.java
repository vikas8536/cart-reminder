package com.quince.cartrecovery.infra.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class LuaScriptsTest {
    private static final String Z = "timers:{0}";
    private static final String H = "timerdata:{0}";
    private static final String WM = "watermarks";

    private RedisCommands<String, String> redis;

    @BeforeEach
    void setUp() {
        redis = TestRedis.sync();
        redis.flushall();
    }

    private static String lua(String name) {
        try (InputStream in = LuaScriptsTest.class.getResourceAsStream("/redis/" + name + ".lua")) {
            if (in == null) throw new IllegalStateException("missing /redis/" + name + ".lua");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String packed(String kind, long version, int offset, int src, long dueAt) {
        return kind + "|" + version + "|" + offset + "|" + src + "|" + dueAt;
    }

    /** [written (Long), previous packed value or ""]; Lettuce may decode Lua's '' as null, which the store treats alike. */
    private List<Object> upsertRaw(String id, String kind, long version, int offset, int src, long dueAt) {
        List<Object> r = redis.eval(lua("upsert"), ScriptOutputType.MULTI, new String[] {Z, H},
                id, packed(kind, version, offset, src, dueAt), Long.toString(version), Integer.toString(offset), Long.toString(dueAt));
        return List.of(r.get(0), r.get(1) == null ? "" : r.get(1));
    }

    private long upsert(String id, String kind, long version, int offset, int src, long dueAt) {
        return (Long) upsertRaw(id, kind, version, offset, src, dueAt).get(0);
    }

    private List<Object> claim(int n, long leaseMs) {
        return redis.eval(lua("claim"), ScriptOutputType.MULTI, new String[] {Z, H}, Integer.toString(n), Long.toString(leaseMs));
    }

    private long release(String id, String data, long delayMs) {
        Long r = redis.eval(lua("release"), ScriptOutputType.INTEGER, new String[] {Z, H}, id, data, Long.toString(delayMs));
        return r;
    }

    private long ack(String id, String data) {
        Long r = redis.eval(lua("ack"), ScriptOutputType.INTEGER, new String[] {Z, H}, id, data);
        return r;
    }

    private String remove(String id, long version) {
        String r = redis.eval(lua("remove"), ScriptOutputType.VALUE, new String[] {Z, H}, id, Long.toString(version));
        return r == null ? "" : r;
    }

    private long wmSet(int partition, long generation, long eventTime) {
        return wmSet(partition, generation, eventTime, 5_000);
    }

    private long wmSet(int partition, long generation, long eventTime, long staleMs) {
        Long r = redis.eval(lua("wmSet"), ScriptOutputType.INTEGER, new String[] {WM},
                Integer.toString(partition), Long.toString(generation), Long.toString(eventTime), Long.toString(staleMs));
        return r;
    }

    private List<Object> wmGet(long staleMs, int... partitions) {
        List<String> args = new ArrayList<>();
        args.add(Long.toString(staleMs));
        for (int p : partitions) args.add(Integer.toString(p));
        return redis.eval(lua("wmGet"), ScriptOutputType.MULTI, new String[] {WM}, args.toArray(String[]::new));
    }

    private long redisNow() { return TestRedis.now().toEpochMilli(); }

    @Test
    void upsertWritesDataAndScore() {
        assertEquals(1, upsert("c1", "CHECK_ABANDON", 1, -1, 3, 5_000));
        assertEquals("CHECK_ABANDON|1|-1|3|5000", redis.hget(H, "c1"));
        assertEquals(5_000.0, redis.zscore(Z, "c1"));
    }

    @Test
    void upsertIsMonotonicWithCheckAbandonBelowReminderZero() {
        assertEquals(1, upsert("c1", "REMINDER", 5, 1, 0, 1_000));
        assertEquals(0, upsert("c1", "REMINDER", 4, 2, 0, 2_000), "older version");
        assertEquals(0, upsert("c1", "REMINDER", 5, 0, 0, 2_000), "same version, lower offset");
        assertEquals(0, upsert("c1", "CHECK_ABANDON", 5, -1, 0, 2_000), "CHECK_ABANDON counts as -1");
        assertEquals(1, upsert("c1", "REMINDER", 5, 2, 0, 3_000));
        assertEquals(1, upsert("c1", "CHECK_ABANDON", 6, -1, 0, 4_000), "newer version beats any offset");
        assertEquals("CHECK_ABANDON|6|-1|0|4000", redis.hget(H, "c1"));
        assertEquals(4_000.0, redis.zscore(Z, "c1"));

        assertEquals(1, upsert("c2", "CHECK_ABANDON", 1, -1, 0, 1_000));
        assertEquals(1, upsert("c2", "REMINDER", 1, 0, 0, 2_000), "REMINDER 0 is greater than CHECK_ABANDON");
    }

    @Test
    void upsertComparesVersionsExactlyBeyondDoublePrecision() {
        // 2^53 and 2^53 + 1 are the same Lua double; the script compares decimal strings instead.
        assertEquals(1, upsert("c1", "CHECK_ABANDON", 9_007_199_254_740_992L, -1, 0, 1_000));
        assertEquals(1, upsert("c1", "CHECK_ABANDON", 9_007_199_254_740_993L, -1, 0, 1_000));
        assertEquals(0, upsert("c1", "CHECK_ABANDON", 9_007_199_254_740_992L, -1, 0, 1_000));
        assertEquals(0, upsert("c1", "CHECK_ABANDON", 999_999_999_999_999L, -1, 0, 1_000), "fewer digits is smaller");
        assertEquals(1, upsert("c1", "CHECK_ABANDON", 10_000_000_000_000_000L, -1, 0, 1_000), "more digits is greater");
    }

    @Test
    void equalKeyIsANoOpThatKeepsTheLeaseScore() {
        upsert("c1", "REMINDER", 2, 0, 0, 1_000);
        assertEquals(List.of("c1", "REMINDER|2|0|0|1000"), claim(10, 60_000));
        Double leased = redis.zscore(Z, "c1");
        assertTrue(leased >= redisNow() + 50_000, "claimed member is re-scored to TIME + lease");

        assertEquals(0, upsert("c1", "REMINDER", 2, 0, 0, 1_000), "identical data");
        assertEquals(0, upsert("c1", "REMINDER", 2, 0, 7, 9_000), "same (version, offset), different data");
        assertEquals(leased, redis.zscore(Z, "c1"));
        assertEquals("REMINDER|2|0|0|1000", redis.hget(H, "c1"));
    }

    @Test
    void claimTakesOnlyDueMembersLowestScoreFirstUpToLimit() {
        long now = redisNow();
        upsert("a", "CHECK_ABANDON", 1, -1, 0, 1_000);
        upsert("b", "CHECK_ABANDON", 1, -1, 0, 2_000);
        upsert("c", "CHECK_ABANDON", 1, -1, 0, now + 600_000);

        assertEquals(List.of("a", "CHECK_ABANDON|1|-1|0|1000"), claim(1, 30_000));
        assertEquals(List.of("b", "CHECK_ABANDON|1|-1|0|2000"), claim(10, 30_000), "a is leased, c not due");
        assertEquals(List.of(), claim(10, 30_000));
    }

    @Test
    void claimDropsIndexEntriesWithoutData() {
        redis.zadd(Z, 1, "ghost");
        assertEquals(List.of(), claim(10, 30_000));
        assertNull(redis.zscore(Z, "ghost"));
    }

    @Test
    void releaseReschedulesOnlyIfUnchanged() {
        upsert("c1", "REMINDER", 3, 0, 0, 1_000);
        String data = "REMINDER|3|0|0|1000";
        claim(10, 60_000);
        Double leased = redis.zscore(Z, "c1");

        assertEquals(0, release("c1", "REMINDER|2|0|0|1000", 0));
        assertEquals(leased, redis.zscore(Z, "c1"));

        assertEquals(1, release("c1", data, 0));
        assertEquals(List.of("c1", data), claim(10, 60_000), "due again at once");

        assertEquals(1, release("c1", data, 600_000));
        assertTrue(redis.zscore(Z, "c1") >= redisNow() + 590_000);
        assertEquals(0, release("missing", data, 0));
    }

    @Test
    void ackRemovesOnlyIfUnchanged() {
        upsert("c1", "CHECK_ABANDON", 1, -1, 0, 1_000);
        claim(10, 60_000);
        upsert("c1", "CHECK_ABANDON", 2, -1, 0, 5_000); // newer event arrived while the timer was processed

        assertEquals(0, ack("c1", "CHECK_ABANDON|1|-1|0|1000"));
        assertEquals("CHECK_ABANDON|2|-1|0|5000", redis.hget(H, "c1"));
        assertEquals(5_000.0, redis.zscore(Z, "c1"));

        assertEquals(1, ack("c1", "CHECK_ABANDON|2|-1|0|5000"));
        assertNull(redis.hget(H, "c1"));
        assertNull(redis.zscore(Z, "c1"));
    }

    @Test
    void removeOnlyIfStoredVersionIsNotGreater() {
        upsert("c1", "REMINDER", 5, 1, 0, 1_000);
        assertEquals("", remove("c1", 4));
        assertEquals("REMINDER|5|1|0|1000", redis.hget(H, "c1"));
        assertEquals("REMINDER|5|1|0|1000", remove("c1", 5));
        assertNull(redis.hget(H, "c1"));
        assertNull(redis.zscore(Z, "c1"));

        upsert("c2", "CHECK_ABANDON", 5, -1, 0, 1_000);
        assertEquals("CHECK_ABANDON|5|-1|0|1000", remove("c2", 9));
        assertEquals("", remove("missing", 1));
    }

    @Test
    void upsertReturnsThePreviousPackedValueOnlyWhenItOverwrites() {
        assertEquals(List.of(1L, ""), upsertRaw("c1", "CHECK_ABANDON", 1, -1, 0, 1_000), "first write");
        assertEquals(List.of(1L, "CHECK_ABANDON|1|-1|0|1000"), upsertRaw("c1", "REMINDER", 1, 0, 0, 2_000), "overwrite");
        assertEquals(List.of(0L, ""), upsertRaw("c1", "REMINDER", 1, 0, 0, 2_000), "equal no-op");
        assertEquals(List.of(0L, ""), upsertRaw("c1", "CHECK_ABANDON", 1, -1, 0, 3_000), "lower no-op");
    }

    @Test
    void cartIdsWithDelimitersAreHashFieldsAndMembersOnly() {
        String id = "shop:42|cart:7";
        assertEquals(1, upsert(id, "REMINDER", 3, 1, 2, 1_000));
        String data = redis.hget(H, id);
        assertEquals("REMINDER|3|1|2|1000", data);
        assertEquals(5, data.split("\\|", -1).length, "packed value holds exactly five fields and no cart id");
        assertEquals(List.of(id, data), claim(10, 30_000));
        assertEquals(1, release(id, data, 0));
        assertEquals(0, upsert(id, "REMINDER", 3, 0, 2, 1_000));
        assertEquals(1, ack(id, data));
        assertNull(redis.zscore(Z, id));
    }

    @Test
    void wmSetFencesOlderGenerationsAndKeepsMaxWithinOne() {
        assertEquals(1, wmSet(0, 5, 1_000));
        assertEquals(1, wmSet(0, 5, 900), "same generation is accepted but keeps the max");
        assertEquals("1000", wmGet(5_000, 0).get(0));

        assertEquals(0, wmSet(0, 4, 9_000), "zombie writer from an older generation");
        assertEquals("1000", wmGet(5_000, 0).get(0));

        assertEquals(1, wmSet(0, 6, 500), "a newer generation overwrites");
        assertEquals("500", wmGet(5_000, 0).get(0));
    }

    /** Final review finding 1: a consumer-group reset restarts generations low; a stale entry must not fence them forever. */
    @Test
    void wmSetAcceptsALowerGenerationOnlyOnceTheStoredEntryIsStale() throws InterruptedException {
        assertEquals(1, wmSet(0, 50, 9_000, 200));
        assertEquals(0, wmSet(0, 1, 1_000, 200), "fresh entry: the lower generation is still a zombie");
        assertEquals("9000", wmGet(200, 0).get(0));

        Thread.sleep(300);
        assertEquals(1, wmSet(0, 1, 1_000, 200), "stale entry: the reset group's lower generation takes over");
        assertEquals("1000", wmGet(200, 0).get(0));
        assertEquals(0, wmSet(0, 0, 5_000, 200), "and is fenced again while it is fresh");
    }

    @Test
    void wmSetStampsRedisTime() {
        long before = redisNow();
        wmSet(3, 1, 42);
        String[] f = redis.hget(WM, "3").split("\\|");
        assertEquals("1", f[0]);
        assertEquals("42", f[1]);
        long updatedAt = Long.parseLong(f[2]);
        assertTrue(updatedAt >= before && updatedAt <= redisNow());
    }

    @Test
    void wmGetIsZeroWhenMissingOrStaleAndMinOverPartitions() throws InterruptedException {
        wmSet(0, 1, 1_000);
        wmSet(1, 1, 2_000);

        List<Object> r = wmGet(5_000, 0, 1);
        assertEquals("1000", r.get(0));
        assertTrue(Math.abs(Long.parseLong((String) r.get(1)) - redisNow()) < 1_000, "second element is Redis TIME");
        assertEquals("0", wmGet(5_000, 0, 1, 2).get(0), "partition 2 never written");

        Thread.sleep(300);
        assertEquals("0", wmGet(200, 0).get(0), "stale after staleAfterMs without writes");
        wmSet(0, 1, 1_000);
        assertEquals("1000", wmGet(200, 0).get(0), "any write refreshes updatedAt");
    }
}
