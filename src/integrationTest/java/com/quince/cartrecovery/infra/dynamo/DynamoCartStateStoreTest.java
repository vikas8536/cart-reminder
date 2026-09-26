package com.quince.cartrecovery.infra.dynamo;

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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@Testcontainers(disabledWithoutDocker = true)
class DynamoCartStateStoreTest {
    private static final Instant T = Instant.parse("2026-01-01T09:00:00Z");
    private static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));
    private static final int SHARDS = 8;

    private final DynamoDbClient ddb = TestDynamo.client();
    private final RecoveryConfig config = RecoveryConfig.defaults();
    private String table;
    private DynamoCartStateStore store;

    @BeforeEach
    void setUp() {
        table = TestDynamo.table("carts");
        DynamoTables.createCarts(ddb, table);
        TestDynamo.noExpiry(table);
        store = new DynamoCartStateStore(ddb, table, config, SHARDS);
    }

    private static CartEvent.CartEdited edit(String id, long version, Instant at, List<CartItem> items, String firstName) {
        return new CartEvent.CartEdited(id, "shopper-" + id, version, at, items, firstName);
    }

    private Map<String, AttributeValue> raw(String id) {
        return ddb.getItem(b -> b.tableName(table).key(Map.of("cartId", AttributeValue.fromS(id))).consistentRead(true)).item();
    }

    @Test
    void absentFirstNameAndEmptyItemsRoundTrip() {
        CartRecord written = store.applyEvent(edit("c1", 1, T, List.of(), null), Arm.TREATMENT, 3).orElseThrow();
        assertNull(written.firstName());
        assertEquals(List.of(), written.items());
        assertFalse(raw("c1").containsKey("firstName"), "null is never written as an attribute");

        CartRecord read = store.get("c1").orElseThrow();
        assertNull(read.firstName());
        assertEquals(List.of(), read.items());
        assertEquals(List.of(), read.sequenceStarts());
        assertEquals(3, read.srcPartition());

        store.applyEvent(edit("c1", 2, T, List.of(new CartItem("SKU-2", null, 2, 100)), ""), Arm.TREATMENT, 3);
        CartRecord blank = store.get("c1").orElseThrow();
        assertEquals("", blank.firstName(), "empty string is a legal non-key attribute value");
        assertEquals(List.of(new CartItem("SKU-2", null, 2, 100)), blank.items());

        store.applyEvent(edit("c1", 3, T, ITEMS, "Ada"), Arm.TREATMENT, 3);
        assertEquals("Ada", store.get("c1").orElseThrow().firstName());
        store.applyEvent(edit("c1", 4, T, ITEMS, null), Arm.TREATMENT, 3);
        assertEquals("Ada", store.get("c1").orElseThrow().firstName(), "an edit without a name keeps the stored one");
    }

    @Test
    void resumeKeepsItemsAndFirstName() {
        store.applyEvent(edit("c1", 1, T, ITEMS, "Ada"), Arm.TREATMENT, 0);
        CartRecord r = store.applyEvent(new CartEvent.CartResumed("c1", "shopper-c1", 2, T.plusSeconds(60)), Arm.TREATMENT, 5).orElseThrow();
        assertEquals(CartStatus.ACTIVE, r.status());
        assertEquals(ITEMS, r.items());
        assertEquals("Ada", r.firstName());
        assertEquals(T.plusSeconds(60), r.lastActivityAt());
        assertEquals(5, r.srcPartition(), "srcPartition follows the partition the latest event came from");
    }

    @Test
    void staleOrDuplicateEventReturnsEmptyAndLeavesTheRecord() {
        store.applyEvent(edit("c2", 5, T, ITEMS, "Ada"), Arm.TREATMENT, 1);
        assertTrue(store.applyEvent(edit("c2", 5, T.plusSeconds(1), List.of(), null), Arm.TREATMENT, 1).isEmpty());
        assertTrue(store.applyEvent(edit("c2", 4, T.plusSeconds(1), List.of(), null), Arm.TREATMENT, 1).isEmpty());
        assertTrue(store.applyEvent(new CartEvent.CartPurchased("c2", "shopper-c2", 3, T), Arm.TREATMENT, 1).isEmpty());
        CartRecord r = store.get("c2").orElseThrow();
        assertEquals(5, r.version());
        assertEquals(CartStatus.ACTIVE, r.status());
        assertEquals(ITEMS, r.items());
        assertEquals("Ada", r.firstName());
    }

    @Test
    void firstWriteFixesShopperKeyAndArm() {
        store.applyEvent(edit("c3", 1, T, ITEMS, null), Arm.HOLDOUT, 0);
        CartRecord r = store.applyEvent(new CartEvent.CartEdited("c3", "someone-else", 2, T, ITEMS, null), Arm.TREATMENT, 0).orElseThrow();
        assertEquals(Arm.HOLDOUT, r.arm());
        assertEquals("shopper-c3", r.shopperKey());
    }

    @Test
    void openIndexUsesShardAndHourRoundedOpenUntil() {
        // defaults: last offset 24 h, last bound 30 min -> 2026-01-02T09:30Z, rounded up to 10:00Z
        store.applyEvent(edit("c4", 1, T, ITEMS, null), Arm.TREATMENT, 0);
        int shard = Shards.of("c4", SHARDS);
        Instant openUntil = Instant.parse("2026-01-02T10:00:00Z");
        Map<String, AttributeValue> raw = raw("c4");
        assertEquals("s#" + shard, raw.get("openShard").s());
        assertEquals(openUntil.toEpochMilli(), Long.parseLong(raw.get("openUntil").n()));
        assertEquals(T.plus(Duration.ofDays(30)).getEpochSecond(), Long.parseLong(raw.get("ttl").n()));

        assertEquals(List.of("c4"), store.openCartIds(shard, T).toList());
        assertEquals(List.of("c4"), store.openCartIds(shard, openUntil).toList(), "openUntil >= now is inclusive");
        assertEquals(List.of(), store.openCartIds(shard, openUntil.plusMillis(1)).toList());
        assertEquals(List.of(), store.openCartIds((shard + 1) % SHARDS, T).toList());
    }

    @Test
    void openUntilOnAnExactHourIsNotRoundedFurther() {
        assertEquals(Instant.parse("2026-01-02T10:00:00Z"),
                DynamoCartStateStore.openUntil(Instant.parse("2026-01-01T09:30:00Z"), config));
        assertEquals(Instant.parse("2026-01-02T11:00:00Z"),
                DynamoCartStateStore.openUntil(Instant.parse("2026-01-01T09:30:00.001Z"), config));
    }

    @Test
    void purchaseClosesAndLeavesTheOpenIndex() {
        store.applyEvent(edit("c5", 1, T, ITEMS, null), Arm.TREATMENT, 0);
        CartRecord r = store.applyEvent(new CartEvent.CartPurchased("c5", "shopper-c5", 2, T.plusSeconds(60)), Arm.TREATMENT, 0).orElseThrow();
        assertEquals(CartStatus.CLOSED, r.status());
        assertEquals(2, r.version());
        assertEquals(ITEMS, r.items());
        assertFalse(raw("c5").containsKey("openShard"));
        assertFalse(raw("c5").containsKey("openUntil"));
        assertEquals(List.of(), store.openCartIds(Shards.of("c5", SHARDS), T).toList());
    }

    @Test
    void purchaseAsTheFirstEventCreatesAClosedRecord() {
        CartRecord r = store.applyEvent(new CartEvent.CartCleared("c6", "shopper-c6", 1, T), Arm.TREATMENT, 2).orElseThrow();
        assertEquals(CartStatus.CLOSED, r.status());
        assertEquals(List.of(), r.items());
        assertEquals(List.of(), r.sequenceStarts());
        assertEquals("shopper-c6", r.shopperKey());
    }

    @Test
    void markAbandonedAndEndSequenceAreConditionedOnVersionAndStatus() {
        CartRecord active = store.applyEvent(edit("c7", 1, T, ITEMS, null), Arm.TREATMENT, 0).orElseThrow();
        List<Instant> starts = List.of(T);
        assertTrue(store.markAbandoned(active, starts, true));
        assertFalse(store.markAbandoned(active, starts, true), "no longer ACTIVE");

        CartRecord abandoned = store.get("c7").orElseThrow();
        assertEquals(CartStatus.ABANDONED, abandoned.status());
        assertEquals(starts, abandoned.sequenceStarts());
        assertTrue(raw("c7").containsKey("openShard"), "eligible cart keeps its next step");

        assertFalse(store.endSequence("c7", 2), "wrong version");
        assertTrue(store.endSequence("c7", 1));
        assertFalse(raw("c7").containsKey("openShard"));
        assertEquals(CartStatus.ABANDONED, store.get("c7").orElseThrow().status());
        assertFalse(store.endSequence("c1-missing", 1));
    }

    @Test
    void ineligibleAbandonmentLeavesTheOpenIndex() {
        CartRecord active = store.applyEvent(edit("c8", 1, T, ITEMS, null), Arm.HOLDOUT, 0).orElseThrow();
        assertTrue(store.markAbandoned(active, List.of(T), false));
        assertFalse(raw("c8").containsKey("openShard"));
        assertFalse(raw("c8").containsKey("openUntil"));
        assertEquals(List.of(), store.openCartIds(Shards.of("c8", SHARDS), T).toList());
    }

    @Test
    void detectorWriteBetweenReloadAndAbandonmentWins() {
        CartRecord reloaded = store.applyEvent(edit("c9", 1, T, ITEMS, null), Arm.TREATMENT, 0).orElseThrow();
        store.applyEvent(edit("c9", 2, T.plusSeconds(5), ITEMS, null), Arm.TREATMENT, 0);
        assertFalse(store.markAbandoned(reloaded, List.of(T), true));
        CartRecord r = store.get("c9").orElseThrow();
        assertEquals(CartStatus.ACTIVE, r.status());
        assertEquals(2, r.version());
        assertEquals(List.of(), r.sequenceStarts(), "sequenceStarts untouched by the losing write");
    }

    @Test
    void itemsAreCappedAtFifty() {
        List<CartItem> many = IntStream.range(0, 60).mapToObj(i -> new CartItem("SKU-" + i, "Item " + i, 1, 100)).toList();
        store.applyEvent(edit("c10", 1, T, many, null), Arm.TREATMENT, 0);
        assertEquals(many.subList(0, 50), store.get("c10").orElseThrow().items());
    }

    @Test
    void getAllReadsManyCartsInInputOrderSkippingMissingAndDuplicates() {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 130; i++) {
            store.applyEvent(edit("g" + i, 1, T, ITEMS, null), Arm.TREATMENT, 0);
            ids.add("g" + i);
        }
        List<String> request = new ArrayList<>(ids);
        request.add("missing");
        request.add("g0");
        List<CartRecord> found = store.getAll(request);
        assertEquals(ids, found.stream().map(CartRecord::cartId).toList());
        assertEquals(List.of(), store.getAll(List.of()));
    }

    @Test
    void recordWrittenBeforeSrcPartitionExistedReadsMinusOne() {
        ddb.putItem(b -> b.tableName(table).item(Map.of(
                "cartId", AttributeValue.fromS("legacy"),
                "shopperKey", AttributeValue.fromS("s"),
                "status", AttributeValue.fromS("ACTIVE"),
                "version", AttributeValue.fromN("1"),
                "lastActivityAt", AttributeValue.fromN(Long.toString(T.toEpochMilli())),
                "items", AttributeValue.fromL(List.of()),
                "arm", AttributeValue.fromS("TREATMENT"),
                "sequenceStarts", AttributeValue.fromL(List.of()))));
        CartRecord r = store.get("legacy").orElseThrow();
        assertEquals(-1, r.srcPartition());
        assertNull(r.firstName());
        assertTrue(store.get("never-written").isEmpty());
    }
}
