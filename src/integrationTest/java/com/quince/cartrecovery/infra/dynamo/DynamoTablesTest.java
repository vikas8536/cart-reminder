package com.quince.cartrecovery.infra.dynamo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus;

@Testcontainers(disabledWithoutDocker = true)
class DynamoTablesTest {
    private final DynamoDbClient ddb = TestDynamo.client();

    /**
     * Spec §6.3: no pinning, including inside the SDK. With apache-client (HttpClient 4, whose pool holds a monitor
     * while it waits for a connection) virtual threads contending for the pool pinned every carrier and the JVM
     * deadlocked, so this test hung. PinningGuard fails it on any pinning report.
     */
    @Test
    void clientDoesNotPinVirtualThreadsUnderPoolContention() throws Exception {
        ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();
        try (DynamoDbClient small = DynamoTables.client(TestDynamo.endpoint(), 2)) {
            List<Future<?>> calls = new ArrayList<>();
            for (int i = 0; i < 64; i++) calls.add(threads.submit(() -> small.listTables()));
            for (Future<?> f : calls) f.get(30, TimeUnit.SECONDS);
        } finally {
            threads.shutdownNow();
        }
    }

    @Test
    void cartsHasSparseKeysOnlyOpenIndexAndTtl() {
        String table = TestDynamo.table("carts");
        DynamoTables.createCarts(ddb, table);
        DynamoTables.createCarts(ddb, table); // idempotent

        GlobalSecondaryIndexDescription gsi = ddb.describeTable(b -> b.tableName(table)).table().globalSecondaryIndexes().get(0);
        assertEquals(DynamoTables.OPEN_BY_SHARD, gsi.indexName());
        assertEquals("openShard", gsi.keySchema().get(0).attributeName());
        assertEquals(KeyType.HASH, gsi.keySchema().get(0).keyType());
        assertEquals("openUntil", gsi.keySchema().get(1).attributeName());
        assertEquals(ProjectionType.KEYS_ONLY, gsi.projection().projectionType());
        assertEquals(TimeToLiveStatus.ENABLED,
                ddb.describeTimeToLive(b -> b.tableName(table)).timeToLiveDescription().timeToLiveStatus());
    }

    @Test
    void ledgerHasRetryIndexProjectingOnlySrcPartitionAndTtl() {
        String table = TestDynamo.table("send-ledger");
        DynamoTables.createLedger(ddb, table);
        DynamoTables.createLedger(ddb, table);

        var description = ddb.describeTable(b -> b.tableName(table)).table();
        assertEquals("cartId", description.keySchema().get(0).attributeName());
        assertEquals("sk", description.keySchema().get(1).attributeName());
        GlobalSecondaryIndexDescription gsi = description.globalSecondaryIndexes().get(0);
        assertEquals(DynamoTables.RETRYING_BY_SHARD, gsi.indexName());
        assertEquals("retryShard", gsi.keySchema().get(0).attributeName());
        assertEquals("nextAttemptAt", gsi.keySchema().get(1).attributeName());
        assertEquals(ProjectionType.INCLUDE, gsi.projection().projectionType());
        assertEquals(List.of("srcPartition"), gsi.projection().nonKeyAttributes());
        assertEquals(TimeToLiveStatus.ENABLED,
                ddb.describeTimeToLive(b -> b.tableName(table)).timeToLiveDescription().timeToLiveStatus());
    }

    @Test
    void createAllCreatesTheThreeProductionTablesIdempotently() {
        DynamoTables.createAll(ddb);
        DynamoTables.createAll(ddb);
        List<String> names = ddb.listTables().tableNames();
        assertTrue(names.containsAll(List.of(DynamoTables.CARTS, DynamoTables.SEND_LEDGER, DynamoTables.RECOVERY_META)), names.toString());
    }
}
