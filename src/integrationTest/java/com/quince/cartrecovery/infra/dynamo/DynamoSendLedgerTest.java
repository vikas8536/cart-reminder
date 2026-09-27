package com.quince.cartrecovery.infra.dynamo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.Shards;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

@Testcontainers(disabledWithoutDocker = true)
class DynamoSendLedgerTest {
    private static final Instant T = Instant.parse("2026-01-01T09:00:00Z");
    private static final Instant SEND_BY = T.plusSeconds(600);
    private static final Duration LEASE = Duration.ofSeconds(90);
    private static final int SHARDS = 8;

    private final DynamoDbClient ddb = TestDynamo.client();
    private DynamoSendLedger ledger;

    @BeforeEach
    void setUp() {
        String table = TestDynamo.table("send-ledger");
        DynamoTables.createLedger(ddb, table);
        TestDynamo.noExpiry(table);
        ledger = new DynamoSendLedger(ddb, table, LEASE, SHARDS);
    }

    private static String key(String cartId, long version, int offset) {
        return new LedgerKey(cartId, version, offset).toString();
    }

    private ClaimResult.Claimed claimed(String key, Instant now) {
        return (ClaimResult.Claimed) ledger.claim(key, SEND_BY, 4, now);
    }

    @Test
    void takeoverAllowedExactlyAtLeaseUntil() {
        String key = key("c1", 7, 0);
        ClaimResult.Claimed first = claimed(key, T);
        assertEquals(1, first.attempts());
        assertEquals(T.plus(LEASE), first.leaseUntil());
        assertEquals(SEND_BY, first.sendBy());
        assertEquals(4, first.srcPartition());

        assertEquals(new ClaimResult.NotClaimed("leased"), ledger.claim(key, SEND_BY, 4, first.leaseUntil().minusMillis(1)));

        ClaimResult.Claimed second = (ClaimResult.Claimed) ledger.claim(key, Instant.EPOCH, 9, first.leaseUntil());
        assertEquals(2, second.attempts());
        assertNotEquals(first.token(), second.token());
        assertEquals(SEND_BY, second.sendBy(), "stored sendBy wins on takeover");
        assertEquals(4, second.srcPartition(), "stored srcPartition wins on takeover");

        assertFalse(ledger.finish(key, first.token(), OutcomeKind.SENT, null), "stale token is fenced");
        assertFalse(ledger.markRetry(key, first.token(), T), "stale token is fenced");
        assertTrue(ledger.finish(key, second.token(), OutcomeKind.SENT, null));
    }

    @Test
    void retryDueExactlyAtNextAttemptAt() {
        String cartId = "a:b|c";
        String key = key(cartId, 3, 1);
        int shard = Shards.of(cartId, SHARDS);
        ClaimResult.Claimed c = claimed(key, T);
        Instant next = T.plusSeconds(30);
        assertTrue(ledger.markRetry(key, c.token(), next));

        assertEquals(List.of(), ledger.dueRetries(shard, next.minusMillis(1), 10));
        assertEquals(List.of(new DueRetry(key, 4, SEND_BY)), ledger.dueRetries(shard, next, 10), "key with ':' and '|' round-trips");

        assertEquals(new ClaimResult.NotClaimed("leased"), ledger.claim(key, SEND_BY, 4, next.minusMillis(1)));
        ClaimResult.Claimed again = claimed(key, next);
        assertEquals(2, again.attempts());
        assertFalse(ledger.markRetry(key, c.token(), next), "the retrying holder's old token is dead");
    }

    @Test
    void sendingRowIsDueForTakeoverAtLeaseUntil() {
        String key = key("c2", 1, 0);
        ClaimResult.Claimed c = claimed(key, T);
        int shard = Shards.of("c2", SHARDS);
        assertEquals(List.of(), ledger.dueRetries(shard, c.leaseUntil().minusMillis(1), 10));
        assertEquals(List.of(new DueRetry(key, 4, SEND_BY)), ledger.dueRetries(shard, c.leaseUntil(), 10));
    }

    @Test
    void finalRowsLeaveTheRetryIndexAndStayFinal() {
        String key = key("c3", 1, 2);
        ClaimResult.Claimed c = claimed(key, T);
        assertTrue(ledger.finish(key, c.token(), OutcomeKind.CANCELLED, "purchased"));
        assertEquals(List.of(), ledger.dueRetries(Shards.of("c3", SHARDS), T.plus(Duration.ofDays(1)), 10));
        assertEquals(new ClaimResult.NotClaimed("final"), ledger.claim(key, SEND_BY, 4, T.plus(Duration.ofDays(1))));
        assertFalse(ledger.finish(key, c.token(), OutcomeKind.SENT, null));
        assertFalse(ledger.reopen(key, T), "only DEAD reopens");
    }

    @Test
    void reopenMovesDeadToRetryingDueNowWithAttemptsReset() {
        String key = key("c4", 2, 0);
        ClaimResult.Claimed c1 = claimed(key, T);
        assertTrue(ledger.markRetry(key, c1.token(), T.plusSeconds(10)));
        ClaimResult.Claimed c2 = claimed(key, T.plusSeconds(10));
        assertEquals(2, c2.attempts());
        assertTrue(ledger.finish(key, c2.token(), OutcomeKind.DEAD, "exhausted"));

        Instant replayAt = T.plusSeconds(100);
        assertTrue(ledger.reopen(key, replayAt));
        assertFalse(ledger.reopen(key, replayAt), "already RETRYING; replaying twice is harmless");
        assertEquals(List.of(new DueRetry(key, 4, SEND_BY)), ledger.dueRetries(Shards.of("c4", SHARDS), replayAt, 10));
        ClaimResult.Claimed c3 = claimed(key, replayAt);
        assertEquals(1, c3.attempts());
        assertEquals(SEND_BY, c3.sendBy());
    }

    @Test
    void dueRetriesHonoursTheLimit() {
        for (int i = 0; i < 5; i++) {
            String key = key("same-cart", 1, i);
            ClaimResult.Claimed c = claimed(key, T);
            ledger.markRetry(key, c.token(), T);
        }
        assertEquals(3, ledger.dueRetries(Shards.of("same-cart", SHARDS), T, 3).size());
        assertEquals(5, ledger.dueRetries(Shards.of("same-cart", SHARDS), T, 10).size());
        assertEquals(List.of(), ledger.dueRetries(Shards.of("same-cart", SHARDS), T, 0));
    }

    @Test
    void highestOffsetIndexIsPerVersion() {
        claimed(key("c5", 5, 0), T);
        claimed(key("c5", 5, 2), T);
        claimed(key("c5", 50, 1), T);
        assertEquals(2, ledger.highestOffsetIndex("c5", 5));
        assertEquals(1, ledger.highestOffsetIndex("c5", 50), "version 5 is not a prefix match for 50");
        assertEquals(-1, ledger.highestOffsetIndex("c5", 6));
        assertEquals(-1, ledger.highestOffsetIndex("nobody", 5));
    }

    @Test
    void finishRejectsANonLedgerOutcome() {
        String key = key("c6", 1, 0);
        ClaimResult.Claimed c = claimed(key, T);
        assertThrows(IllegalArgumentException.class, () -> ledger.finish(key, c.token(), OutcomeKind.ABANDONED, null));
    }

    /** Controller ruling F1: a table created before this change projects only srcPartition, not sendBy. */
    @Test
    void dueRetriesFailsClearlyWhenTheIndexDoesNotProjectSendBy() {
        String table = TestDynamo.table("send-ledger-old-projection");
        ddb.createTable(CreateTableRequest.builder()
                .tableName(table)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("cartId").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("retryShard").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("nextAttemptAt").attributeType(ScalarAttributeType.N).build())
                .keySchema(
                        KeySchemaElement.builder().attributeName("cartId").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build())
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                        .indexName(DynamoTables.RETRYING_BY_SHARD)
                        .keySchema(
                                KeySchemaElement.builder().attributeName("retryShard").keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName("nextAttemptAt").keyType(KeyType.RANGE).build())
                        .projection(Projection.builder().projectionType(ProjectionType.INCLUDE)
                                .nonKeyAttributes("srcPartition").build())
                        .build())
                .build());
        try (DynamoDbWaiter waiter = ddb.waiter()) {
            waiter.waitUntilTableExists(b -> b.tableName(table));
        }
        DynamoSendLedger oldLedger = new DynamoSendLedger(ddb, table, LEASE, SHARDS);
        String key = key("old", 1, 0);
        oldLedger.claim(key, SEND_BY, 4, T);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> oldLedger.dueRetries(Shards.of("old", SHARDS), T.plus(LEASE), 10));
        assertEquals("retrying-by-shard index does not project sendBy: recreate the send-ledger table", e.getMessage());
    }
}
