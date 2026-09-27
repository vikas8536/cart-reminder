package com.quince.cartrecovery.infra.dynamo;

import static com.quince.cartrecovery.infra.dynamo.Attrs.conditionalUpdate;
import static com.quince.cartrecovery.infra.dynamo.Attrs.instant;
import static com.quince.cartrecovery.infra.dynamo.Attrs.millis;
import static com.quince.cartrecovery.infra.dynamo.Attrs.n;
import static com.quince.cartrecovery.infra.dynamo.Attrs.names;
import static com.quince.cartrecovery.infra.dynamo.Attrs.num;
import static com.quince.cartrecovery.infra.dynamo.Attrs.s;
import static com.quince.cartrecovery.infra.dynamo.Attrs.str;

import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.SendLedger;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

/**
 * The {@code send-ledger} table (spec §5.3). Every claim is one conditional write with a fresh fencing token;
 * every later transition is conditioned on {@code status = SENDING AND leaseToken = :token}. Every non-final row
 * carries {@code retryShard}, so the sparse {@code retrying-by-shard} index holds exactly the rows a retry loop may take over.
 */
public final class DynamoSendLedger implements SendLedger {
    private static final String SENDING = "SENDING";
    private static final String RETRYING = "RETRYING";
    private static final Set<String> FINAL = Set.of("SENT", "SKIPPED_LATE", "CANCELLED", "DEAD");
    private static final Duration TTL = Duration.ofDays(30);
    private static final String OWNED = "#status = :sending AND #leaseToken = :token";

    private final DynamoDbClient ddb;
    private final String table;
    private final Duration lease;
    private final int shards;

    public DynamoSendLedger(DynamoDbClient ddb, String table, Duration lease, int shards) {
        this.ddb = ddb;
        this.table = table;
        this.lease = lease;
        this.shards = shards;
    }

    /** Sort key: version as 20 digits, '#', offset as 2 digits, so begins_with("<version>#") never matches a longer version. */
    public static String sk(long version, int offsetIndex) {
        return String.format("%020d#%02d", version, offsetIndex);
    }

    @Override
    public ClaimResult claim(String key, Instant sendBy, int srcPartition, Instant now) {
        Objects.requireNonNull(sendBy, "sendBy");
        LedgerKey k = LedgerKey.parse(key);
        String token = UUID.randomUUID().toString();
        Instant leaseUntil = now.plus(lease);
        String update = "SET #status = :sending, #leaseToken = :token, #leaseUntil = :leaseUntil, "
                + "#nextAttemptAt = :leaseUntil, #retryShard = :retryShard, "
                + "#sendBy = if_not_exists(#sendBy, :sendBy), #srcPartition = if_not_exists(#srcPartition, :srcPartition), "
                + "#ttl = if_not_exists(#ttl, :ttl), #attempts = if_not_exists(#attempts, :zero) + :one "
                + "REMOVE #reason";
        String condition = "attribute_not_exists(#cartId) "
                + "OR (#status = :retrying AND #nextAttemptAt <= :now) "
                + "OR (#status = :sending AND #leaseUntil <= :now)";
        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":sending", s(SENDING));
        v.put(":retrying", s(RETRYING));
        v.put(":token", s(token));
        v.put(":leaseUntil", millis(leaseUntil));
        v.put(":retryShard", s(retryShard(k.cartId())));
        v.put(":sendBy", millis(sendBy));
        v.put(":srcPartition", n(srcPartition));
        v.put(":ttl", n(now.plus(TTL).getEpochSecond()));
        v.put(":zero", n(0));
        v.put(":one", n(1));
        v.put(":now", millis(now));
        try {
            Map<String, AttributeValue> row = ddb.updateItem(b -> b.tableName(table).key(key(k))
                    .updateExpression(update).conditionExpression(condition)
                    .expressionAttributeNames(names(update, condition)).expressionAttributeValues(v)
                    .returnValues(ReturnValue.ALL_NEW)).attributes();
            return new ClaimResult.Claimed(token, (int) num(row, "attempts"), instant(row, "sendBy"),
                    (int) num(row, "srcPartition"), leaseUntil);
        } catch (ConditionalCheckFailedException e) {
            // Rare path; one consistent read tells a final row from one that is leased or not yet due.
            String status = str(row(k), "status");
            return new ClaimResult.NotClaimed(status != null && FINAL.contains(status) ? "final" : "leased");
        }
    }

    @Override
    public boolean markRetry(String key, String token, Instant nextAt) {
        return conditionalUpdate(ddb, table, key(LedgerKey.parse(key)),
                "SET #status = :retrying, #nextAttemptAt = :next REMOVE #leaseToken, #leaseUntil", OWNED,
                Map.of(":retrying", s(RETRYING), ":next", millis(nextAt), ":sending", s(SENDING), ":token", s(token)));
    }

    @Override
    public boolean finish(String key, String token, OutcomeKind outcome, String reason) {
        if (!FINAL.contains(outcome.name())) throw new IllegalArgumentException("not a final ledger status: " + outcome);
        String update = "SET #status = :final" + (reason == null ? "" : ", #reason = :reason")
                + " REMOVE #leaseToken, #leaseUntil, #retryShard, #nextAttemptAt";
        Map<String, AttributeValue> v = new HashMap<>(Map.of(":final", s(outcome.name()), ":sending", s(SENDING), ":token", s(token)));
        if (reason != null) v.put(":reason", s(reason));
        return conditionalUpdate(ddb, table, key(LedgerKey.parse(key)), update, OWNED, v);
    }

    /** Eventually consistent index read; the conditional claim settles races between retry pollers. */
    @Override
    public List<DueRetry> dueRetries(int shard, Instant now, int limit) {
        if (limit <= 0) return List.of();
        String kc = "#retryShard = :retryShard AND #nextAttemptAt <= :now";
        QueryRequest q = QueryRequest.builder().tableName(table).indexName(DynamoTables.RETRYING_BY_SHARD)
                .keyConditionExpression(kc).expressionAttributeNames(names(kc))
                .expressionAttributeValues(Map.of(":retryShard", s("s#" + shard), ":now", millis(now)))
                .limit(limit)
                .build();
        return ddb.queryPaginator(q).items().stream().limit(limit)
                .map(row -> {
                    if (!row.containsKey("sendBy")) {
                        throw new IllegalStateException(
                                "retrying-by-shard index does not project sendBy: recreate the send-ledger table");
                    }
                    return new DueRetry(keyOf(row), (int) num(row, "srcPartition"), instant(row, "sendBy"));
                })
                .toList();
    }

    @Override
    public boolean reopen(String key, Instant now) {
        LedgerKey k = LedgerKey.parse(key);
        return conditionalUpdate(ddb, table, key(k),
                "SET #status = :retrying, #nextAttemptAt = :now, #attempts = :zero, #retryShard = :retryShard REMOVE #reason",
                "#status = :dead",
                Map.of(":retrying", s(RETRYING), ":now", millis(now), ":zero", n(0),
                        ":retryShard", s(retryShard(k.cartId())), ":dead", s("DEAD")));
    }

    @Override
    public int highestOffsetIndex(String cartId, long version) {
        String kc = "#cartId = :cartId AND begins_with(#sk, :prefix)";
        QueryResponse r = ddb.query(b -> b.tableName(table).keyConditionExpression(kc).expressionAttributeNames(names(kc))
                .expressionAttributeValues(Map.of(":cartId", s(cartId), ":prefix", s(String.format("%020d#", version))))
                .scanIndexForward(false).limit(1).consistentRead(true));
        return r.items().isEmpty() ? -1 : Integer.parseInt(str(r.items().get(0), "sk").substring(21));
    }

    private String retryShard(String cartId) {
        return "s#" + Shards.of(cartId, shards);
    }

    private Map<String, AttributeValue> row(LedgerKey k) {
        return ddb.getItem(b -> b.tableName(table).key(key(k)).consistentRead(true)).item();
    }

    private static Map<String, AttributeValue> key(LedgerKey k) {
        return Map.of("cartId", s(k.cartId()), "sk", s(sk(k.version(), k.offsetIndex())));
    }

    private static String keyOf(Map<String, AttributeValue> row) {
        String sk = str(row, "sk");
        int hash = sk.indexOf('#');
        return new LedgerKey(str(row, "cartId"), Long.parseLong(sk.substring(0, hash)),
                Integer.parseInt(sk.substring(hash + 1))).toString();
    }
}
