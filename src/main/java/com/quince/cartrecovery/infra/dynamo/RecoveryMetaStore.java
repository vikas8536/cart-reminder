package com.quince.cartrecovery.infra.dynamo;

import static com.quince.cartrecovery.infra.dynamo.Attrs.instant;
import static com.quince.cartrecovery.infra.dynamo.Attrs.millis;
import static com.quince.cartrecovery.infra.dynamo.Attrs.n;
import static com.quince.cartrecovery.infra.dynamo.Attrs.names;
import static com.quince.cartrecovery.infra.dynamo.Attrs.num;
import static com.quince.cartrecovery.infra.dynamo.Attrs.s;
import static com.quince.cartrecovery.infra.dynamo.Attrs.str;

import java.time.Instant;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * The single {@code recovery-meta} item (spec §5.3): S, P, the guardrail pause switch, and the last seen Redis
 * identity with the earliest unrepaired Redis change (the reconciler's failover replay restarts from it).
 */
public final class RecoveryMetaStore {
    public record Meta(int shards, int partitions, boolean paused, String redisRunId, String redisRole, Instant redisChangeAt) {}

    private static final Map<String, AttributeValue> KEY = Map.of("id", s("meta"));

    private final DynamoDbClient ddb;
    private final String table;

    public RecoveryMetaStore(DynamoDbClient ddb) {
        this(ddb, DynamoTables.RECOVERY_META);
    }

    /** Tests use a fresh table per case. */
    RecoveryMetaStore(DynamoDbClient ddb, String table) {
        this.ddb = ddb;
        this.table = table;
    }

    /** Creates the {@code recovery-meta} table if absent and waits until it is active. */
    public static void createTable(DynamoDbClient ddb) {
        DynamoTables.createMeta(ddb, DynamoTables.RECOVERY_META);
    }

    /** Writes S and P only if absent and never overwrites them; callers compare {@link #read()} with their config. */
    public void init(int shards, int partitions) {
        set("SET #shards = if_not_exists(#shards, :shards), #partitions = if_not_exists(#partitions, :partitions), "
                + "#paused = if_not_exists(#paused, :false)",
                Map.of(":shards", n(shards), ":partitions", n(partitions), ":false", AttributeValue.fromBool(false)));
    }

    /** Consistent read; throws {@link IllegalStateException} when init has not run. */
    public Meta read() {
        Map<String, AttributeValue> i = ddb.getItem(b -> b.tableName(table).key(KEY).consistentRead(true)).item();
        if (i == null || !i.containsKey("shards")) throw new IllegalStateException("recovery-meta missing: run --role=init first");
        return new Meta((int) num(i, "shards"), (int) num(i, "partitions"),
                i.containsKey("paused") && i.get("paused").bool(),
                str(i, "redisRunId"), str(i, "redisRole"),
                i.containsKey("redisChangeAt") ? instant(i, "redisChangeAt") : null);
    }

    public void setPaused(boolean paused) {
        set("SET #paused = :paused", Map.of(":paused", AttributeValue.fromBool(paused)));
    }

    /** Stores the Redis identity (on first sight or after a completed failover replay) and clears the pending change. */
    public void setRedisIdentity(String runId, String role) {
        set("SET #redisRunId = :runId, #redisRole = :role REMOVE #redisChangeAt", Map.of(":runId", s(runId), ":role", s(role)));
    }

    /** Records a detected Redis restart or failover; keeps the earliest unrepaired change. */
    public void markRedisChange(Instant at) {
        set("SET #redisChangeAt = if_not_exists(#redisChangeAt, :at)", Map.of(":at", millis(at)));
    }

    private void set(String update, Map<String, AttributeValue> values) {
        ddb.updateItem(b -> b.tableName(table).key(KEY).updateExpression(update)
                .expressionAttributeNames(names(update)).expressionAttributeValues(values));
    }
}
