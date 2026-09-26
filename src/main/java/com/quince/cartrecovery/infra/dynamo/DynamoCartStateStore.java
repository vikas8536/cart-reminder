package com.quince.cartrecovery.infra.dynamo;

import static com.quince.cartrecovery.infra.dynamo.Attrs.backoff;
import static com.quince.cartrecovery.infra.dynamo.Attrs.conditionalUpdate;
import static com.quince.cartrecovery.infra.dynamo.Attrs.instant;
import static com.quince.cartrecovery.infra.dynamo.Attrs.instantList;
import static com.quince.cartrecovery.infra.dynamo.Attrs.instants;
import static com.quince.cartrecovery.infra.dynamo.Attrs.millis;
import static com.quince.cartrecovery.infra.dynamo.Attrs.n;
import static com.quince.cartrecovery.infra.dynamo.Attrs.names;
import static com.quince.cartrecovery.infra.dynamo.Attrs.num;
import static com.quince.cartrecovery.infra.dynamo.Attrs.s;
import static com.quince.cartrecovery.infra.dynamo.Attrs.str;

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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

/**
 * The {@code carts} table (spec §5.3): field-scoped conditional UpdateItem, never read-modify-write.
 * openShard/openUntil form the sparse {@code open-by-shard} index and are present only while the cart has a next step.
 */
public final class DynamoCartStateStore implements CartStateStore {
    static final int MAX_ITEMS = 50;
    private static final int BATCH_GET = 100;
    private static final Duration TTL = Duration.ofDays(30);
    private static final String NEWER = "attribute_not_exists(#cartId) OR #version < :v";

    private final DynamoDbClient ddb;
    private final String table;
    private final RecoveryConfig config;
    private final int shards;

    public DynamoCartStateStore(DynamoDbClient ddb, String table, RecoveryConfig config, int shards) {
        this.ddb = ddb;
        this.table = table;
        this.config = config;
        this.shards = shards;
    }

    /** lastActivityAt + last offset + its lateness bound, rounded up to the next whole hour. */
    public static Instant openUntil(Instant lastActivityAt, RecoveryConfig config) {
        int last = config.offsets().size() - 1;
        Instant t = lastActivityAt.plus(config.offsets().get(last)).plus(config.latenessBounds().get(last));
        Instant hour = t.truncatedTo(ChronoUnit.HOURS);
        return hour.equals(t) ? t : hour.plus(Duration.ofHours(1));
    }

    @Override
    public Optional<CartRecord> get(String cartId) {
        Map<String, AttributeValue> item = ddb.getItem(b -> b.tableName(table).key(key(cartId)).consistentRead(true)).item();
        return item == null || item.isEmpty() ? Optional.empty() : Optional.of(toRecord(item));
    }

    @Override
    public List<CartRecord> getAll(Collection<String> cartIds) {
        List<String> ids = List.copyOf(new LinkedHashSet<>(cartIds)); // BatchGetItem rejects duplicate keys
        Map<String, CartRecord> found = new HashMap<>();
        for (int i = 0; i < ids.size(); i += BATCH_GET) {
            List<Map<String, AttributeValue>> keys = ids.subList(i, Math.min(ids.size(), i + BATCH_GET)).stream()
                    .map(DynamoCartStateStore::key).toList();
            Map<String, KeysAndAttributes> pending =
                    Map.of(table, KeysAndAttributes.builder().keys(keys).consistentRead(true).build());
            for (int round = 0; !pending.isEmpty(); round++) {
                if (round > 0) backoff(round);
                BatchGetItemResponse r = ddb.batchGetItem(BatchGetItemRequest.builder().requestItems(pending).build());
                r.responses().getOrDefault(table, List.of()).forEach(item -> found.put(str(item, "cartId"), toRecord(item)));
                pending = r.unprocessedKeys();
            }
        }
        return ids.stream().map(found::get).filter(Objects::nonNull).toList();
    }

    @Override
    public Optional<CartRecord> applyEvent(CartEvent event, Arm arm, int srcPartition) {
        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":v", n(event.version()));
        v.put(":at", millis(event.occurredAt()));
        v.put(":sp", n(srcPartition));
        v.put(":sk", s(event.shopperKey()));
        v.put(":arm", s(arm.name()));
        v.put(":empty", AttributeValue.fromL(List.of()));
        v.put(":ttl", n(event.occurredAt().plus(TTL).getEpochSecond()));
        List<String> set = new ArrayList<>(List.of(
                "#version = :v", "#lastActivityAt = :at", "#srcPartition = :sp", "#ttl = :ttl",
                "#shopperKey = if_not_exists(#shopperKey, :sk)", "#arm = if_not_exists(#arm, :arm)",
                "#sequenceStarts = if_not_exists(#sequenceStarts, :empty)", "#status = :status"));
        List<String> remove = new ArrayList<>();
        if (event instanceof CartEvent.CartPurchased || event instanceof CartEvent.CartCleared) {
            v.put(":status", s(CartStatus.CLOSED.name()));
            set.add("#items = if_not_exists(#items, :empty)");
            remove.add("#openShard");
            remove.add("#openUntil");
        } else {
            v.put(":status", s(CartStatus.ACTIVE.name()));
            v.put(":shard", s("s#" + Shards.of(event.cartId(), shards)));
            v.put(":until", millis(openUntil(event.occurredAt(), config)));
            set.add("#openShard = :shard");
            set.add("#openUntil = :until");
            if (event instanceof CartEvent.CartEdited e) {
                v.put(":items", items(e.items()));
                set.add("#items = :items");
                if (e.firstName() != null) {   // an edit without a name keeps the stored one (controller ruling R4)
                    v.put(":fn", s(e.firstName()));
                    set.add("#firstName = :fn");
                }
            } else {
                set.add("#items = if_not_exists(#items, :empty)");
            }
        }
        String update = "SET " + String.join(", ", set) + (remove.isEmpty() ? "" : " REMOVE " + String.join(", ", remove));
        try {
            Map<String, AttributeValue> item = ddb.updateItem(b -> b.tableName(table).key(key(event.cartId()))
                    .updateExpression(update).conditionExpression(NEWER)
                    .expressionAttributeNames(names(update, NEWER)).expressionAttributeValues(v)
                    .returnValues(ReturnValue.ALL_NEW)).attributes();
            return Optional.of(toRecord(item));
        } catch (ConditionalCheckFailedException staleOrDuplicate) {
            return Optional.empty();
        }
    }

    @Override
    public boolean markAbandoned(CartRecord record, List<Instant> sequenceStarts, boolean eligible) {
        String update = "SET #status = :abandoned, #sequenceStarts = :starts" + (eligible ? "" : " REMOVE #openShard, #openUntil");
        return conditionalUpdate(ddb, table, key(record.cartId()), update, "#version = :v AND #status = :active", Map.of(
                ":abandoned", s(CartStatus.ABANDONED.name()),
                ":starts", instants(sequenceStarts),
                ":v", n(record.version()),
                ":active", s(CartStatus.ACTIVE.name())));
    }

    @Override
    public boolean endSequence(String cartId, long version) {
        return conditionalUpdate(ddb, table, key(cartId), "REMOVE #openShard, #openUntil", "#version = :v AND #status = :abandoned",
                Map.of(":v", n(version), ":abandoned", s(CartStatus.ABANDONED.name())));
    }

    /** Eventually consistent index read; every consumer reloads or conditions its write (spec §5.3). */
    @Override
    public Stream<String> openCartIds(int shard, Instant now) {
        String kc = "#openShard = :shard AND #openUntil >= :now";
        QueryRequest q = QueryRequest.builder().tableName(table).indexName(DynamoTables.OPEN_BY_SHARD)
                .keyConditionExpression(kc).expressionAttributeNames(names(kc))
                .expressionAttributeValues(Map.of(":shard", s("s#" + shard), ":now", millis(now)))
                .build();
        return ddb.queryPaginator(q).items().stream().map(item -> str(item, "cartId"));
    }

    private static Map<String, AttributeValue> key(String cartId) {
        return Map.of("cartId", s(cartId));
    }

    private static AttributeValue items(List<CartItem> items) {
        return AttributeValue.fromL(items.stream().limit(MAX_ITEMS).map(DynamoCartStateStore::item).toList());
    }

    private static AttributeValue item(CartItem i) {
        Map<String, AttributeValue> m = new HashMap<>();
        if (i.sku() != null) m.put("sku", s(i.sku()));
        if (i.name() != null) m.put("name", s(i.name()));
        m.put("quantity", n(i.quantity()));
        m.put("priceCents", n(i.priceCents()));
        return AttributeValue.fromM(m);
    }

    private static List<CartItem> itemsOf(AttributeValue v) {
        if (v == null) return List.of();
        return v.l().stream().map(AttributeValue::m)
                .map(m -> new CartItem(str(m, "sku"), str(m, "name"), (int) num(m, "quantity"), num(m, "priceCents")))
                .toList();
    }

    static CartRecord toRecord(Map<String, AttributeValue> i) {
        return new CartRecord(str(i, "cartId"), str(i, "shopperKey"), CartStatus.valueOf(str(i, "status")), num(i, "version"),
                instant(i, "lastActivityAt"), itemsOf(i.get("items")), Arm.valueOf(str(i, "arm")),
                instantList(i.get("sequenceStarts")), str(i, "firstName"),
                i.containsKey("srcPartition") ? (int) num(i, "srcPartition") : -1);
    }
}
