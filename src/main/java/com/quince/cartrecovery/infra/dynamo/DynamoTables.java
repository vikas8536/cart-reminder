package com.quince.cartrecovery.infra.dynamo;

import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

/** Table and index definitions (spec §5.3) and the shared client factory. */
public final class DynamoTables {
    public static final String CARTS = "carts";
    public static final String SEND_LEDGER = "send-ledger";
    public static final String RECOVERY_META = "recovery-meta";
    public static final String OPEN_BY_SHARD = "open-by-shard";
    public static final String RETRYING_BY_SHARD = "retrying-by-shard";

    private DynamoTables() {}

    /**
     * maxConnections should match MAX_IN_FLIGHT: the SDK default of 50 caps a JVM near 5k events per second.
     * A non-blank endpoint means DynamoDB Local, which ignores region and credentials.
     * Apache HttpClient 5: the 4.x pool behind apache-client holds monitors while it waits for a connection or
     * drains a response, which pins virtual threads (spec §6.3 forbids any pinning report).
     */
    public static DynamoDbClient client(String endpoint, int maxConnections) {
        DynamoDbClientBuilder b = DynamoDbClient.builder()
                .httpClientBuilder(Apache5HttpClient.builder().maxConnections(maxConnections));
        if (endpoint != null && !endpoint.isBlank()) {
            b.endpointOverride(URI.create(endpoint))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")));
        }
        return b.build();
    }

    public static void createAll(DynamoDbClient ddb) {
        createCarts(ddb, CARTS);
        createLedger(ddb, SEND_LEDGER);
        createMeta(ddb, RECOVERY_META);
    }

    public static void createCarts(DynamoDbClient ddb, String table) {
        create(ddb, CreateTableRequest.builder()
                .tableName(table)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(attr("cartId", ScalarAttributeType.S), attr("openShard", ScalarAttributeType.S),
                        attr("openUntil", ScalarAttributeType.N))
                .keySchema(key("cartId", KeyType.HASH))
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                        .indexName(OPEN_BY_SHARD)
                        .keySchema(key("openShard", KeyType.HASH), key("openUntil", KeyType.RANGE))
                        .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                        .build())
                .build(), true);
    }

    public static void createLedger(DynamoDbClient ddb, String table) {
        create(ddb, CreateTableRequest.builder()
                .tableName(table)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(attr("cartId", ScalarAttributeType.S), attr("sk", ScalarAttributeType.S),
                        attr("retryShard", ScalarAttributeType.S), attr("nextAttemptAt", ScalarAttributeType.N))
                .keySchema(key("cartId", KeyType.HASH), key("sk", KeyType.RANGE))
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                        .indexName(RETRYING_BY_SHARD)
                        .keySchema(key("retryShard", KeyType.HASH), key("nextAttemptAt", KeyType.RANGE))
                        .projection(Projection.builder().projectionType(ProjectionType.INCLUDE)
                                .nonKeyAttributes("srcPartition").build())
                        .build())
                .build(), true);
    }

    public static void createMeta(DynamoDbClient ddb, String table) {
        create(ddb, CreateTableRequest.builder()
                .tableName(table)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(attr("id", ScalarAttributeType.S))
                .keySchema(key("id", KeyType.HASH))
                .build(), false);
    }

    private static void create(DynamoDbClient ddb, CreateTableRequest request, boolean ttl) {
        String table = request.tableName();
        try {
            ddb.createTable(request);
        } catch (ResourceInUseException alreadyExists) {
            // idempotent: init may run more than once
        }
        try (DynamoDbWaiter waiter = ddb.waiter()) {
            waiter.waitUntilTableExists(b -> b.tableName(table));
        }
        if (ttl) {
            TimeToLiveStatus status = ddb.describeTimeToLive(b -> b.tableName(table)).timeToLiveDescription().timeToLiveStatus();
            if (status != TimeToLiveStatus.ENABLED && status != TimeToLiveStatus.ENABLING) {
                ddb.updateTimeToLive(b -> b.tableName(table).timeToLiveSpecification(t -> t.enabled(true).attributeName("ttl")));
            }
        }
    }

    private static AttributeDefinition attr(String name, ScalarAttributeType type) {
        return AttributeDefinition.builder().attributeName(name).attributeType(type).build();
    }

    private static KeySchemaElement key(String name, KeyType type) {
        return KeySchemaElement.builder().attributeName(name).keyType(type).build();
    }
}
