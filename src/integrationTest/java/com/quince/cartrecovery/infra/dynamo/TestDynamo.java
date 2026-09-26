package com.quince.cartrecovery.infra.dynamo;

import java.util.UUID;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** One DynamoDB Local container and client shared by every integration test in the JVM, started on first use. */
public final class TestDynamo {
    private static GenericContainer<?> container;
    private static DynamoDbClient client;

    private TestDynamo() {}

    public static synchronized DynamoDbClient client() {
        if (client == null) {
            container = new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local:3.3.1"))
                    .withExposedPorts(8000)
                    .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");
            container.start();
            client = DynamoTables.client(endpoint(), 64);
        }
        return client;
    }

    public static synchronized String endpoint() {
        if (container == null) client();
        return "http://" + container.getHost() + ":" + container.getMappedPort(8000);
    }

    /** A fresh table name per test, so tests never see each other's rows or index entries. */
    public static String table(String base) {
        return base + "-" + UUID.randomUUID();
    }

    /**
     * Turns TTL off on a test table. Fixtures use fixed 2026 instants whose spec-mandated ttl (+30 days) is already
     * past, and DynamoDB Local's sweeper deletes expired rows mid-test.
     */
    public static String noExpiry(String table) {
        client().updateTimeToLive(b -> b.tableName(table).timeToLiveSpecification(t -> t.enabled(false).attributeName("ttl")));
        return table;
    }
}
