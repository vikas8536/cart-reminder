package com.quince.cartrecovery.app;

import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.TopicAdmin;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.infra.redis.RedisMeta;
import com.quince.cartrecovery.model.CartEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/** Containers shared by every role and end-to-end test in the JVM, started once, plus demo-scale config. */
public final class RoleInfra {
    public static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"))
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
    public static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
        .withCommand("redis-server", "--appendonly", "yes")
        .withExposedPorts(6379);
    public static final GenericContainer<?> DYNAMO = new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local:3.3.1"))
        .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
        .withExposedPorts(8000);

    private static RoleContext ctx;

    private RoleInfra() {}

    /** Starts the containers and creates topics, tables, the meta item and the epoch, once per JVM. */
    public static synchronized void start() {
        if (ctx != null) return;
        Startables.deepStart(KAFKA, REDIS, DYNAMO).join();
        ctx = new RoleContext(config(Map.of()));
        new TopicAdmin(ctx.admin()).createAll(8, 1, 1);
        DynamoTables.createAll(ctx.dynamo());   // carts, send-ledger, recovery-meta
        new RecoveryMetaStore(ctx.dynamo()).init(8, 8);
        new RedisMeta(ctx.redis()).writeEpoch();
    }

    public static RoleContext ctx() { return ctx; }

    public static String bootstrap() { return KAFKA.getBootstrapServers(); }

    /** Demo-scale config against the shared containers; overrides replace single variables. */
    public static InfraConfig config(Map<String, String> overrides) {
        Map<String, String> env = new HashMap<>();
        env.put("KAFKA_BOOTSTRAP", KAFKA.getBootstrapServers());
        env.put("REDIS_URL", "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        env.put("DYNAMO_ENDPOINT", "http://" + DYNAMO.getHost() + ":" + DYNAMO.getMappedPort(8000));
        env.put("SHARDS", "8");
        env.put("PARTITIONS", "8");
        env.put("REPLICATION_FACTOR", "1");
        env.put("MIN_INSYNC_REPLICAS", "1");
        env.put("WINDOW", "PT2S");
        env.put("OFFSETS", "PT3S,PT6S,PT9S");
        env.put("LATENESS_BOUNDS", "PT20S,PT20S,PT20S");
        env.put("FREQUENCY_CAP", "3");
        env.put("FREQUENCY_WINDOW", "P7D");
        env.put("HOLDOUT_PERCENT", "0");
        env.put("MAX_SEND_ATTEMPTS", "5");
        env.put("RETRY_BASE", "PT0.2S");
        env.put("FAST_OFFSETS", "2");
        env.put("LEASE", "PT3S");
        env.put("GATEWAY_TIMEOUT", "PT1S");
        // The detector's watermark trails Redis TIME by one or two poll iterations (spec §5.4); a smaller
        // skew never lets the dispatcher gate open (controller ruling). C1b's DispatcherRoleIT already
        // overrides to the same value.
        env.put("CLOCK_SKEW", "PT2S");
        env.put("MAX_SEND_RATE", "500");
        env.put("FAST_RESERVE", "0.3");
        env.put("SEND_FAILURE_RATE", "0");
        env.put("RECONCILE_INTERVAL", "PT10M");
        env.put("RETRY_POLL", "PT0.2S");
        env.put("MAX_IN_FLIGHT", "64");
        env.put("HEALTH_PORT", "18080");
        env.putAll(overrides);
        return InfraConfig.fromEnv(env);
    }

    /** A unique cart-id prefix per test, so shared topics and tables can be filtered. */
    public static String prefix(String test) {
        return test + "-" + UUID.randomUUID().toString().substring(0, 8) + "-";
    }

    public static RecordMetadata produce(CartEvent event) {
        return send(Topics.CART_EVENTS, event.cartId(), JsonCodec.encode(event));
    }

    public static RecordMetadata send(String topic, String key, byte[] value) {
        try {
            return ctx.producer().send(new ProducerRecord<>(topic, key, value)).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
