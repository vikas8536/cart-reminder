package com.quince.cartrecovery.app;

import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.TopicAdmin;
import com.quince.cartrecovery.infra.kafka.Topics;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** The clients one role process shares, the startup check every role runs, and the loop runner. */
public final class RoleContext implements AutoCloseable {
    public static final String ARM_SALT = "cart-recovery-v1";

    private final InfraConfig config;
    private final DynamoDbClient dynamo;
    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> redis;
    private final KafkaProducer<String, byte[]> producer;
    private final Admin admin;

    public RoleContext(InfraConfig config) {
        this.config = config;
        this.dynamo = DynamoTables.client(config.dynamoEndpoint(), config.maxInFlight());   // spec §6.3 client sizing
        this.redisClient = RedisClient.create(config.redisUrl());
        this.redis = redisClient.connect();
        this.producer = new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap(),
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
            ProducerConfig.LINGER_MS_CONFIG, 5,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
        this.admin = Admin.create(Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap()));
    }

    public InfraConfig config() { return config; }
    public DynamoDbClient dynamo() { return dynamo; }
    public StatefulRedisConnection<String, String> redis() { return redis; }
    public Producer<String, byte[]> producer() { return producer; }
    public Admin admin() { return admin; }

    /** Classic group protocol (watermark fencing relies on classic generations), manual commits. */
    public Map<String, Object> consumerProps(String groupId) {
        return Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap(),
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    }

    /**
     * Refuses to start when S or P differ from recovery-meta or any topic's partition count differs from P
     * (spec §7.1), then loads producer metadata for every topic on this platform thread so virtual threads
     * never wait for metadata inside the producer's monitor.
     */
    public void verifyStartup() {
        RecoveryMetaStore.Meta meta;
        try {
            meta = new RecoveryMetaStore(dynamo).read();
        } catch (IllegalStateException missing) {   // init has not run
            meta = null;
        }
        Map<String, Integer> counts = new TopicAdmin(admin).partitionCounts();
        startupProblem(config.shards(), config.partitions(),
                meta == null ? null : meta.shards(), meta == null ? null : meta.partitions(), counts)
            .ifPresent(problem -> { throw new IllegalStateException(problem); });
        for (String topic : Topics.ALL) producer.partitionsFor(topic);
    }

    static Optional<String> startupProblem(int shards, int partitions, Integer metaShards, Integer metaPartitions,
                                           Map<String, Integer> topicPartitions) {
        if (metaShards == null || metaPartitions == null) return Optional.of("recovery-meta missing: run --role=init first");
        if (metaShards != shards) return Optional.of("SHARDS=" + shards + " differs from recovery-meta shards=" + metaShards);
        if (metaPartitions != partitions)
            return Optional.of("PARTITIONS=" + partitions + " differs from recovery-meta partitions=" + metaPartitions);
        for (String topic : Topics.ALL) {
            Integer n = topicPartitions.get(topic);
            if (n == null) return Optional.of("topic " + topic + " missing: run --role=init first");
            if (n != partitions) return Optional.of("topic " + topic + " has " + n + " partitions, PARTITIONS=" + partitions);
        }
        return Optional.empty();
    }

    /**
     * Runs each loop on its own platform thread. Returns after stopping all loops when the calling thread is
     * interrupted (SIGTERM); throws if a loop ends on its own, so the process exits and is restarted.
     */
    public static void runLoops(Runnable stop, List<Runnable> loops) {
        CountDownLatch anyEnded = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < loops.size(); i++) {
            Runnable loop = loops.get(i);
            threads.add(Thread.ofPlatform().name("loop-" + i).start(() -> {
                try {
                    loop.run();
                } catch (Throwable e) {
                    System.err.println(Thread.currentThread().getName() + " failed: " + e);
                } finally {
                    anyEnded.countDown();
                }
            }));
        }
        boolean interrupted = false;
        try {
            anyEnded.await();
        } catch (InterruptedException e) {
            interrupted = true;
        }
        stop.run();
        for (Thread t : threads) {
            try {
                t.join(Duration.ofSeconds(35));
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
            return;
        }
        throw new IllegalStateException("a role loop ended unexpectedly");
    }

    /** Sleeps; false if interrupted (the interrupt flag is restored). */
    public static boolean sleep(Duration d) {
        try {
            Thread.sleep(d);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** Clears the thread's interrupt (SIGTERM) while closing so the producer flushes and joins cleanly, then restores it. */
    @Override
    public void close() {
        boolean interrupted = Thread.interrupted();
        try {
            producer.close(Duration.ofSeconds(5));
            admin.close(Duration.ofSeconds(5));
            redis.close();
            redisClient.shutdown();
            dynamo.close();
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }
}
