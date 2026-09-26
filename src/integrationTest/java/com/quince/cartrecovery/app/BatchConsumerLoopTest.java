package com.quince.cartrecovery.app;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.app.BatchConsumerLoop.Verdict;
import com.quince.cartrecovery.core.Metrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class BatchConsumerLoopTest {
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));
    static final Duration WAIT = Duration.ofSeconds(30);

    private static Admin admin;
    private static KafkaProducer<String, String> producer;
    private static KafkaProducer<String, byte[]> dlqProducer;

    private final List<BatchConsumerLoop<String>> loops = new ArrayList<>();
    private final Metrics metrics = new Metrics();
    private final Health health = new Health();
    private final String group = "g-" + UUID.randomUUID();

    @BeforeAll
    static void clients() {
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
        Map<String, Object> props = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
            ProducerConfig.ACKS_CONFIG, "all");
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        dlqProducer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
    }

    @AfterAll
    static void closeClients() {
        producer.close();
        dlqProducer.close();
        admin.close();
    }

    @AfterEach
    void closeLoops() {
        loops.forEach(BatchConsumerLoop::close);
    }

    @Test
    void recordsWithinAKeyRunInOrderAndEveryPartitionIsCommitted() throws Exception {
        String topic = topic(4);
        for (int i = 0; i < 20; i++)
            for (int k = 0; k < 10; k++) send(topic, "k" + k, "k" + k + ":" + i);
        Map<String, List<Integer>> seen = new ConcurrentHashMap<>();
        start(loop(topic, 50, 16, null, new StringDeserializer(), r -> {
            Thread.sleep(ThreadLocalRandom.current().nextInt(3));
            seen.computeIfAbsent(r.key(), k -> Collections.synchronizedList(new ArrayList<>()))
                .add(Integer.parseInt(r.value().substring(r.value().indexOf(':') + 1)));
            return Verdict.DONE;
        }));

        Await.until(() -> totalCommitted(topic, 4) == 200, WAIT);
        List<Integer> expected = IntStream.range(0, 20).boxed().toList();
        for (int k = 0; k < 10; k++) assertEquals(expected, seen.get("k" + k), "order for k" + k);
    }

    @Test
    void distinctKeysRunConcurrentlyUpToMaxInFlight() throws Exception {
        String topic = topic(1);
        for (int k = 0; k < 8; k++) send(topic, "k" + k, "v");
        AtomicInteger running = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        AtomicInteger handled = new AtomicInteger();
        start(loop(topic, 100, 4, null, new StringDeserializer(), r -> {
            peak.accumulateAndGet(running.incrementAndGet(), Math::max);
            Thread.sleep(300);
            running.decrementAndGet();
            handled.incrementAndGet();
            return Verdict.DONE;
        }));

        Await.until(() -> committed(new TopicPartition(topic, 0)) == 8, WAIT);
        assertEquals(8, handled.get());
        assertTrue(peak.get() >= 2, "keys should run concurrently, peak " + peak.get());
        assertTrue(peak.get() <= 4, "maxInFlight bounds concurrency, peak " + peak.get());
    }

    @Test
    void holdPausesSeeksBackAndCommitsBelowTheHeldRecord() throws Exception {
        String topic = topic(1);
        TopicPartition tp = new TopicPartition(topic, 0);
        for (int i = 0; i < 5; i++) send(topic, "k" + i, "v" + i);
        AtomicBoolean hold = new AtomicBoolean(true);
        AtomicInteger holds = new AtomicInteger();
        Map<String, AtomicInteger> done = new ConcurrentHashMap<>();
        BatchConsumerLoop<String> loop = loop(topic, 100, 8, null, new StringDeserializer(), r -> {
            if (r.value().equals("v2") && hold.get()) {
                holds.incrementAndGet();
                return Verdict.HOLD;
            }
            done.computeIfAbsent(r.value(), v -> new AtomicInteger()).incrementAndGet();
            return Verdict.DONE;
        });
        loop.pauseWhile(p -> hold.get() && holds.get() > 0);
        start(loop);

        Await.until(() -> committed(tp) == 2 && done.containsKey("v3") && done.containsKey("v4"), WAIT);
        Thread.sleep(1000);
        assertEquals(1, holds.get(), "a paused partition is not re-polled");
        assertEquals(2, committed(tp), "never committed past the held record");
        assertEquals(1, metrics.get("consumer.hold"));

        hold.set(false);
        Await.until(() -> committed(tp) == 5, WAIT);
        assertEquals(1, done.get("v2").get());
        assertEquals(2, done.get("v3").get(), "records after the held one are redelivered");
    }

    @Test
    void poisonAndUndeserializableRecordsGoToTheDlqWithOriginalBytes() throws Exception {
        String topic = topic(1);
        String dlq = topic(1);
        send(topic, "a", "ok1");
        send(topic, "b", "poison");
        send(topic, "c", "undeserializable");
        send(topic, "d", "ok2");
        Deserializer<String> deserializer = (t, data) -> {
            String s = new String(data, UTF_8);
            if (s.equals("undeserializable")) throw new SerializationException("bad bytes");
            return s;
        };
        Set<String> handled = ConcurrentHashMap.newKeySet();
        start(loop(topic, 100, 8, dlq, deserializer, r -> {
            if (r.value().equals("poison")) throw new PoisonException("unknown event type", null);
            handled.add(r.value());
            return Verdict.DONE;
        }));

        Await.until(() -> committed(new TopicPartition(topic, 0)) == 4, WAIT);
        assertEquals(Set.of("ok1", "ok2"), handled);
        assertEquals(2, metrics.get("consumer.poison"));

        Map<String, ConsumerRecord<String, byte[]>> dead = readAll(dlq, 2).stream()
            .collect(Collectors.toMap(ConsumerRecord::key, r -> r));
        assertArrayEquals("poison".getBytes(UTF_8), dead.get("b").value());
        assertEquals("unknown event type", header(dead.get("b"), "error"));
        assertEquals(topic + "-0@1", header(dead.get("b"), "source-offset"));
        assertArrayEquals("undeserializable".getBytes(UTF_8), dead.get("c").value());
        assertTrue(header(dead.get("c"), "error").contains("bad bytes"));
        assertEquals(topic + "-0@2", header(dead.get("c"), "source-offset"));
    }

    @Test
    void transientFailuresRetryInProcessThenRedeliverAfterBackoff() throws Exception {
        String topic = topic(1);
        send(topic, "a", "flaky2");   // fails twice, succeeds on the third in-process attempt
        send(topic, "b", "flaky3");   // fails all three in-process attempts, succeeds on redelivery
        Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        start(loop(topic, 100, 8, null, new StringDeserializer(), r -> {
            int n = attempts.computeIfAbsent(r.value(), v -> new AtomicInteger()).incrementAndGet();
            if (r.value().equals("flaky2") && n <= 2) throw new IllegalStateException("transient " + n);
            if (r.value().equals("flaky3") && n <= 3) throw new IllegalStateException("transient " + n);
            return Verdict.DONE;
        }));

        Await.until(() -> committed(new TopicPartition(topic, 0)) == 2, WAIT);
        assertEquals(3, attempts.get("flaky2").get(), "no redelivery after an in-process success");
        assertEquals(4, attempts.get("flaky3").get(), "three in-process attempts, then one redelivery");
        assertEquals(4, metrics.get("consumer.retry"));
        assertEquals(1, metrics.get("consumer.retry_exhausted"));
    }

    // Review Focus 4: a revoke while groups are in flight commits only the completed prefix.
    @Test
    void revokeMidFlightCommitsOnlyTheCompletedPrefix() throws Exception {
        String topic = topic(1);
        TopicPartition tp = new TopicPartition(topic, 0);
        send(topic, "a", "fast-a");
        send(topic, "s", "slow");
        send(topic, "c", "fast-c");
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean slowFinished = new AtomicBoolean();
        Set<String> done = ConcurrentHashMap.newKeySet();
        BatchConsumerLoop.Handler<String> handler = r -> {
            if (r.value().equals("slow")) {
                release.await();
                slowFinished.set(true);
            }
            done.add(r.value());
            return Verdict.DONE;
        };
        start(loop(topic, 100, 8, null, new StringDeserializer(), handler));
        Await.until(() -> done.containsAll(Set.of("fast-a", "fast-c")), WAIT);
        assertEquals(-1, committed(tp), "nothing is committed while the batch is in flight");

        start(loop(topic, 100, 8, null, new StringDeserializer(), handler));   // same group: forces a rebalance
        Await.until(() -> committed(tp) >= 0, WAIT);
        assertFalse(slowFinished.get());
        assertEquals(1, committed(tp), "commit stops at the unfinished record, never past it");

        release.countDown();
        Await.until(() -> committed(tp) == 3, WAIT);
    }

    @Test
    void pauseWhileHoldsEveryPartitionAndTheLoopStillBeats() throws Exception {
        String topic = topic(2);
        AtomicBoolean paused = new AtomicBoolean(true);
        List<String> handled = Collections.synchronizedList(new ArrayList<>());
        BatchConsumerLoop<String> loop = loop(topic, 100, 8, null, new StringDeserializer(), r -> {
            handled.add(r.value());
            return Verdict.DONE;
        });
        loop.pauseWhile(p -> paused.get());
        start(loop);
        for (int i = 0; i < 3; i++) send(topic, "k" + i, "v" + i);

        Thread.sleep(1500);
        assertTrue(handled.isEmpty(), "paused partitions deliver nothing");
        assertTrue(health.healthy(Duration.ofSeconds(1)), "a paused loop keeps beating");

        paused.set(false);
        Await.until(() -> totalCommitted(topic, 2) == 3, WAIT);
        assertEquals(3, handled.size());
    }

    @Test
    void afterCommitRunsEveryIterationEvenWhenNothingIsCommitted() throws Exception {
        String topic = topic(1);
        AtomicInteger emptyCalls = new AtomicInteger();
        BatchConsumerLoop<String> loop = new BatchConsumerLoop<>(
            Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
            new BatchConsumerLoop.Settings(group, List.of(topic), 100, Duration.ofMillis(200), 8, null),
            new StringDeserializer(), r -> Verdict.DONE,
            new BatchConsumerLoop.Hooks<String>() {
                @Override public void afterCommit(org.apache.kafka.clients.consumer.Consumer<String, String> consumer,
                                                  Map<TopicPartition, Long> committed, int generation) {
                    if (committed.isEmpty()) emptyCalls.incrementAndGet();
                }
            }, dlqProducer, health, metrics);
        loops.add(loop);
        start(loop);   // an empty topic: no record is ever committed
        Await.until(() -> emptyCalls.get() >= 3, WAIT);
    }

    @Test
    void closeLetsInFlightGroupsFinishAndCommits() throws Exception {
        String topic = topic(1);
        TopicPartition tp = new TopicPartition(topic, 0);
        send(topic, "a", "fast-a");
        send(topic, "s", "slow");
        send(topic, "c", "fast-c");
        CountDownLatch slowStarted = new CountDownLatch(1);
        AtomicBoolean slowDone = new AtomicBoolean();
        BatchConsumerLoop<String> loop = loop(topic, 100, 8, null, new StringDeserializer(), r -> {
            if (r.value().equals("slow")) {
                slowStarted.countDown();
                Thread.sleep(1000);
                slowDone.set(true);
            }
            return Verdict.DONE;
        });
        start(loop);
        assertTrue(slowStarted.await(30, TimeUnit.SECONDS));

        long t0 = System.nanoTime();
        loop.close();
        long elapsed = System.nanoTime() - t0;

        assertTrue(slowDone.get(), "the in-flight group finished before close returned");
        assertEquals(3, committed(tp));
        assertTrue(elapsed < Duration.ofSeconds(30).toNanos(), "closed within 30 s");
    }

    private BatchConsumerLoop<String> loop(String topic, int maxPollRecords, int maxInFlight, String dlq,
                                           Deserializer<String> deserializer, BatchConsumerLoop.Handler<String> handler) {
        Map<String, Object> props = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        BatchConsumerLoop<String> loop = new BatchConsumerLoop<>(props,
            new BatchConsumerLoop.Settings(group, List.of(topic), maxPollRecords, Duration.ofMillis(200), maxInFlight, dlq),
            deserializer, handler, new BatchConsumerLoop.Hooks<>() { }, dlqProducer, health, metrics);
        loops.add(loop);
        return loop;
    }

    private static void start(BatchConsumerLoop<String> loop) {
        Thread.ofPlatform().name("poll-loop").start(loop::run);
    }

    private static String topic(int partitions) throws Exception {
        String name = "t-" + UUID.randomUUID();
        admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get();
        return name;
    }

    private static void send(String topic, String key, String value) throws Exception {
        producer.send(new ProducerRecord<>(topic, key, value)).get();
    }

    /** Unchecked, so it can run inside Await.until's BooleanSupplier. */
    private long committed(TopicPartition tp) {
        try {
            OffsetAndMetadata o = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().get(tp);
            return o == null ? -1 : o.offset();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof GroupIdNotFoundException) return -1;
            throw new IllegalStateException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private long totalCommitted(String topic, int partitions) {
        long total = 0;
        for (int p = 0; p < partitions; p++) total += Math.max(0, committed(new TopicPartition(topic, p)));
        return total;
    }

    private static List<ConsumerRecord<String, byte[]>> readAll(String topic, int expected) {
        Map<String, Object> props = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer())) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c.assign(List.of(tp));
            c.seekToBeginning(List.of(tp));
            List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (out.size() < expected && System.nanoTime() < deadline) c.poll(Duration.ofMillis(200)).forEach(out::add);
            return out;
        }
    }

    private static String header(ConsumerRecord<String, byte[]> r, String name) {
        return new String(r.headers().lastHeader(name).value(), UTF_8);
    }

}
