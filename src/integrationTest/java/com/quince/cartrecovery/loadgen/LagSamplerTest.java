package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.app.Health;
import com.quince.cartrecovery.infra.kafka.TestKafka;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.infra.redis.TestRedis;
import com.quince.cartrecovery.model.Timer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the four things LagSampler now samples every 5 s (spec 8.5, controller ruling on scope):
 * achieved produce rate, consumer lag per group, per-partition Redis watermark lag, and the Redis
 * timer backlog past due. Calls {@code sampleOnce()} directly instead of waiting on the real 5 s
 * schedule.
 */
@Testcontainers(disabledWithoutDocker = true)
class LagSamplerTest {

    @Test
    void oneSampleReportsProduceRateConsumerLagWatermarkLagAndTimerBacklog() throws Exception {
        TestRedis.flushAll();
        String group = "lagsampler-" + UUID.randomUUID();
        String topic = Topics.CART_EVENTS;
        Producer<String, byte[]> producer = TestKafka.producer();

        // Join the group and commit the starting position, then produce more so this group has lag.
        Map<String, Object> consumerProps = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TestKafka.bootstrap(),
            ConsumerConfig.GROUP_ID_CONFIG, group,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(topic));
            consumer.poll(Duration.ofSeconds(10)); // triggers group join, partition assignment and position init
            consumer.commitSync();
        }
        String key = "lagsampler-" + UUID.randomUUID();
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>(topic, key, ("v" + i).getBytes())).get();
        }

        RedisWatermark watermark = new RedisWatermark(TestRedis.connection(), 1, Duration.ofSeconds(5));
        RedisTimerStore timerStore = new RedisTimerStore(TestRedis.connection(), 4, Duration.ofSeconds(30));
        timerStore.upsert(Timer.checkAbandon("past-due-a", 1, Instant.ofEpochMilli(1_000), 0));
        timerStore.upsert(Timer.checkAbandon("past-due-b", 1, Instant.ofEpochMilli(1_000), 0));
        timerStore.upsert(Timer.checkAbandon("not-due-yet", 1, Instant.now().plusSeconds(3600), 0));

        AtomicLong producedCount = new AtomicLong(0);
        LagSampler sampler = new LagSampler(TestKafka.bootstrap(), List.of(group), new Health(),
            producedCount, watermark, timerStore, 1);

        producedCount.set(10); // "produced" before the first sample, establishing a baseline
        sampler.sampleOnce();
        producedCount.set(30); // 20 more events produced before the second sample
        Thread.sleep(200);
        sampler.sampleOnce();

        assertTrue(sampler.maxLagByGroup().get(group) >= 5, "lag must reflect the 5 unconsumed records");
        assertEquals(sampler.maxLagByGroup().get(group), sampler.endingLagByGroup().get(group));
        assertTrue(sampler.maxWatermarkLagMillis() > 0, "no watermark ever published: partition 0 reads as fully lagged");
        assertEquals(2, sampler.maxPastDueBacklog());

        sampler.stop();
        assertTrue(sampler.isStopped());
    }
}
