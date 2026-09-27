package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Dispatcher;
import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.inmemory.UnlimitedSendBudget;
import com.quince.cartrecovery.infra.dynamo.DynamoCartStateStore;
import com.quince.cartrecovery.infra.dynamo.DynamoSendLedger;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.KafkaDeadLetterQueue;
import com.quince.cartrecovery.infra.kafka.KafkaOutcomeRecorder;
import com.quince.cartrecovery.infra.kafka.KafkaRecordingSink;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.ports.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** One-off: reads reminder-dlq with its own group up to the end offsets at start and reopens DEAD ledger rows. */
public final class ReplayRole implements Role {
    static final String GROUP = "replay";

    @Override
    public String name() { return "replay"; }

    @Override
    public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        try (RoleContext ctx = new RoleContext(config);
             KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(ctx.consumerProps(GROUP))) {
            ctx.verifyStartup();
            Clock clock = Instant::now;
            Dispatcher dispatcher = new Dispatcher(config.recovery(), config.dispatch(),
                new DynamoCartStateStore(ctx.dynamo(), DynamoTables.CARTS, config.recovery(), config.shards()),
                new DynamoSendLedger(ctx.dynamo(), DynamoTables.SEND_LEDGER, config.dispatch().lease(), config.shards()),
                new RedisWatermark(ctx.redis(), config.partitions()), new UnlimitedSendBudget(),
                new KafkaRecordingSink(ctx.producer(), clock, 0.0, new Random()),
                new KafkaOutcomeRecorder(ctx.producer()), new KafkaDeadLetterQueue(ctx.producer()), clock, metrics);
            List<TopicPartition> parts = consumer.partitionsFor(Topics.REMINDER_DLQ).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
            consumer.assign(parts);
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(parts));
            for (TopicPartition p : parts) {
                OffsetAndMetadata o = committed.get(p);
                if (o == null) consumer.seekToBeginning(List.of(p)); else consumer.seek(p, o.offset());
            }
            Map<TopicPartition, Long> ends = consumer.endOffsets(parts);
            int replayed = 0;
            while (parts.stream().anyMatch(p -> consumer.position(p) < ends.get(p))) {
                if (Thread.currentThread().isInterrupted()) return;
                health.beat("replay");
                List<DeadLetter> batch = new ArrayList<>();
                for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(500))) {
                    try {
                        batch.add(JsonCodec.decodeDeadLetter(r.value()));
                    } catch (RuntimeException e) {
                        metrics.increment("replay.undecodable");   // raw poison bytes routed by the consumer loop
                    }
                }
                dispatcher.replay(batch);   // skips reason "poison"; reopening twice is harmless
                replayed += batch.size();
                consumer.commitSync();
            }
            System.out.printf("replay: processed %d dead letters%n", replayed);
        }
    }
}
