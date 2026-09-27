package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.AbandonmentDetector;
import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.inmemory.HashArmAssigner;
import com.quince.cartrecovery.infra.dynamo.DynamoCartStateStore;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.model.CartEvent;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Consumes cart-events: timer first, then the conditional cart update; publishes watermarks every loop. */
public final class DetectorRole implements Role {
    static final String GROUP = "detector";

    @Override
    public String name() { return "detector"; }

    @Override
    public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        try (RoleContext ctx = new RoleContext(config)) {
            ctx.verifyStartup();
            AbandonmentDetector detector = new AbandonmentDetector(config.recovery(),
                new DynamoCartStateStore(ctx.dynamo(), DynamoTables.CARTS, config.recovery(), config.shards()),
                new RedisTimerStore(ctx.redis(), config.shards(), config.dispatch().lease()),
                new HashArmAssigner(HashArmAssigner.SALT, config.recovery().holdoutPercent()), metrics);
            DetectorWatermarkHooks hooks = new DetectorWatermarkHooks(
                new RedisWatermark(ctx.redis(), config.partitions()), health, metrics, System::nanoTime);
            BatchConsumerLoop<byte[]> loop = new BatchConsumerLoop<>(ctx.consumerProps(GROUP),
                new BatchConsumerLoop.Settings(GROUP, List.of(Topics.CART_EVENTS), 500, Duration.ofMillis(500),
                    config.maxInFlight(), Topics.CART_EVENTS_DLQ),
                new ByteArrayDeserializer(), record -> handle(detector, record),
                RoleContext.withStuckDetection(hooks, health), ctx.producer(), health, metrics);
            RoleContext.runLoops(loop::close, List.of(loop::run));
        }
    }

    /** Bad JSON, an unknown type, or a deterministic store error is poison (DLQ and commit); anything else is retried. */
    static BatchConsumerLoop.Verdict handle(AbandonmentDetector detector, ConsumerRecord<String, byte[]> record) {
        CartEvent event;
        try {
            event = JsonCodec.decodeCartEvent(record.value());
        } catch (RuntimeException e) {
            throw new PoisonException("undecodable cart event at " + record.topic() + "-" + record.partition()
                + "@" + record.offset(), e);
        }
        try {
            detector.handle(event, record.partition());
        } catch (RuntimeException e) {
            if (Failures.isDeterministic(e)) throw new PoisonException("deterministic failure: " + e.getMessage(), e);
            throw e;
        }
        return BatchConsumerLoop.Verdict.DONE;
    }
}
