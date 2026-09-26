package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.core.ReminderScheduler;
import com.quince.cartrecovery.infra.dynamo.DynamoCartStateStore;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.kafka.KafkaIntentPublisher;
import com.quince.cartrecovery.infra.kafka.KafkaOutcomeRecorder;
import com.quince.cartrecovery.infra.redis.RedisTimerStore;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerDecision;
import com.quince.cartrecovery.ports.TimerStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/** Claims due timers from all shards, runs them concurrently on virtual threads, acks or releases each. */
public final class SchedulerRole implements Role {
    static final Duration IDLE = Duration.ofMillis(200);

    @Override
    public String name() { return "scheduler"; }

    @Override
    public void run(InfraConfig config, Health health, Metrics metrics) throws Exception {
        try (RoleContext ctx = new RoleContext(config)) {
            ctx.verifyStartup();
            TimerStore timers = new RedisTimerStore(ctx.redis(), config.shards(), config.dispatch().lease());
            // The scheduler has no Clock: its time source is the Redis-timed watermark (controller ruling R13).
            ReminderScheduler scheduler = new ReminderScheduler(config.recovery(), config.dispatch(),
                new DynamoCartStateStore(ctx.dynamo(), DynamoTables.CARTS, config.recovery(), config.shards()), timers,
                new RedisWatermark(ctx.redis(), config.partitions()),
                new KafkaIntentPublisher(ctx.producer(), config.dispatch().fastOffsets()),
                new KafkaOutcomeRecorder(ctx.producer()), metrics);
            AtomicBoolean running = new AtomicBoolean(true);
            RoleContext.runLoops(() -> running.set(false),
                List.of(() -> claimLoop(timers, scheduler, config.maxInFlight(), running, health, metrics)));
        }
    }

    static void claimLoop(TimerStore timers, ReminderScheduler scheduler, int batch, AtomicBoolean running,
                          Health health, Metrics metrics) {
        while (running.get()) {
            health.beat("scheduler");
            List<Timer> due;
            try {
                due = timers.claimDue(batch);
            } catch (RuntimeException e) {
                metrics.increment("scheduler.claim_failed");   // Redis unreachable: keep beating, try again
                due = List.of();
            }
            if (due.isEmpty()) {
                if (!RoleContext.sleep(IDLE)) return;
                continue;
            }
            try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
                for (Timer t : due) exec.submit(() -> fire(timers, scheduler, t, metrics));
            }
        }
    }

    /**
     * Ack after processing; Release for the gate. Deterministic SDK errors (DynamoDB ValidationException and other
     * non-retryable 400s, see Failures) are poison: ack and count timers.poison (controller ruling R13). Others stay
     * leased for redelivery.
     */
    static void fire(TimerStore timers, ReminderScheduler scheduler, Timer timer, Metrics metrics) {
        try {
            switch (scheduler.onTimer(timer)) {
                case TimerDecision.Ack ack -> timers.ack(timer);
                case TimerDecision.Release release -> timers.release(timer, release.delay());
            }
        } catch (RuntimeException e) {
            if (Failures.isDeterministic(e)) {
                timers.ack(timer);
                metrics.increment("timers.poison");
            } else {
                metrics.increment("scheduler.timer_failed");
            }
        }
    }
}
