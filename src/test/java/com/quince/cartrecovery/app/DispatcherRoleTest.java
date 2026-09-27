package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.inmemory.InMemoryWatermark;
import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class DispatcherRoleTest {
    static final TopicPartition TP = new TopicPartition("reminder-intents-fast", 0);
    static final ReminderMessage MESSAGE = new ReminderMessage("c:1:0", "c", "s", "Ada", List.of());

    final long[] nanos = {0};
    final TokenBucket bucket = new TokenBucket(10, 0.3, () -> nanos[0]);
    final CircuitBreaker breaker = new CircuitBreaker(m -> SendResult.TRANSIENT_FAILURE, 4, 0.5,
        Duration.ofSeconds(30), () -> Instant.parse("2026-01-01T00:00:00Z"));
    boolean metaPaused;

    Predicate<TopicPartition> fast() { return DispatcherRole.pauseFast(bucket, breaker, () -> metaPaused); }
    Predicate<TopicPartition> slow() { return DispatcherRole.pauseSlow(bucket, breaker, () -> metaPaused); }

    void drainAll() {
        for (int i = 0; i < 1000 && bucket.tryAcquire(Lane.FAST); i++) { }
    }

    /** Minor: both lanes must drain at the same time, or shutdown can exceed Main's 30 s wait. */
    @Test
    void closeTogetherSignalsBothLanesBeforeAwaitingEither() {
        CountDownLatch bothClosing = new CountDownLatch(2);
        boolean[] met = new boolean[2];
        Runnable[] lanes = new Runnable[2];
        for (int i = 0; i < 2; i++) {
            int lane = i;
            lanes[i] = () -> {
                bothClosing.countDown();
                try {
                    met[lane] = bothClosing.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };
        }
        DispatcherRole.closeTogether(Duration.ofSeconds(5), lanes);
        assertTrue(met[0] && met[1], "each lane was closing while the other was");
    }

    @Test
    void closeTogetherReturnsAtTheSharedDeadline() {
        long start = System.nanoTime();
        Runnable hung = () -> {
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        DispatcherRole.closeTogether(Duration.ofMillis(300), hung, hung);
        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(2)) < 0);
    }

    @Test
    void fullBucketPausesNothing() {
        assertFalse(fast().test(TP));
        assertFalse(slow().test(TP));
    }

    @Test
    void bucketAtReservePausesOnlyTheSlowLane() {
        for (int i = 0; i < 1000 && bucket.slowAllowed(); i++) assertTrue(bucket.tryAcquire(Lane.FAST));
        assertTrue(bucket.anyAvailable());
        assertFalse(fast().test(TP));
        assertTrue(slow().test(TP));
    }

    @Test
    void emptyBucketPausesBothLanes() {
        drainAll();
        assertTrue(fast().test(TP));
        assertTrue(slow().test(TP));
    }

    @Test
    void refilledBucketResumes() {
        drainAll();
        nanos[0] += Duration.ofSeconds(1).toNanos();
        assertFalse(fast().test(TP));
        assertFalse(slow().test(TP));
    }

    @Test
    void openBreakerPausesBothLanes() {
        for (int i = 0; i < 4; i++) breaker.send(MESSAGE);
        assertTrue(breaker.isOpen());
        assertTrue(fast().test(TP));
        assertTrue(slow().test(TP));
    }

    @Test
    void guardrailSwitchPausesBothLanes() {
        metaPaused = true;
        assertTrue(fast().test(TP));
        assertTrue(slow().test(TP));
    }

    @Test
    void gateHoldLastsUntilTheSourcePartitionCatchesUp() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        InMemoryWatermark watermark = new InMemoryWatermark(new FakeClock(now));
        watermark.publish(3, 1, now);
        DispatcherRole.GateHolds holds = new DispatcherRole.GateHolds(watermark, Duration.ofSeconds(5));
        TopicPartition other = new TopicPartition("reminder-intents-fast", 1);
        holds.onHold(TP, 5);       // partition 5 has no watermark: the gate held it
        holds.onHold(other, 3);    // partition 3 is current: a HOLD for want of a token, not held
        assertTrue(holds.holds(TP));
        assertFalse(holds.holds(other));
        holds.recheck();
        assertTrue(holds.holds(TP));
        watermark.publish(5, 1, now);
        holds.recheck();
        assertFalse(holds.holds(TP));
    }

    @Test
    void aRetryPassRunsEveryShardConcurrentlyAndWaitsForAll() {
        CountDownLatch allStarted = new CountDownLatch(8);
        Set<Integer> done = ConcurrentHashMap.newKeySet();
        DispatcherRole.retryPass(shard -> {
            allStarted.countDown();
            try {
                if (allStarted.await(1, TimeUnit.SECONDS)) done.add(shard);   // a serial pass never gets past shard 0 in time
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 8, new Metrics());
        assertEquals(Set.of(0, 1, 2, 3, 4, 5, 6, 7), done);
    }

    @Test
    void aFailingShardDoesNotStopTheOthers() {
        Set<Integer> done = ConcurrentHashMap.newKeySet();
        Metrics metrics = new Metrics();
        DispatcherRole.retryPass(shard -> {
            if (shard == 3) throw new IllegalStateException("dynamo unreachable");
            done.add(shard);
        }, 8, metrics);
        assertEquals(7, done.size());
        assertEquals(1, metrics.get("dispatch.retry_error"));
    }

    @Test
    void effectiveAttemptsFitTheLatenessBound() {
        assertEquals(3, DispatcherRole.effectiveAttempts(Duration.ofMinutes(5), Duration.ofMinutes(1), 5));
        assertEquals(5, DispatcherRole.effectiveAttempts(Duration.ofMinutes(30), Duration.ofMinutes(1), 5));
        assertEquals(1, DispatcherRole.effectiveAttempts(Duration.ZERO, Duration.ofMinutes(1), 5));
        assertEquals(2, DispatcherRole.effectiveAttempts(Duration.ofHours(1), Duration.ofMinutes(1), 2));
    }
}
