package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.app.Health;
import com.quince.cartrecovery.model.RecoveryConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LoadgenRoleTest {
    @Test void drainWaitIsWindowPlusLastOffsetPlusLastLatenessPlusOneReconcileInterval() {
        RecoveryConfig config = RecoveryConfig.defaults(); // window 30m, last offset 24h, last lateness 30m
        Duration reconcileInterval = Duration.ofMinutes(5);

        Duration wait = LoadgenRole.drainWait(config, reconcileInterval);

        Duration expected = config.window()
            .plus(config.offsets().get(config.offsets().size() - 1))
            .plus(config.latenessBounds().get(config.latenessBounds().size() - 1))
            .plus(reconcileInterval);
        assertEquals(expected, wait);
    }

    // Fix round 1, finding 1: achieved rate must divide by the publish-only interval, not publish + drain.
    @Test void achievedRateDividesByThePublishIntervalOnly() {
        Instant publishStart = Instant.parse("2026-01-01T00:00:00Z");
        Instant publishFinished = publishStart.plusSeconds(10);

        double rate = LoadgenRole.achievedRate(500, publishStart, publishFinished);

        assertEquals(50.0, rate, 1e-9);
    }

    @Test void achievedRateGuardsAgainstANonPositiveInterval() {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        assertEquals(0.0, LoadgenRole.achievedRate(500, start, start));
    }

    // Fix round 1, finding 3: the drain wait must beat health in increments, not go silent for the whole sleep.
    @Test void sleepInBeatsCallsOnBeatOnceEveryStep() throws InterruptedException {
        AtomicInteger beats = new AtomicInteger();

        LoadgenRole.sleepInBeats(Duration.ofMillis(650), Duration.ofMillis(200), beats::incrementAndGet);

        assertEquals(4, beats.get(), "200,200,200,50ms steps: 4 beats");
    }

    @Test void sleepInBeatsReturnsPromptlyWhenInterruptedInsteadOfSleepingTheWholeTotal() throws InterruptedException {
        AtomicInteger beats = new AtomicInteger();
        AtomicInteger caught = new AtomicInteger();
        Thread worker = new Thread(() -> {
            try {
                LoadgenRole.sleepInBeats(Duration.ofSeconds(30), Duration.ofSeconds(1), beats::incrementAndGet);
            } catch (InterruptedException e) {
                caught.incrementAndGet();
            }
        });

        worker.start();
        Thread.sleep(50); // let it enter the first step
        worker.interrupt();
        worker.join(Duration.ofSeconds(5).toMillis());

        assertFalse(worker.isAlive(), "must not still be sleeping out the full 30 s total");
        assertEquals(1, caught.get());
    }

    // Fix round 1, finding 2: an interrupt or a thrown exception must never leak the sampler's or
    // collector's background threads (ScheduledThreadPoolExecutor, AdminClient, poll thread).
    @Test void publishAndDrainStopsSamplerAndCollectorEvenWhenBodyThrows() {
        LagSampler sampler = new LagSampler("localhost:59999", List.of("g"), new Health(),
            new AtomicLong(), null, null, 0);
        OutcomeCollector collector = new OutcomeCollector("localhost:59999", "run", Map.of(), List.of(), 2);

        assertThrows(RuntimeException.class, () ->
            LoadgenRole.publishAndDrain(sampler, collector, () -> { throw new RuntimeException("boom"); }));

        assertTrue(sampler.isStopped());
        assertFalse(collector.isRunning());
    }

    @Test void publishAndDrainStopsSamplerAndCollectorOnInterrupt() {
        LagSampler sampler = new LagSampler("localhost:59999", List.of("g"), new Health(),
            new AtomicLong(), null, null, 0);
        OutcomeCollector collector = new OutcomeCollector("localhost:59999", "run", Map.of(), List.of(), 2);

        assertThrows(InterruptedException.class, () ->
            LoadgenRole.publishAndDrain(sampler, collector, () -> { throw new InterruptedException(); }));

        assertTrue(sampler.isStopped());
        assertFalse(collector.isRunning());
    }
}
