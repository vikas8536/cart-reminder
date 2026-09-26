package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.ports.NotificationSink;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CircuitBreakerTest {
    private static final ReminderMessage MSG = new ReminderMessage("c1:1:0", "c1", "shopper-1", null, List.of());
    private static final SendResult S = SendResult.SENT;
    private static final SendResult T = SendResult.TRANSIENT_FAILURE;

    private final FakeClock clock = new FakeClock(Instant.parse("2026-01-01T00:00:00Z"));
    private final Deque<SendResult> script = new ArrayDeque<>();
    private final AtomicInteger calls = new AtomicInteger();
    private final NotificationSink sink = m -> {
        calls.incrementAndGet();
        return script.isEmpty() ? SendResult.SENT : script.poll();
    };
    private final CircuitBreaker breaker = new CircuitBreaker(sink, 10, 0.5, Duration.ofSeconds(30), clock);

    private void sendAll(SendResult... results) {
        script.addAll(List.of(results));
        for (int i = 0; i < results.length; i++) breaker.send(MSG);
    }

    private void open() {
        sendAll(T, T, T, T, T, T, T, T, T, T);
        assertTrue(breaker.isOpen());
    }

    @Test
    void ratioAtTheThresholdStaysClosed() {
        sendAll(S, S, S, S, S, T, T, T, T, T);
        assertFalse(breaker.isOpen());
    }

    @Test
    void opensOnlyOnceTheWindowIsFullAndTheRatioIsOverTheThreshold() {
        sendAll(S, S, S, S, T, T, T, T, T);
        assertFalse(breaker.isOpen(), "window of 10 not full yet");
        sendAll(T);
        assertTrue(breaker.isOpen());
    }

    @Test
    void theWindowSlidesOverTheLastAttempts() {
        sendAll(S, S, S, S, S, S, S, S, S, S, T, T, T, T, T);
        assertFalse(breaker.isOpen(), "5 of the last 10");
        sendAll(T);
        assertTrue(breaker.isOpen(), "6 of the last 10");
    }

    @Test
    void whileOpenSendsFailFastWithoutReachingTheDelegate() {
        open();
        int before = calls.get();
        assertEquals(T, breaker.send(MSG));
        assertEquals(before, calls.get());
        clock.advance(Duration.ofSeconds(29));
        assertTrue(breaker.isOpen());
        assertEquals(T, breaker.send(MSG));
        assertEquals(before, calls.get());
    }

    @Test
    void aSuccessfulProbeClosesAndClearsTheWindow() {
        open();
        clock.advance(Duration.ofSeconds(30));
        assertFalse(breaker.isOpen(), "due for a probe: consumers resume and supply it");
        int before = calls.get();
        assertEquals(S, breaker.send(MSG));
        assertEquals(before + 1, calls.get());
        assertFalse(breaker.isOpen());
        sendAll(T, T, T, T, T, T, T, T, T);
        assertFalse(breaker.isOpen(), "old failures were cleared");
    }

    @Test
    void aFailedProbeReopensForAnotherPeriod() {
        open();
        clock.advance(Duration.ofSeconds(30));
        script.add(T);
        assertEquals(T, breaker.send(MSG));
        assertTrue(breaker.isOpen());
        clock.advance(Duration.ofSeconds(29));
        assertTrue(breaker.isOpen());
        clock.advance(Duration.ofSeconds(1));
        assertFalse(breaker.isOpen());
    }

    @Test
    void onlyOneProbeRunsWhileHalfOpen() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger n = new AtomicInteger();
        NotificationSink slow = m -> {
            if (n.incrementAndGet() <= 10) return T;
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return S;
        };
        CircuitBreaker b = new CircuitBreaker(slow, 10, 0.5, Duration.ofSeconds(30), clock);
        for (int i = 0; i < 10; i++) b.send(MSG);
        assertTrue(b.isOpen());
        clock.advance(Duration.ofSeconds(30));

        Thread probe = Thread.ofPlatform().start(() -> b.send(MSG));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertTrue(b.isOpen(), "open while the probe is in flight");
        assertEquals(T, b.send(MSG));
        assertEquals(11, n.get(), "the second caller never reached the delegate");

        release.countDown();
        probe.join();
        assertFalse(b.isOpen());
    }
}
