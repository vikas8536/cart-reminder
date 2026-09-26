package com.quince.cartrecovery.app;

import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.NotificationSink;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

/**
 * Wraps the sink (spec §6.2). Over the last {@code window} attempts, a transient-failure ratio above
 * {@code threshold} opens the breaker for {@code openFor}; while open, sends fail fast with
 * TRANSIENT_FAILURE without reaching the delegate. After {@code openFor}, exactly one probe send
 * decides: success closes and clears the window, a transient failure reopens.
 * {@link #isOpen()} is what consumers and the retry loop pause on.
 */
public final class CircuitBreaker implements NotificationSink {
    private final NotificationSink delegate;
    private final int window;
    private final double threshold;
    private final Duration openFor;
    private final Clock clock;
    private final boolean[] outcomes;   // ring of closed-state attempts; true = transient failure
    private int count;
    private int next;
    private int transients;
    private Instant openUntil;          // null while closed
    private boolean probing;

    public CircuitBreaker(NotificationSink delegate, int window, double threshold, Duration openFor, Clock clock) {
        if (window < 1) throw new IllegalArgumentException("window must be at least 1");
        if (threshold < 0 || threshold >= 1) throw new IllegalArgumentException("threshold must be in [0, 1)");
        if (!openFor.isPositive()) throw new IllegalArgumentException("openFor must be positive");
        this.delegate = delegate;
        this.window = window;
        this.threshold = threshold;
        this.openFor = openFor;
        this.clock = clock;
        this.outcomes = new boolean[window];
    }

    @Override
    public SendResult send(ReminderMessage message) {
        boolean probe = false;
        synchronized (this) {
            if (openUntil != null) {
                if (probing || clock.now().isBefore(openUntil)) return SendResult.TRANSIENT_FAILURE;
                probing = true;
                probe = true;
            }
        }
        SendResult result = null;
        try {
            result = delegate.send(message);   // outside the lock: a slow gateway never blocks isOpen()
            return result;
        } finally {
            settle(probe, result);
        }
    }

    public synchronized boolean isOpen() {
        return openUntil != null && (probing || clock.now().isBefore(openUntil));
    }

    private synchronized void settle(boolean probe, SendResult result) {
        boolean failed = result == null || result == SendResult.TRANSIENT_FAILURE;   // a throw counts as transient
        if (probe) {
            probing = false;
            if (failed) {
                openUntil = clock.now().plus(openFor);
            } else {
                openUntil = null;
                clear();
            }
            return;
        }
        if (openUntil != null) return;     // a send that started before the breaker opened
        if (count == window && outcomes[next]) transients--;
        outcomes[next] = failed;
        if (failed) transients++;
        next = (next + 1) % window;
        count = Math.min(count + 1, window);
        if (count == window && transients > threshold * window) {
            openUntil = clock.now().plus(openFor);
            clear();
        }
    }

    private void clear() {
        Arrays.fill(outcomes, false);
        count = 0;
        next = 0;
        transients = 0;
    }
}
