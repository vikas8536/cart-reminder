package com.quince.cartrecovery.app;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Liveness and readiness state of one role (spec §7.4). A loop registers on its first beat and must
 * beat on every iteration, including while backing off or paused, so a dependency outage never fails
 * the health check.
 */
public final class Health {
    private final LongSupplier nanoTime;
    private final Map<String, Long> beats = new ConcurrentHashMap<>();
    private final Map<String, String> ready = new ConcurrentHashMap<>();

    public Health() {
        this(System::nanoTime);
    }

    Health(LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
    }

    /** The loop made progress (an iteration, including backoff or pause). */
    public void beat(String loop) {
        beats.put(loop, nanoTime.getAsLong());
    }

    /** Shown on /ready as {@code key: value}. */
    public void setReady(String key, String value) {
        ready.put(key, value);
    }

    /** True when every registered loop beat within {@code maxSilence}; true when none registered. */
    public boolean healthy(Duration maxSilence) {
        return stale(maxSilence).isEmpty();
    }

    List<String> stale(Duration maxSilence) {
        long now = nanoTime.getAsLong();
        long limit = maxSilence.toNanos();
        return beats.entrySet().stream()
            .filter(e -> now - e.getValue() > limit)
            .map(Map.Entry::getKey)
            .sorted()
            .toList();
    }

    Map<String, String> readiness() {
        return new TreeMap<>(ready);
    }
}
