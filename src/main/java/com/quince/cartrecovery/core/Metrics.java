package com.quince.cartrecovery.core;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/** Named counters, safe to use from many threads. Production: emitted to the metrics backend from every stage. */
public final class Metrics {
    private final Map<String, LongAdder> counters = new ConcurrentHashMap<>();

    public void increment(String name) {
        counters.computeIfAbsent(name, k -> new LongAdder()).increment();
    }

    public long get(String name) {
        LongAdder c = counters.get(name);
        return c == null ? 0L : c.sum();
    }

    /** A read-only copy, sorted by name. */
    public Map<String, Long> snapshot() {
        Map<String, Long> copy = new TreeMap<>();
        counters.forEach((k, v) -> copy.put(k, v.sum()));
        return Collections.unmodifiableMap(copy);
    }
}
