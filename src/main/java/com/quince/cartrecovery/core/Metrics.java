package com.quince.cartrecovery.core;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/** Named counters. Production: emitted to the metrics backend from every stage. */
public final class Metrics {
    private final Map<String, Long> counters = new TreeMap<>();

    public void increment(String name) {
        counters.merge(name, 1L, Long::sum);
    }

    public long get(String name) {
        return counters.getOrDefault(name, 0L);
    }

    /** A read-only copy, sorted by name. */
    public Map<String, Long> snapshot() {
        return Collections.unmodifiableMap(new TreeMap<>(counters));
    }
}
