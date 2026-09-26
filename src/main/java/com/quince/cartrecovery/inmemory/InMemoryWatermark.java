package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Same semantics as the Redis watermark scripts, timed by the given clock. setLagging pins a partition's
 * watermark (a stalled detector) regardless of publishes until clearLag.
 */
public final class InMemoryWatermark implements Watermark {
    static final Duration STALE_AFTER = Duration.ofSeconds(5);

    private record Entry(long generation, Instant eventTime, Instant updatedAt) {}

    private final Clock clock;
    private final Map<Integer, Entry> entries = new HashMap<>();
    private final Map<Integer, Instant> lagging = new HashMap<>();

    public InMemoryWatermark(Clock clock) { this.clock = clock; }

    @Override public synchronized void publish(int partition, long generation, Instant eventTime) {
        Entry stored = entries.get(partition);
        if (stored != null && generation < stored.generation() && !isStale(stored)) return;
        Instant time = stored != null && generation == stored.generation() && stored.eventTime().isAfter(eventTime)
            ? stored.eventTime() : eventTime;
        entries.put(partition, new Entry(generation, time, clock.now()));
    }

    @Override public synchronized Instant current(int srcPartition) {
        if (srcPartition >= 0) return valueOf(srcPartition);
        Instant min = null;
        for (Integer p : allPartitions()) {
            Instant v = valueOf(p);
            if (min == null || v.isBefore(min)) min = v;
        }
        return min == null ? Instant.EPOCH : min;
    }

    @Override public Instant now() { return clock.now(); }

    public synchronized void setLagging(int partition, Instant eventTime) { lagging.put(partition, eventTime); }

    public synchronized void clearLag() { lagging.clear(); }

    private Instant valueOf(int partition) {
        Instant pinned = lagging.get(partition);
        if (pinned != null) return pinned;
        Entry e = entries.get(partition);
        if (e == null || isStale(e)) return Instant.EPOCH;
        return e.eventTime();
    }

    private boolean isStale(Entry e) { return Duration.between(e.updatedAt(), clock.now()).compareTo(STALE_AFTER) > 0; }

    private Set<Integer> allPartitions() {
        Set<Integer> all = new HashSet<>(entries.keySet());
        all.addAll(lagging.keySet());
        return all;
    }
}
