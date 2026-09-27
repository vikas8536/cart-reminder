package com.quince.cartrecovery.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class MetricsTest {

    @Test
    void countsFromManyThreadsWithoutLosingIncrements() throws Exception {
        Metrics metrics = new Metrics();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 1000; i++) pool.submit(() -> metrics.increment("x"));
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);
        }
        assertEquals(1000, metrics.get("x"));
        assertEquals(0, metrics.get("never"));
    }

    @Test
    void snapshotIsSortedByName() {
        Metrics metrics = new Metrics();
        metrics.increment("b");
        metrics.increment("a");
        assertEquals(List.of("a", "b"), List.copyOf(metrics.snapshot().keySet()));
    }
}
