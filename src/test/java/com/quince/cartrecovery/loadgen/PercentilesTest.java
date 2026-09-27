package com.quince.cartrecovery.loadgen;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PercentilesTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    @Test void hundredEvenlySpacedSamplesGiveTheExpectedRankedPercentiles() {
        List<LatencySample> samples = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            samples.add(new LatencySample(START.plusSeconds(60), Duration.ofMillis(i)));
        }

        Percentiles.Result result = Percentiles.compute(samples, START, Duration.ofSeconds(30));

        assertEquals(50, result.p50Millis());
        assertEquals(95, result.p95Millis());
        assertEquals(99, result.p99Millis());
        assertEquals(100, result.sampleCount());
    }

    @Test void samplesScheduledDuringWarmupAreExcluded() {
        List<LatencySample> samples = List.of(
            new LatencySample(START.plusSeconds(5), Duration.ofMillis(9999)),   // during warm-up: excluded
            new LatencySample(START.plusSeconds(31), Duration.ofMillis(100)));  // after warm-up: included

        Percentiles.Result result = Percentiles.compute(samples, START, Duration.ofSeconds(30));

        assertEquals(1, result.sampleCount());
        assertEquals(100, result.p50Millis());
    }

    @Test void noSamplesAfterWarmupGivesAnEmptyResultNotAnException() {
        List<LatencySample> samples = List.of(
            new LatencySample(START.plusSeconds(5), Duration.ofMillis(50)));

        Percentiles.Result result = Percentiles.compute(samples, START, Duration.ofSeconds(30));

        assertEquals(0, result.sampleCount());
        assertEquals(0, result.p50Millis());
    }

    @Test void aSampleExactlyAtTheWarmupBoundaryIsIncluded() {
        List<LatencySample> samples = List.of(
            new LatencySample(START.plusSeconds(30), Duration.ofMillis(42)));

        Percentiles.Result result = Percentiles.compute(samples, START, Duration.ofSeconds(30));

        assertEquals(1, result.sampleCount());
    }
}
