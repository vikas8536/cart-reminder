package com.quince.cartrecovery.loadgen;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** p50/p95/p99 of scheduled-to-sent latency, excluding a warm-up window at the start of the run. */
public final class Percentiles {
    private Percentiles() {}

    public record Result(long p50Millis, long p95Millis, long p99Millis, int sampleCount) {
        static final Result EMPTY = new Result(0, 0, 0, 0);
    }

    public static Result compute(List<LatencySample> samples, Instant testStart, Duration warmup) {
        Instant warmupEnds = testStart.plus(warmup);
        long[] sortedMillis = samples.stream()
            .filter(s -> !s.scheduledFor().isBefore(warmupEnds))
            .map(LatencySample::latency)
            .mapToLong(Duration::toMillis)
            .sorted()
            .toArray();

        if (sortedMillis.length == 0) return Result.EMPTY;

        return new Result(percentile(sortedMillis, 50), percentile(sortedMillis, 95),
            percentile(sortedMillis, 99), sortedMillis.length);
    }

    /** Nearest-rank percentile: index = ceil(p/100 * n) - 1, clamped into range. */
    private static long percentile(long[] sortedMillis, int p) {
        int n = sortedMillis.length;
        int index = (int) Math.ceil(p / 100.0 * n) - 1;
        index = Math.max(0, Math.min(n - 1, index));
        return sortedMillis[index];
    }
}
