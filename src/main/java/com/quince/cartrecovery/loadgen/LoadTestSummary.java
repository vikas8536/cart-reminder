package com.quince.cartrecovery.loadgen;

import java.time.Instant;
import java.util.Map;

/** Every field spec section 8.5 asks the load test report to carry. */
public record LoadTestSummary(
    String runId,
    Instant startedAt,
    Instant finishedAt,
    double targetRatePerSecond,
    double achievedRatePerSecond,
    Map<String, Long> maxConsumerLagByGroup,
    Map<String, Long> endingConsumerLagByGroup,
    String bottleneckStage,
    long maxWatermarkLagMillis,
    long maxTimerBacklogPastDue,
    Map<String, Percentiles.Result> latencyByLane,
    long expectedSends,
    long sentSends,
    long skippedLate,
    long cancelled,
    long dead,
    long duplicateSends,
    long postPurchaseSends,
    long unexplainedMissing,
    double unexplainedMissingRatio,
    int machineCores,
    long machineMemoryBytes) {}
