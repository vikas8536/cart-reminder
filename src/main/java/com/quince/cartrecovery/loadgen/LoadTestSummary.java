package com.quince.cartrecovery.loadgen;

import java.time.Instant;
import java.util.Map;

/** Every field spec section 8.5 asks the load test report to carry, plus the E2 fix round 1/2 additions. */
public record LoadTestSummary(
    String runId,
    Instant startedAt,
    Instant finishedAt,
    double targetRatePerSecond,
    double achievedRatePerSecond,
    long nominalDurationSeconds,
    long publishSpanSeconds,
    Map<String, Long> maxConsumerLagByGroup,
    Map<String, Long> endingConsumerLagByGroup,
    String bottleneckStage,
    long maxWatermarkLagMillis,
    /** Fix round 2: true if any sampled partition ever read its watermark back as EPOCH (never
     * published, or stale) — {@code maxWatermarkLagMillis} above excludes those reads rather than
     * folding a meaningless epoch-derived number into the max. */
    boolean watermarkEverStale,
    long maxTimerBacklogPastDue,
    Map<String, Percentiles.Result> latencyByLane,
    long expectedSends,
    long sentSends,
    long skippedLate,
    long cancelled,
    long dead,
    long duplicateSends,
    long postPurchaseSends,
    long supersededBeforeSend,
    /** Final review: unexplained missing split into pure lateness and possible silent loss; their sum is unexplainedMissing. */
    long supersededAfterSendBy,
    long neverSupersededNoOutcome,
    /** Fix round 2: outcomes recorded for a key {@link Expected} never counted; kept out of every other
     * count (see {@link CorrectnessSummary}) and reported here instead of silently dropped. */
    long outcomesOnNonExpectedKeys,
    long unexplainedMissing,
    double unexplainedMissingRatio,
    /** Fix round 2: true if the bounded final catch-up read did not reach sink-sends'/reminder-outcomes'
     * end offsets in time — the counts above may be a hair short of the true final state. */
    boolean finalReadTimedOut,
    int machineCores,
    long machineMemoryBytes) {}
