package com.quince.cartrecovery.loadgen;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/** Renders the load test summary as markdown (spec 8.5) and writes it to {@code build/reports/load/<timestamp>.md}. */
public final class Report {
    private static final DateTimeFormatter FILE_STAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss");

    private Report() {}

    public static String render(LoadTestSummary s) {
        StringBuilder md = new StringBuilder();
        md.append("# Load test report: ").append(s.runId()).append("\n\n");
        md.append("Started: ").append(s.startedAt()).append("  \n");
        md.append("Finished: ").append(s.finishedAt()).append("\n\n");

        md.append("## Throughput\n\n");
        md.append("- Target rate: ").append(s.targetRatePerSecond()).append(" events/s\n");
        md.append("- Achieved rate: ").append(s.achievedRatePerSecond()).append(" events/s")
          .append(" (over a ").append(s.publishSpanSeconds()).append(" s actual publish span; nominal DURATION was ")
          .append(s.nominalDurationSeconds()).append(" s)\n\n");

        md.append("## Consumer lag by group\n\n");
        md.append("| Group | Max lag | Ending lag |\n|---|---|---|\n");
        for (String group : s.maxConsumerLagByGroup().keySet()) {
            md.append("| ").append(group).append(" | ")
              .append(s.maxConsumerLagByGroup().get(group)).append(" | ")
              .append(s.endingConsumerLagByGroup().getOrDefault(group, 0L)).append(" |\n");
        }
        md.append("\nBottleneck stage: **").append(s.bottleneckStage()).append("**\n\n");
        md.append("Max per-partition watermark lag: ").append(s.maxWatermarkLagMillis()).append(" ms");
        if (s.watermarkEverStale()) {
            md.append(" (excludes at least one partition that read **stale** — never published, or gone stale —")
              .append(" rather than folding a meaningless epoch-derived number into this max)");
        }
        md.append("  \n");
        md.append("Max Redis timer backlog past due: ").append(s.maxTimerBacklogPastDue()).append("\n\n");

        md.append("## Scheduled-to-sent latency by lane (ms, excluding the 30 s warm-up)\n\n");
        md.append("| Lane | p50 | p95 | p99 | samples |\n|---|---|---|---|---|\n");
        for (Map.Entry<String, Percentiles.Result> e : s.latencyByLane().entrySet()) {
            Percentiles.Result r = e.getValue();
            md.append("| ").append(e.getKey()).append(" | ").append(r.p50Millis()).append(" | ")
              .append(r.p95Millis()).append(" | ").append(r.p99Millis()).append(" | ")
              .append(r.sampleCount()).append(" |\n");
        }
        md.append("\n");

        md.append("## Outcomes\n\n");
        md.append("Sent, skipped late, cancelled and dead below are counted only over expected keys (fix round 2); ")
          .append("see \"Outcomes on non-expected keys\" for the rest.\n\n");
        md.append("| Expected | Sent | Skipped late | Cancelled | Dead |\n|---|---|---|---|---|\n");
        md.append("| ").append(s.expectedSends()).append(" | ").append(s.sentSends()).append(" | ")
          .append(s.skippedLate()).append(" | ").append(s.cancelled()).append(" | ").append(s.dead()).append(" |\n\n");

        md.append("## Correctness at the sink\n\n");
        md.append("- Duplicate sends: **").append(s.duplicateSends()).append("**\n");
        md.append("- Post-purchase sends: **").append(s.postPurchaseSends()).append("**\n");
        md.append("- Superseded before send (a resume or purchase superseded the cycle at or before that offset's ")
          .append("sendBy — a correct non-send, excluded from unexplained missing): ").append(s.supersededBeforeSend()).append("\n");
        md.append("- Missed while lagging (superseded only after sendBy, or never superseded at all — a real ")
          .append("failure; equals unexplained missing exactly, key for key): ").append(s.missedWhileLagging()).append("\n");
        md.append("- Outcomes on non-expected keys (an outcome recorded for a key Expected never counted — e.g. a ")
          .append("real-vs-nominal timing edge at a cycle boundary; excluded from every count above so it can't ")
          .append("silently cancel out a real miss): ").append(s.outcomesOnNonExpectedKeys()).append("\n");
        md.append("- Unexplained missing: ").append(s.unexplainedMissing())
          .append(" (").append(String.format("%.4f", s.unexplainedMissingRatio() * 100)).append("% of expected)\n\n");

        md.append("## Machine\n\n");
        md.append("- Cores: ").append(s.machineCores()).append("\n");
        md.append("- Memory: ").append(s.machineMemoryBytes() / (1024 * 1024))
          .append(" MB (the loadgen JVM's own max heap, `Runtime.maxMemory()` — not the machine's total memory)\n");
        md.append("- Note: the loadgen process shares this machine with every role and every infra container; ")
          .append("these figures are not an isolated benchmark.\n");
        if (s.finalReadTimedOut()) {
            md.append("- **Warning: the bounded final catch-up read did not reach sink-sends'/reminder-outcomes' end ")
              .append("offsets in time.** The counts above may be a hair short of the true final state.\n");
        }

        return md.toString();
    }

    /** Writes the rendered report to {@code reportsDir/<timestamp>.md} and returns the path written. */
    public static Path write(LoadTestSummary s, Path reportsDir) {
        try {
            Files.createDirectories(reportsDir);
            String fileName = FILE_STAMP.format(s.startedAt().atZone(java.time.ZoneOffset.UTC)) + ".md";
            Path target = reportsDir.resolve(fileName);
            Files.writeString(target, render(s));
            return target;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
