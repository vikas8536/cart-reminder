package com.quince.cartrecovery.loadgen;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportTest {
    private static LoadTestSummary sample() {
        return sample(false, false);
    }

    private static LoadTestSummary sample(boolean watermarkEverStale, boolean finalReadTimedOut) {
        return new LoadTestSummary(
            "run-abc123",
            Instant.parse("2026-09-26T10:00:00Z"),
            Instant.parse("2026-09-26T10:05:12Z"),
            5000.0, 4870.3,
            300L, 305L,
            Map.of("detector", 12000L, "dispatcher-fast", 300L),
            Map.of("detector", 0L, "dispatcher-fast", 0L),
            "detector",
            420, watermarkEverStale, 7,
            Map.of("fast", new Percentiles.Result(800, 1500, 2200, 4000),
                   "slow", new Percentiles.Result(900, 1600, 2400, 500)),
            10000, 9985, 5, 3, 2,
            0, 0,
            4, 1, 2,
            new MissingBreakdown.Result(5, 0, 3),
            6,
            3, 0.0003,
            finalReadTimedOut,
            8, 16L * 1024 * 1024 * 1024);
    }

    @Test void rendersEveryFieldFromSpec85() {
        String md = Report.render(sample());

        assertTrue(md.contains("run-abc123"));
        assertTrue(md.contains("5000.0"));
        assertTrue(md.contains("4870.3"));
        assertTrue(md.contains("Bottleneck stage: **detector**"));
        assertTrue(md.contains("Max per-partition watermark lag: 420 ms"));
        assertTrue(md.contains("Max Redis timer backlog past due: 7"));
        assertTrue(md.contains("SENT > DEAD > CANCELLED > SKIPPED_LATE > SUPERSEDED"));
        assertTrue(md.contains("| 10000 | 9985 | 5 | 3 | 2 | 5 |"), "superseded column = before + after sendBy");
        assertTrue(md.contains("Duplicate sends: **0**"));
        assertTrue(md.contains("Post-purchase sends: **0**"));
        assertTrue(md.contains("Superseded before sendBy ("));
        assertTrue(md.contains("excluded from unexplained missing): 4\n"));
        assertTrue(md.contains("Superseded after sendBy (pure lateness"));
        assertTrue(md.contains("sendBy): 1\n"), "supersededAfterSendBy value must render");
        assertTrue(md.contains("Never superseded, no outcome ("));
        assertTrue(md.contains("possible silent loss): 2\n"), "neverSupersededNoOutcome value must render");
        assertTrue(md.contains("Outcomes on non-expected keys"));
        assertTrue(md.contains("): 6\n"), "outcomesOnNonExpectedKeys value must render");
        assertTrue(md.contains("never superseded, no outcome): 3 ("));
        assertTrue(md.contains("Script inference cross-check"));
        assertTrue(md.contains("superseded before send 5, superseded after sendBy 0, never superseded 3"));
        assertTrue(md.contains("305 s actual publish span"));
        assertTrue(md.contains("nominal DURATION was 300 s"));
        assertTrue(md.contains("Cores: 8"));
        assertTrue(md.contains("shares this machine"));
        assertTrue(md.contains("loadgen JVM's own max heap"));
    }

    @Test void aStaleWatermarkRendersAsStaleNotAsAMeaninglessMsFigure() {
        assertTrue(Report.render(sample(true, false)).contains("stale"));
    }

    @Test void aTimedOutFinalReadIsFlaggedInTheReport() {
        assertTrue(Report.render(sample(false, true)).contains("did not reach sink-sends'/reminder-outcomes' end offsets in time"));
    }

    @Test void writeCreatesATimestampedMarkdownFileUnderTheReportsDir(@TempDir Path tempDir) throws IOException {
        Path reportsDir = tempDir.resolve("build/reports/load");

        Path written = Report.write(sample(), reportsDir);

        assertTrue(Files.exists(written));
        assertTrue(written.getFileName().toString().endsWith(".md"));
        assertEquals(reportsDir, written.getParent());
        assertTrue(Files.readString(written).contains("run-abc123"));
    }
}
