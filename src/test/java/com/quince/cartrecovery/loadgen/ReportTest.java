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
            4, 1,
            6,
            1, 0.0001,
            finalReadTimedOut,
            8, 16L * 1024 * 1024 * 1024);
    }

    @Test void rendersEveryFieldFromSpec85() {
        String md = Report.render(sample());

        assertTrue(md.contains("run-abc123"));
        assertTrue(md.contains("5000.0"));
        assertTrue(md.contains("4870.3"));
        assertTrue(md.contains("detector"));
        assertTrue(md.contains("Bottleneck stage: **detector**"));
        assertTrue(md.contains("Max per-partition watermark lag: 420 ms"));
        assertTrue(md.contains("Max Redis timer backlog past due: 7"));
        assertTrue(md.contains("fast"));
        assertTrue(md.contains("slow"));
        assertTrue(md.contains("Duplicate sends: **0**"));
        assertTrue(md.contains("Post-purchase sends: **0**"));
        assertTrue(md.contains("Superseded before send"));
        assertTrue(md.contains("Missed while lagging"));
        assertTrue(md.contains("Outcomes on non-expected keys"));
        assertTrue(md.contains("): 6\n"), "outcomesOnNonExpectedKeys value must render");
        assertTrue(md.contains("Unexplained missing: 1"));
        assertTrue(md.contains("305 s actual publish span"));
        assertTrue(md.contains("nominal DURATION was 300 s"));
        assertTrue(md.contains("Cores: 8"));
        assertTrue(md.contains("shares this machine"));
        assertTrue(md.contains("loadgen JVM's own max heap"));
    }

    // Fix round 2: an EPOCH watermark read must render as "stale", never as a huge epoch-derived ms figure.
    @Test void aStaleWatermarkRendersAsStaleNotAsAMeaninglessMsFigure() {
        String md = Report.render(sample(true, false));

        assertTrue(md.contains("stale"), "a stale watermark read must say so, not just show a raw ms number");
    }

    // Fix round 2: a timed-out final read must be flagged in the report, not silently reported as final.
    @Test void aTimedOutFinalReadIsFlaggedInTheReport() {
        String md = Report.render(sample(false, true));

        assertTrue(md.contains("did not reach sink-sends'/reminder-outcomes' end offsets in time"));
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
