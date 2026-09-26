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
        return new LoadTestSummary(
            "run-abc123",
            Instant.parse("2026-09-26T10:00:00Z"),
            Instant.parse("2026-09-26T10:05:12Z"),
            5000.0, 4870.3,
            Map.of("detector", 12000L, "dispatcher-fast", 300L),
            Map.of("detector", 0L, "dispatcher-fast", 0L),
            "detector",
            Map.of("fast", new Percentiles.Result(800, 1500, 2200, 4000),
                   "slow", new Percentiles.Result(900, 1600, 2400, 500)),
            10000, 9990, 5, 3, 2,
            0, 0,
            0, 0.0,
            8, 16L * 1024 * 1024 * 1024);
    }

    @Test void rendersEveryFieldFromSpec85() {
        String md = Report.render(sample());

        assertTrue(md.contains("run-abc123"));
        assertTrue(md.contains("5000.0"));
        assertTrue(md.contains("4870.3"));
        assertTrue(md.contains("detector"));
        assertTrue(md.contains("Bottleneck stage: **detector**"));
        assertTrue(md.contains("fast"));
        assertTrue(md.contains("slow"));
        assertTrue(md.contains("Duplicate sends: **0**"));
        assertTrue(md.contains("Post-purchase sends: **0**"));
        assertTrue(md.contains("Unexplained missing: 0"));
        assertTrue(md.contains("Cores: 8"));
        assertTrue(md.contains("shares this machine"));
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
