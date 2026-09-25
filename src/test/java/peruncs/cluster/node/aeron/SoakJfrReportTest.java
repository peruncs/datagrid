package peruncs.cluster.node.aeron;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


/// Covers the JFR budget verdict and, when a soak recording exists, the
/// aggregation pass itself. Without `target/soak.jfr` (no `-Psoak` run yet)
/// the recording assertions skip in unit gates but fail when gating is armed:
/// the nightly pipeline runs `mvn -Psoak verify` first, then this test with
/// `-Dsoak.jfr.fail=true`, where a missing recording is a failure, not a skip.
class SoakJfrReportTest {
    /// Verifies a quiet recording stays inside both budgets with no failures.
    @Test
    void verdictPassesWithinBudget() {
        final SoakJfrReport.Signals signals = new SoakJfrReport.Signals(
                10L, 1_000L, 120L, 1L, 300L, 900L, "com.example.Monitor", List.of(), 0L, 0L, 0L, 0L);
        assertTrue(SoakJfrReport.verdict(signals, 1_000L, 2_000L).isEmpty());
    }

    /// Verifies an overloaded recording reports one failure per exceeded budget.
    @Test
    void verdictFailsBothBudgets() {
        final SoakJfrReport.Signals signals = new SoakJfrReport.Signals(
                10L, 1_000L, 5_000L, 4L, 9_000L, 12_000L, "com.example.Monitor", List.of(), 0L, 0L, 0L, 0L);
        final List<String> failures = SoakJfrReport.verdict(signals, 1_000L, 2_000L);
        assertEquals(2, failures.size());
    }

    /// Verifies undecodable durations fail the verdict instead of reading as
    /// zero compliance: a signal that cannot be proven inside budget is a failure.
    @Test
    void verdictFailsOnUndecodableDurations() {
        final SoakJfrReport.Signals signals = new SoakJfrReport.Signals(
                10L, 1_000L, 0L, 0L, 0L, 0L, "<none>", List.of(), 3L, 0L, 0L, 0L);
        final List<String> failures = SoakJfrReport.verdict(signals, 1_000L, 2_000L);
        assertEquals(1, failures.size());
        assertTrue(failures.getFirst().contains("undecodable"));
    }

    /// Verifies incomplete allocation/monitor metadata fails the verdict.
    @Test
    void verdictFailsOnUnknownMetadata() {
        final SoakJfrReport.Signals signals = new SoakJfrReport.Signals(
                10L, 1_000L, 0L, 0L, 0L, 0L, "<none>", List.of(), 0L, 2L, 0L, 0L);
        final List<String> failures = SoakJfrReport.verdict(signals, 1_000L, 2_000L);
        assertEquals(1, failures.size());
        assertTrue(failures.getFirst().contains("metadata"));
    }

    /// Verifies the aggregation pass reads a real soak recording when one
    /// exists, skips in plain unit gates, and fails when gating is armed but
    /// no recording was produced.
    @Test
    void analyzesTheSoakRecordingWhenPresent() throws Exception {
        final Path recording = Path.of("target/soak.jfr");
        final boolean gating = Boolean.parseBoolean(System.getProperty("soak.jfr.fail", "false"));
        if (!Files.exists(recording) && gating) {
            fail("gating is armed but no soak recording exists; run mvn -Psoak verify first");
        }
        assumeTrue(Files.exists(recording), "no soak recording; run mvn -Psoak verify first");
        final SoakJfrReport.Signals signals = SoakJfrReport.analyze(recording);
        assertTrue(signals.totalEvents() > 0, "recording holds no events");
        System.out.print(SoakJfrReport.format(signals));
        assertFalse(SoakJfrReport.format(signals).isBlank());
        if (Boolean.parseBoolean(System.getProperty("soak.jfr.fail", "false"))) {
            final List<String> failures = SoakJfrReport.verdict(signals,
                    Long.getLong("soak.jfr.maxGcPauseMs", 1_000L),
                    Long.getLong("soak.jfr.maxMonitorMs", 2_000L));
            assertTrue(failures.isEmpty(), "soak JFR budgets exceeded: %s".formatted(failures));
        }
    }

    /// Verifies the top-allocated table ranks classes by descending bytes.
    @Test
    void topAllocatedOrderingIsDescending() {
        final SoakJfrReport.Signals signals = new SoakJfrReport.Signals(3L, 30L, 0L, 0L, 0L, 0L, "<none>",
                List.of(Map.entry("a.A", 300L), Map.entry("b.B", 100L)), 0L, 0L, 0L, 0L);
        assertEquals("a.A", signals.topAllocated().getFirst().getKey());
    }

    /// Verifies virtual-thread pinning is included in the rendered report.
    @Test
    void formatIncludesVirtualThreadPinning() {
        final SoakJfrReport.Signals signals = new SoakJfrReport.Signals(
                1L, 2L, 0L, 0L, 0L, 0L, "<none>", List.of(), 0L, 0L, 3L, 17L);
        assertTrue(SoakJfrReport.format(signals).contains(
                "virtualThreadPinnedEvents=3 maxVirtualThreadPinnedMs=17"));
    }
}
