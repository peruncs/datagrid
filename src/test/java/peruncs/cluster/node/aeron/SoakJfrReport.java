package peruncs.cluster.node.aeron;

import jdk.jfr.consumer.RecordedClass;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

/// Offline analysis for the flight recording the soak profile dumps.
///
/// `mvn -Psoak` starts the fork with `-XX:StartFlightRecording=disk=true,
/// dumponexit=true,filename=target/soak.jfr`, which nobody read until now.
/// This pass aggregates the recording in deterministic code (top-N tables,
/// never raw events, per the project's JFR guidance) and reports GC pauses,
/// monitor-block time, virtual-thread pinning, and allocation spikes.
///
/// Warn-first by design: CI hardware varies too much to fail on absolute
/// budgets from day one. The report always prints; it fails only with
/// `-Dsoak.jfr.fail=true` when an aggregated signal exceeds its budget
/// (`-Dsoak.jfr.maxGcPauseMs=`, `-Dsoak.jfr.maxMonitorMs=`). Calibrate the
/// budgets from 2-3 nightly baselines before enabling the gate.
///
/// Nightly wiring: `mvn -Psoak verify && mvn test
/// -Dtest=SoakJfrReportTest -Dsoak.jfr.fail=true`.
final class SoakJfrReport {
    private SoakJfrReport() {
    }

    /// Aggregated signals extracted from one recording.
    record Signals(
            long eventTypes,
            long totalEvents,
            long maxGcPauseMs,
            long gcPausesOver100Ms,
            long maxMonitorBlockMs,
            long totalMonitorBlockMs,
            String topBlockedMonitor,
            List<Map.Entry<String, Long>> topAllocated,
            long unknownDurations,
            long unknownMetadata,
            long virtualThreadPinnedEvents,
            long maxVirtualThreadPinnedMs) {
    }

    /// Pure budget verdict, unit-testable without a recording.
    ///
    /// @return failure descriptions, empty when within budget
    static List<String> verdict(final Signals signals, final long maxGcPauseMs, final long maxMonitorMs) {
        final List<String> failures = new ArrayList<>();
        /* Fail closed on undecodable durations: a GC pause or monitor block
         * whose duration field is missing must not read as zero compliance. */
        if (signals.unknownDurations() > 0) {
            failures.add("undecodable event durations: %d (compliance unprovable)".formatted(signals.unknownDurations()));
        }
        if (signals.unknownMetadata() > 0) {
            failures.add("undecodable event metadata: %d (allocation/monitor analysis incomplete)"
                    .formatted(signals.unknownMetadata()));
        }
        if (signals.maxGcPauseMs() > maxGcPauseMs) {
            failures.add("max GC pause %dms exceeded budget %dms".formatted(signals.maxGcPauseMs(), maxGcPauseMs));
        }
        if (signals.maxMonitorBlockMs() > maxMonitorMs) {
            failures.add("max monitor block %dms exceeded budget %dms".formatted(
                    signals.maxMonitorBlockMs(), maxMonitorMs));
        }
        return failures;
    }

    /// Aggregates one recording file into top-N signals.
    ///
    /// @param recording flight recording produced by the soak fork
    /// @return aggregated signals
    /// @throws IOException if the recording cannot be read
    static Signals analyze(final Path recording) throws IOException {
        long totalEvents = 0L;
        final Map<String, Long> typeCounts = new HashMap<>();
        long maxGcPauseMs = 0L;
        long gcPausesOver100Ms = 0L;
        long maxMonitorBlockMs = 0L;
        long totalMonitorBlockMs = 0L;
        long unknownDurations = 0L;
        long unknownMetadata = 0L;
        long virtualThreadPinnedEvents = 0L;
        long maxVirtualThreadPinnedMs = 0L;
        final Map<String, Long> blockedByMonitor = new HashMap<>();
        final Map<String, Long> allocatedByClass = new HashMap<>();
        try (RecordingFile file = new RecordingFile(recording)) {
            while (file.hasMoreEvents()) {
                final RecordedEvent event = file.readEvent();
                totalEvents++;
                typeCounts.merge(event.getEventType().getName(), 1L, Long::sum);
                final String type = event.getEventType().getName();
                switch (type) {
                    case "jdk.GarbageCollection", "jdk.GCPhasePause", "jdk.GCPhasePauseLevel1" -> {
                        final Duration pause = durationOf(event);
                        if (pause == null) {
                            unknownDurations++;
                        } else {
                            final long pauseMs = pause.toMillis();
                            maxGcPauseMs = Math.max(maxGcPauseMs, pauseMs);
                            if (pauseMs > 100L) gcPausesOver100Ms++;
                        }
                    }
                    case "jdk.JavaMonitorEnter" -> {
                        final Duration blocked = durationOf(event);
                        if (blocked == null) {
                            unknownDurations++;
                        } else {
                            final long blockedMs = blocked.toMillis();
                            maxMonitorBlockMs = Math.max(maxMonitorBlockMs, blockedMs);
                            totalMonitorBlockMs += blockedMs;
                            final String monitor = monitorName(event);
                            if (monitor == null) unknownMetadata++;
                            else blockedByMonitor.merge(monitor, blockedMs, Long::sum);
                        }
                    }
                    case "jdk.VirtualThreadPinned" -> {
                        virtualThreadPinnedEvents++;
                        final Duration pinned = durationOf(event);
                        if (pinned == null) {
                            unknownDurations++;
                        } else {
                            maxVirtualThreadPinnedMs = Math.max(maxVirtualThreadPinnedMs, pinned.toMillis());
                        }
                    }
                    case "jdk.ObjectAllocationInNewTLAB", "jdk.ObjectAllocationOutsideTLAB" -> {
                        try {
                            final RecordedClass klass = event.getClass("objectClass");
                            final long size = event.getLong("allocationSize");
                            allocatedByClass.merge(klass.getName(), size, Long::sum);
                        } catch (final Exception missing) {
                            unknownMetadata++;
                        }
                    }
                    default -> {
                    }
                }
            }
        }
        final String topBlocked = blockedByMonitor.entrySet().stream()
                .max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElse("<none>");
        final List<Map.Entry<String, Long>> topAllocated = allocatedByClass.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(5).toList();
        return new Signals(typeCounts.size(), totalEvents, maxGcPauseMs, gcPausesOver100Ms,
                maxMonitorBlockMs, totalMonitorBlockMs, topBlocked, topAllocated,
                unknownDurations, unknownMetadata, virtualThreadPinnedEvents, maxVirtualThreadPinnedMs);
    }

    /// Reads an event duration, returning null (instead of a fake zero) when
    /// the field is absent or undecodable. Callers count the nulls.
    private static Duration durationOf(final RecordedEvent event) {
        try {
            return event.getDuration();
        } catch (final Exception missing) {
            return null;
        }
    }

    private static String monitorName(final RecordedEvent event) {
        try {
            return event.getClass("monitorClass").getName();
        } catch (final Exception missing) {
            return null;
        }
    }

    /// Formats the human-readable report printed to stdout and
    /// `target/soak-jfr.txt`.
    static String format(final Signals signals) {
        final StringBuilder report = new StringBuilder();
        report.append("SOAK-JFR eventTypes=%d totalEvents=%d%n".formatted(signals.eventTypes(), signals.totalEvents()));
        report.append("SOAK-JFR maxGcPauseMs=%d gcPausesOver100Ms=%d%n".formatted(
                signals.maxGcPauseMs(), signals.gcPausesOver100Ms()));
        report.append("SOAK-JFR maxMonitorBlockMs=%d totalMonitorBlockMs=%d topBlocked=%s%n".formatted(
                signals.maxMonitorBlockMs(), signals.totalMonitorBlockMs(), signals.topBlockedMonitor()));
        report.append("SOAK-JFR unknownDurations=%d%n".formatted(signals.unknownDurations()));
        report.append("SOAK-JFR unknownMetadata=%d%n".formatted(signals.unknownMetadata()));
        report.append("SOAK-JFR virtualThreadPinnedEvents=%d maxVirtualThreadPinnedMs=%d%n".formatted(
                signals.virtualThreadPinnedEvents(), signals.maxVirtualThreadPinnedMs()));
        report.append("SOAK-JFR top-allocated classes:%n".formatted());
        for (final Map.Entry<String, Long> entry : signals.topAllocated()) {
            report.append("SOAK-JFR   %s bytes=%d%n".formatted(entry.getKey(), entry.getValue()));
        }
        return report.toString();
    }

    /// CLI entry: `java ... SoakJfrReport [recording]`. Prints the report and
    /// exits non-zero only when `-Dsoak.jfr.fail=true` and a budget is exceeded.
    static void main(final String[] arguments) throws Exception {
        final Path recording = Path.of(arguments.length > 0 ? arguments[0] : "target/soak.jfr");
        if (!Files.exists(recording)) {
            final String hint = "SOAK-JFR no recording at %s; run mvn -Psoak verify first".formatted(recording);
            System.out.println(hint);
            if (Boolean.parseBoolean(System.getProperty("soak.jfr.fail", "false"))) {
                throw new AssertionError(hint);
            }
            return;
        }
        final Signals signals = analyze(recording);
        final String report = format(signals);
        System.out.print(report);
        try {
            Files.writeString(Path.of("target/soak-jfr.txt"), report);
        } catch (final IOException logging) {
            System.out.printf("SOAK-JFR cannot persist report: %s%n", logging);
        }
        if (Boolean.parseBoolean(System.getProperty("soak.jfr.fail", "false"))) {
            final long maxGc = Long.getLong("soak.jfr.maxGcPauseMs", 1_000L);
            final long maxMonitor = Long.getLong("soak.jfr.maxMonitorMs", 2_000L);
            final List<String> failures = verdict(signals, maxGc, maxMonitor);
            if (!failures.isEmpty()) {
                throw new AssertionError("soak JFR budgets exceeded: %s".formatted(failures));
            }
        }
    }
}
