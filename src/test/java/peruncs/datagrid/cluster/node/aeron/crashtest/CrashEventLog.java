package peruncs.datagrid.cluster.node.aeron.crashtest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/// Seed-replayable event log for forked crash cells.
///
/// Every cell appends `selection`, `milestone`, `outcome`, and store-size
/// lines to `control/events.jsonl`. The failure collector already copies the
/// whole `control/` directory into the evidence bundle, so the log survives
/// exactly when it is needed: on failure. Logging never fails a cell.
final class CrashEventLog {
    private CrashEventLog() {
    }

    static void append(final Path control, final String event, final String detail) {
        try {
            Files.createDirectories(control);
            final String line = "{\"t\":%d,\"event\":\"%s\",\"detail\":\"%s\"}%n".formatted(
                    System.currentTimeMillis(), escape(event), escape(detail));
            Files.writeString(control.resolve("events.jsonl"), line,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (final Exception ignored) {
            System.out.printf("CRASH event-log unavailable: %s %s%n", event, detail);
        }
    }

    private static String escape(final String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
}
