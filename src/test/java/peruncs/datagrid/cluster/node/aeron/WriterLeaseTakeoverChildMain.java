package peruncs.datagrid.cluster.node.aeron;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

/// Holds a lease across forked takeover tests for both offer fencing and
/// process-death takeover. Heartbeats stay live unless the offer-boundary
/// test explicitly leaves the default suspension enabled.
public final class WriterLeaseTakeoverChildMain {
    private WriterLeaseTakeoverChildMain() {
    }

    static void main(final String[] args) throws Exception {
        final Path volume = Path.of(args[0]);
        final UUID cluster = UUID.fromString(args[1]);
        final UUID generation = UUID.fromString(args[2]);
        final Path ready = volume.resolve("child-ready");
        final Path release = volume.resolve("child-release");
        final Path result = volume.resolve("child-result");
        try (WriterFencingLease lease = WriterFencingLease.acquire(
                volume, cluster, generation, UUID.randomUUID(), Duration.ofMillis(300))) {
            if (Boolean.parseBoolean(System.getProperty("dg.lease.suspendHeartbeat", "true"))) {
                lease.suspendHeartbeatForTest();
            }
            Files.writeString(ready, "ready");
            final long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (!Files.exists(release) && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            if (!Files.exists(release)) throw new IllegalStateException("parent did not release offer boundary");
            try {
                lease.executeUnderOwnership(() -> {
                    try {
                        Files.writeString(volume.resolve("child-offered"), "offered");
                    } catch (final java.io.IOException failure) {
                        throw new IllegalStateException(failure);
                    }
                    return 1L;
                });
                Files.writeString(result, "OFFERED");
            } catch (final IllegalStateException fenced) {
                if (!fenced.getMessage().contains("fenced") && !fenced.getMessage().contains("closed")) {
                    throw fenced;
                }
                Files.writeString(result, "FENCED");
            }
        }
    }
}
