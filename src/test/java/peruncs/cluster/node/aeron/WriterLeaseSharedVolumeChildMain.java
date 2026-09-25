package peruncs.cluster.node.aeron;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

/// Holds one cluster's lease and drives renewals plus commits while the
/// parent process runs an unrelated cluster on the same volume. This is the
/// cross-process proof for per-cluster lease lock files.
public final class WriterLeaseSharedVolumeChildMain {
    private WriterLeaseSharedVolumeChildMain() {
    }

    static void main(final String[] args) throws Exception {
        final Path volume = Path.of(args[0]);
        final UUID cluster = UUID.fromString(args[1]);
        final UUID generation = UUID.fromString(args[2]);
        final int offers = Integer.parseInt(args[3]);
        final Path ready = volume.resolve("child-ready");
        final Path go = volume.resolve("child-go");
        final Path result = volume.resolve("child-result");
        try (WriterFencingLease lease = WriterFencingLease.acquire(
                volume, cluster, generation, UUID.randomUUID(), Duration.ofMillis(300))) {
            Files.writeString(ready, "ready");
            final long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (!Files.exists(go) && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            if (!Files.exists(go)) throw new IllegalStateException("parent never released the barrier");
            for (int round = 0; round < offers; round++) {
                final long expected = round;
                final long position = lease.executeUnderOwnership(ignored -> expected);
                if (position != expected) throw new IllegalStateException("wrong position " + position);
            }
            /* Let a few heartbeats pass while the parent's commits run — the
             * shared volume must not fence either side. */
            Thread.sleep(200L);
            if (!lease.isCurrent()) throw new IllegalStateException("child lease lost to a foreign cluster");
            Files.writeString(result, "OK");
        }
    }
}
