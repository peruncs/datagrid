package peruncs.cluster.node.aeron;

import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.node.replication.ClusterReplicationTransport;
import peruncs.cluster.node.replication.ReplicationHealth;
import peruncs.cluster.storage.binary.ReplicationApplier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

/// Forked production-provider driver-timeout probe.
public final class AeronProviderDriverFailureChildMain {
    private AeronProviderDriverFailureChildMain() {
    }

    static void main(final String[] ignored) throws Exception {
        final Path root = Path.of(System.getProperty("dg.driver.failure.root"));
        Files.createDirectories(root.resolve("control"));
        final ClusterReplicationTransport transport = new AeronTransport(properties(root));
        final ReplicationApplier client = ReplicationApplier.noOp();
        final ReplicationHealth health = transport.health(() -> true, client);
        final var positionProvider = transport.positionProvider();
        positionProvider.init();
        Files.writeString(root.resolve("control/connected"), "connected");
        AeronTransport.stopDriverForTest(transport);
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (health.isHealthy() && System.nanoTime() < deadline) LockSupport.parkNanos(1_000_000L);
        Files.writeString(root.resolve("control/outcome"), health.isHealthy() ? "NO_FAILURE" : "FAILED");
        try {
            transport.close();
        } catch (final RuntimeException ignoredFailure) {
        }
    }

    private static NodeConfig properties(final Path root) {
        return TestNodeConfig.aeron(root, "writer", false,
                Map.of(NodeConfig.Setting.AERON_DRIVER_TIMEOUT_MILLIS.key(), "1000"));
    }
}
