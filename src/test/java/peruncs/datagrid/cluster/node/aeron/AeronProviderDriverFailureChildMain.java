package peruncs.datagrid.cluster.node.aeron;

import peruncs.datagrid.cluster.node.NodeSettingsSource;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.node.replication.ReplicationHealth;
import peruncs.datagrid.cluster.storage.binary.ReplicationApplier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

/// Forked production-provider driver-timeout probe.
public final class AeronProviderDriverFailureChildMain {
    private AeronProviderDriverFailureChildMain() {
    }

    static void main(final String[] ignored) throws Exception {
        final Path root = Path.of(System.getProperty("dg.driver.failure.root"));
        Files.createDirectories(root.resolve("control"));
        final ClusterReplicationTransport transport = new AeronTransport(properties(root));
        final ReplicationApplier client = transport.client(null, "store", null, null, false);
        final ReplicationHealth health = transport.health(() -> true, client);
        final var positionProvider = transport.positionProvider("store");
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

    private static NodeSettingsSource properties(final Path root) {
        final UUID cluster = UUID.randomUUID();
        final UUID node = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        return new TestNodeProperties() {
            @Override
            public String replicationRole() {
                return "writer";
            }

            @Override
            public String replicationProperty(final String name) {
                return switch (name) {
                    case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> cluster.toString();
                    case "ECLIPSE_DATAGRID_AERON_NODE_ID" -> node.toString();
                    case "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> generation.toString();
                    case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> root.resolve("driver").toString();
                    case "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY" -> root.resolve("archive").toString();
                    case "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH" -> root.resolve("checkpoint/writer").toString();
                    case "ECLIPSE_DATAGRID_BACKUP_PATH" -> root.resolve("backups").toString();
                    case "ECLIPSE_DATAGRID_AERON_DRIVER_TIMEOUT_MILLIS" -> "250";
                    default -> null;
                };
            }
        };
    }
}
