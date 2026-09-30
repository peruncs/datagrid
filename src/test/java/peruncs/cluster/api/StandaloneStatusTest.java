package peruncs.cluster.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Checks the public status contract when the node has no replication transport.
class StandaloneStatusTest {
    @Test
    void statusReportsStandaloneAndNotConfigured(@TempDir final Path root) {
        final NodeConfig settings = NodeConfig.fromMap(Map.of(
                NodeConfig.Setting.PROD_MODE.key(), "true",
                NodeConfig.Setting.STORAGE_PATH.key(), root.resolve("store-parent").toString(),
                NodeConfig.Setting.STORAGE_LIMIT_GB.key(), "1",
                NodeConfig.Setting.STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES.key(), "1"));

        try (final ClusterNode<Object> node = ClusterStorage.<Object>Foundation()
                .setRootSupplier(Object::new)
                .setNodeConfig(settings)
                .startNode()) {
            assertEquals(NodeRole.STANDALONE, node.status().role());
            assertEquals(ReplicationState.NOT_CONFIGURED, node.status().replication().state());
        }
    }
}
