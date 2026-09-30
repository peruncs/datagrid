package peruncs.cluster.node.aeron;

import peruncs.cluster.api.NodeConfig;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/// Builds isolated typed configurations for node and Aeron tests.
public final class TestNodeConfig {
    private TestNodeConfig() {
    }

    public static NodeConfig aeron(final Path root, final String role, final boolean production,
                                   final Map<String, String> overrides) {
        final Map<String, String> values = base(root);
        values.put(NodeConfig.Setting.REPLICATION_TRANSPORT.key(), "aeron");
        values.put(NodeConfig.Setting.REPLICATION_ROLE.key(), role);
        values.put(NodeConfig.Setting.PROD_MODE.key(), Boolean.toString(production));
        values.put(NodeConfig.Setting.AERON_CLUSTER_ID.key(), UUID.randomUUID().toString());
        values.put(NodeConfig.Setting.AERON_NODE_ID.key(), UUID.randomUUID().toString());
        values.put(NodeConfig.Setting.AERON_STORE_GENERATION.key(), UUID.randomUUID().toString());
        values.putAll(overrides);
        return NodeConfig.fromMap(values);
    }

    public static NodeConfig local(final Path root, final Map<String, String> overrides) {
        final Map<String, String> values = base(root);
        values.put(NodeConfig.Setting.REPLICATION_TRANSPORT.key(), "none");
        values.put(NodeConfig.Setting.PROD_MODE.key(), "false");
        values.putAll(overrides);
        return NodeConfig.fromMap(values);
    }

    private static Map<String, String> base(final Path root) {
        final Map<String, String> values = new HashMap<>();
        values.put(NodeConfig.Setting.STORAGE_PATH.key(), root.toString());
        values.put(NodeConfig.Setting.BACKUP_PATH.key(), root.resolve("backups").toString());
        values.put(NodeConfig.Setting.STORAGE_LIMIT_GB.key(), "1");
        return values;
    }
}
