package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/// Covers environment-variable resolution: prefixed names win, pre-prefix
/// names still resolve as a fallback, and unknown names stay `null`.
class NodeLibraryEnvKeysTest {
    @Test
    void prefixedNameWinsOverLegacy() {
        final Map<String, String> environment = Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB, "64",
                "STORAGE_LIMIT_GB", "32");

        assertEquals("64", NodeLibraryPropertiesProvider.Env.resolve(
                environment, NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB));
    }

    @Test
    void legacyNameResolvesWhenPrefixedIsUnset() {
        assertEquals("32", NodeLibraryPropertiesProvider.Env.resolve(
                Map.of("STORAGE_LIMIT_GB", "32"),
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB));
        assertEquals("secret", NodeLibraryPropertiesProvider.Env.resolve(
                Map.of("MSCNL_PROD_MODE", "secret"),
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_PROD_MODE));
    }

    @Test
    void unsetNameResolvesToNull() {
        assertNull(NodeLibraryPropertiesProvider.Env.resolve(
                Map.of(), NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB));
        assertNull(NodeLibraryPropertiesProvider.Env.resolve(
                Map.of("STORAGE_LIMIT_GB", "32"),
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_PATH));
        assertNull(NodeLibraryPropertiesProvider.Env.resolve(
                Map.of("UNRELATED", "1"),
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE));
    }
}
