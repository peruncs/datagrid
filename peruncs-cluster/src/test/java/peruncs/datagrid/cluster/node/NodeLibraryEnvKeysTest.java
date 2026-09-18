package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/// Covers environment-variable resolution: prefixed names win, pre-prefix
/// names still resolve as a fallback, and unknown names stay `null`.
class NodeLibraryEnvKeysTest {
    /// Verifies a prefixed environment name wins over its legacy unprefixed fallback when both are set.
    @Test
    void prefixedNameWinsOverLegacy() {
        final Map<String, String> environment = Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB, "64",
                "STORAGE_LIMIT_GB", "32");

        assertEquals("64", NodeLibraryPropertiesProvider.Env.resolve(
                environment, NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB));
    }

    /// Verifies legacy unprefixed names still resolve when the prefixed name is unset.
    @Test
    void legacyNameResolvesWhenPrefixedIsUnset() {
        assertEquals("32", NodeLibraryPropertiesProvider.Env.resolve(
                Map.of("STORAGE_LIMIT_GB", "32"),
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB));
        assertEquals("secret", NodeLibraryPropertiesProvider.Env.resolve(
                Map.of("MSCNL_PROD_MODE", "secret"),
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_PROD_MODE));
    }

    /// Verifies unknown or unset names resolve to null instead of a default value.
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

        /// Booleans accept only trimmed `true`/`false`; anything else fails like
    /// the integer and long parsers instead of silently reading `false`.
    @Test
    void booleanParsingIsStrictTrueOrFalse() {
        assertTrue(env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE, "true")).isBackupNode());
        assertTrue(env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE, " True ")).isBackupNode());
        assertFalse(env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE, "FALSE")).isBackupNode());
    }

        /// An unset or blank boolean reads absent (`false`) instead of failing.
    @Test
    void blankBooleanReadsAbsent() {
        assertFalse(env(Map.of()).isBackupNode());
        assertFalse(env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE, "   ")).isBackupNode());
    }

        /// Lenient spellings (`yes`, `1`, `on`) are rejected, not coerced to `false`.
    @Test
    void invalidBooleanIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE, "yes")).isBackupNode());
        assertThrows(IllegalArgumentException.class, () -> env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE, "1")).isBackupNode());
        assertThrows(IllegalArgumentException.class, () -> env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE, "on")).isBackupNode());
    }

        /// The storage limit accepts an optional `G` suffix and surrounding blanks.
    @Test
    void storageLimitSuffixAndBlanks() {
        assertEquals(64, env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB, "64G")).storageLimitGB());
        assertEquals(64, env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB, " 64g ")).storageLimitGB());
        assertEquals(64, env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB, " 64 ")).storageLimitGB());
        assertNull(env(Map.of()).storageLimitGB());
        assertNull(env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB, "  ")).storageLimitGB());
    }

        /// Malformed integers and limits fail instead of falling back to defaults.
    @Test
    void invalidIntegersAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB, "sixty-four")).storageLimitGB());
        assertThrows(IllegalArgumentException.class, () -> env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.KEPT_BACKUPS_COUNT, "many")).keptBackupsCount());
        assertThrows(IllegalArgumentException.class, () -> env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.DATA_MERGER_TIMEOUT_MS, "soon")).dataMergerTimeoutMs());
    }

        /// An unset role falls back to `writer`, or `backup-reader` on backup nodes.
    @Test
    void roleFallsBackWhenUnconfigured() {
        assertEquals(NodeLibraryPropertiesProvider.WRITER_ROLE, env(Map.of()).replicationRole());
        assertFalse(env(Map.of()).replicationRoleConfigured());
        assertEquals(NodeLibraryPropertiesProvider.BACKUP_READER_ROLE, env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE, "true")).replicationRole());
        assertEquals(NodeLibraryPropertiesProvider.READER_ROLE, env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.REPLICATION_ROLE, "reader")).replicationRole());
        assertTrue(env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.REPLICATION_ROLE, "reader")).replicationRoleConfigured());
    }

    private static NodeLibraryPropertiesProvider.Env env(final Map<String, String> environment) {
        return new NodeLibraryPropertiesProvider.Env(environment);
    }
}
