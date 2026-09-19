package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;

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

        assertEquals("64", env(environment).resolve(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB));
    }

    /// Verifies legacy unprefixed names still resolve when the prefixed name is unset.
    @Test
    void legacyNameResolvesWhenPrefixedIsUnset() {
        assertEquals("32", env(Map.of("STORAGE_LIMIT_GB", "32")).resolve(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB));
        assertEquals("secret", env(Map.of("MSCNL_PROD_MODE", "secret")).resolve(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_PROD_MODE));
    }

    /// Verifies unknown or unset names resolve to null instead of a default value.
    @Test
    void unsetNameResolvesToNull() {
        assertNull(env(Map.of()).resolve(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB));
        assertNull(env(Map.of("STORAGE_LIMIT_GB", "32")).resolve(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_PATH));
        assertNull(env(Map.of("UNRELATED", "1")).resolve(
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
        assertThrows(NodeLibraryException.class, () -> env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE, "yes")).isBackupNode());
        assertThrows(NodeLibraryException.class, () -> env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE, "1")).isBackupNode());
        assertThrows(NodeLibraryException.class, () -> env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE, "on")).isBackupNode());
    }

        /// The storage limit accepts a `G` or `GB` suffix and surrounding blanks.
    @Test
    void storageLimitSuffixAndBlanks() {
        assertEquals(64, env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB, "64G")).storageLimitGB());
        assertEquals(64, env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB, "64GB")).storageLimitGB());
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
        assertThrows(NodeLibraryException.class, () -> env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_LIMIT_GB, "sixty-four")).storageLimitGB());
        assertThrows(NodeLibraryException.class, () -> env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.KEPT_BACKUPS_COUNT, "many")).keptBackupsCount());
        assertThrows(NodeLibraryException.class, () -> env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.DATA_MERGER_TIMEOUT_MS, "soon")).dataMergerTimeoutMs());
    }

        /// An unset role is absent and resolves through [NodeRole] to the
    /// writer, or to the backup reader on legacy backup nodes.
    @Test
    void roleFallsBackWhenUnconfigured() {
        assertNull(env(Map.of()).replicationRole());
        assertEquals(NodeRole.WRITER, env(Map.of()).nodeRole());
        assertEquals(NodeRole.BACKUP_READER, env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.IS_BACKUP_NODE, "true")).nodeRole());
        assertEquals(NodeRole.READER, env(Map.of(
                NodeLibraryPropertiesProvider.Env.EnvKeys.REPLICATION_ROLE, "reader")).nodeRole());
    }

    private static NodeLibraryPropertiesProvider.Env env(final Map<String, String> environment) {
        return new NodeLibraryPropertiesProvider.Env(environment);
    }
}
