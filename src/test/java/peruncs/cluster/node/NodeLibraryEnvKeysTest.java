package peruncs.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.cluster.api.NodeSettingsSource;
import peruncs.cluster.errors.NodeException;

import java.util.Map;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/// Covers environment-variable resolution: prefixed names win, pre-prefix
/// names still resolve as a fallback, and unknown names stay `null`.
class NodeLibraryEnvKeysTest {
    /// Verifies a prefixed environment name wins over its legacy unprefixed fallback when both are set.
    @Test
    void prefixedNameWinsOverLegacy() {
        final Map<String, String> environment = Map.of(
                NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB, "64",
                "STORAGE_LIMIT_GB", "32");

        assertEquals("64", env(environment).replicationProperty(
                NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB));
    }

    /// Verifies legacy unprefixed names still resolve when the prefixed name is unset.
    @Test
    void legacyNameResolvesWhenPrefixedIsUnset() {
        assertEquals("32", env(Map.of("STORAGE_LIMIT_GB", "32")).replicationProperty(
                NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB));
        assertEquals("secret", env(Map.of("MSCNL_PROD_MODE", "secret")).replicationProperty(
                NodeSettingsSource.EnvKeys.IS_PROD_MODE));
    }

    /// Verifies unknown or unset names resolve to null instead of a default value.
    @Test
    void unsetNameResolvesToNull() {
        assertNull(env(Map.of()).replicationProperty(
                NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB));
        assertNull(env(Map.of("STORAGE_LIMIT_GB", "32")).replicationProperty(
                NodeSettingsSource.EnvKeys.STORAGE_PATH));
        assertNull(env(Map.of("UNRELATED", "1")).replicationProperty(
                NodeSettingsSource.EnvKeys.IS_BACKUP_NODE));
    }

        /// Booleans accept only trimmed `true`/`false`; anything else fails like
    /// the integer and long parsers instead of silently reading `false`.
    @Test
    void booleanParsingIsStrictTrueOrFalse() {
        assertTrue(env(Map.of(
                NodeSettingsSource.EnvKeys.IS_BACKUP_NODE, "true")).isBackupNode());
        assertTrue(env(Map.of(
                NodeSettingsSource.EnvKeys.IS_BACKUP_NODE, " True ")).isBackupNode());
        assertFalse(env(Map.of(
                NodeSettingsSource.EnvKeys.IS_BACKUP_NODE, "FALSE")).isBackupNode());
    }

        /// An unset or blank boolean reads absent (`false`) instead of failing.
    @Test
    void blankBooleanReadsAbsent() {
        assertFalse(env(Map.of()).isBackupNode());
        assertFalse(env(Map.of(
                NodeSettingsSource.EnvKeys.IS_BACKUP_NODE, "   ")).isBackupNode());
    }

        /// Lenient spellings (`yes`, `1`, `on`) are rejected, not coerced to `false`.
    @Test
    void invalidBooleanIsRejected() {
        assertThrows(NodeException.class, () -> env(Map.of(
                NodeSettingsSource.EnvKeys.IS_BACKUP_NODE, "yes")).isBackupNode());
        assertThrows(NodeException.class, () -> env(Map.of(
                NodeSettingsSource.EnvKeys.IS_BACKUP_NODE, "1")).isBackupNode());
        assertThrows(NodeException.class, () -> env(Map.of(
                NodeSettingsSource.EnvKeys.IS_BACKUP_NODE, "on")).isBackupNode());
    }

        /// The storage limit accepts a `G` or `GB` suffix and surrounding blanks.
    @Test
    void storageLimitSuffixAndBlanks() {
        assertEquals(64, env(Map.of(
                NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB, "64G")).storageLimitGB());
        assertEquals(64, env(Map.of(
                NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB, "64GB")).storageLimitGB());
        assertEquals(64, env(Map.of(
                NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB, " 64g ")).storageLimitGB());
        assertEquals(64, env(Map.of(
                NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB, " 64 ")).storageLimitGB());
        assertNull(env(Map.of()).storageLimitGB());
        assertNull(env(Map.of(
                NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB, "  ")).storageLimitGB());
    }

        /// Malformed integers and limits fail instead of falling back to defaults.
    @Test
    void invalidIntegersAreRejected() {
        assertThrows(NodeException.class, () -> env(Map.of(
                NodeSettingsSource.EnvKeys.STORAGE_LIMIT_GB, "sixty-four")).storageLimitGB());
        assertThrows(NodeException.class, () -> env(Map.of(
                NodeSettingsSource.EnvKeys.KEPT_BACKUPS_COUNT, "many")).keptBackupsCount());
        assertThrows(NodeException.class, () -> env(Map.of(
                NodeSettingsSource.EnvKeys.DATA_MERGER_TIMEOUT_MS, "soon")).dataMergerTimeoutMs());
    }

    @Test
    void graphDrainTimeoutHasPositiveDefaultAndRejectsNonPositiveValues() {
        assertEquals(5_000L, env(Map.of()).graphDrainTimeoutMillis());
        assertEquals(2_500L, env(Map.of(
                NodeSettingsSource.EnvKeys.GRAPH_DRAIN_TIMEOUT_MILLIS, "2500")).graphDrainTimeoutMillis());
        assertThrows(NodeException.class, () -> env(Map.of(
                NodeSettingsSource.EnvKeys.GRAPH_DRAIN_TIMEOUT_MILLIS, "0")).graphDrainTimeoutMillis());
    }

    @Test
    void backupCloseTimeoutHasPositiveDefaultAndRejectsNonPositiveValues() {
        assertEquals(60_000L, env(Map.of()).backupCloseTimeoutMillis());
        assertEquals(120_000L, env(Map.of(
                NodeSettingsSource.EnvKeys.BACKUP_CLOSE_TIMEOUT_MILLIS, "120000"))
                .backupCloseTimeoutMillis());
        assertThrows(NodeException.class, () -> env(Map.of(
                NodeSettingsSource.EnvKeys.BACKUP_CLOSE_TIMEOUT_MILLIS, "0"))
                .backupCloseTimeoutMillis());
    }

    @Test
    void lifecycleAppliesTheRetentionIntervalDefaultAndValidation() {
        assertEquals(Duration.ofMinutes(1), NodeCollaborators.maintenanceInterval(
                env(Map.of()).aeronRetentionIntervalMinutes(),
                NodeSettingsSource.EnvKeys.AERON_RETENTION_INTERVAL_MINUTES, 1));
        final Integer configured = env(Map.of(
                NodeSettingsSource.EnvKeys.AERON_RETENTION_INTERVAL_MINUTES, "2"))
                .aeronRetentionIntervalMinutes();
        assertEquals(Duration.ofMinutes(2), NodeCollaborators.maintenanceInterval(
                configured, NodeSettingsSource.EnvKeys.AERON_RETENTION_INTERVAL_MINUTES, 1));
        assertThrows(NodeException.class, () -> NodeCollaborators.maintenanceInterval(
                env(Map.of(NodeSettingsSource.EnvKeys.AERON_RETENTION_INTERVAL_MINUTES, "0"))
                        .aeronRetentionIntervalMinutes(),
                NodeSettingsSource.EnvKeys.AERON_RETENTION_INTERVAL_MINUTES, 1));
    }

        /// An unset role is absent and resolves through [NodeRole] to the
    /// writer, or to the backup reader on legacy backup nodes.
    @Test
    void roleFallsBackWhenUnconfigured() {
        assertNull(env(Map.of()).replicationRole());
        assertEquals(NodeRole.WRITER, NodeRole.of(env(Map.of())));
        assertEquals(NodeRole.BACKUP_READER, NodeRole.of(env(Map.of(
                NodeSettingsSource.EnvKeys.IS_BACKUP_NODE, "true"))));
        assertEquals(NodeRole.READER, NodeRole.of(env(Map.of(
                NodeSettingsSource.EnvKeys.REPLICATION_ROLE, "reader"))));
    }

    private static NodeSettingsSource env(final Map<String, String> environment) {
        return NodeSettingsSource.env(environment);
    }
}
