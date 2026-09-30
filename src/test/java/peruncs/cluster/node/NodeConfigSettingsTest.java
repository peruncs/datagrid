package peruncs.cluster.node;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.api.NodeRole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/// Covers typed setting parsing, defaults, and the README settings table.
class NodeConfigSettingsTest {
    @Test
    void ignoresUnprefixedAndRetiredAliases() {
        final NodeConfig config = NodeConfig.fromMap(Map.of(
                "STORAGE_LIMIT_GB", "32",
                "MSCNL_PROD_MODE", "true"));
        assertNull(config.storage().limitBytes());
        assertFalse(config.productionMode());
        assertEquals(64_000_000_000L, NodeConfig.fromMap(Map.of(
                NodeConfig.Setting.STORAGE_LIMIT_GB.key(), "64",
                "STORAGE_LIMIT_GB", "32")).storage().limitBytes());
    }

    @Test
    void acceptsStorageLimitSuffixesAndBlanksUseTheDefault() {
        for (final String value : new String[]{"64G", "64GB", " 64g ", " 64 "}) {
            assertEquals(64_000_000_000L, NodeConfig.fromMap(Map.of(
                    NodeConfig.Setting.STORAGE_LIMIT_GB.key(), value)).storage().limitBytes());
        }
        assertNull(NodeConfig.fromMap(Map.of()).storage().limitBytes());
        assertNull(NodeConfig.fromMap(Map.of(NodeConfig.Setting.STORAGE_LIMIT_GB.key(), "  "))
                .storage().limitBytes());
    }

    @Test
    void rejectsMalformedAndOutOfRangeValues() {
        assertThrows(IllegalArgumentException.class, () -> config(NodeConfig.Setting.STORAGE_LIMIT_GB, "sixty-four"));
        assertThrows(IllegalArgumentException.class, () -> config(NodeConfig.Setting.KEPT_BACKUPS_COUNT, "many"));
        assertThrows(IllegalArgumentException.class, () -> config(NodeConfig.Setting.DATA_MERGER_CACHE_TIMEOUT_MILLIS, "soon"));
        assertThrows(IllegalArgumentException.class, () -> config(NodeConfig.Setting.GRAPH_DRAIN_TIMEOUT_MILLIS, "0"));
        assertThrows(IllegalArgumentException.class, () -> config(NodeConfig.Setting.BACKUP_CLOSE_TIMEOUT_MILLIS, "0"));
    }

    @Test
    void parsesLifecycleTimeoutsAndRetentionInterval() {
        assertEquals(Duration.ofSeconds(5), NodeConfig.fromMap(Map.of()).timeouts().graphDrain());
        assertEquals(Duration.ofMillis(2_500), config(NodeConfig.Setting.GRAPH_DRAIN_TIMEOUT_MILLIS, "2500")
                .timeouts().graphDrain());
        assertEquals(Duration.ofSeconds(60), NodeConfig.fromMap(Map.of()).backup().closeTimeout());
        assertEquals(Duration.ofSeconds(120), config(NodeConfig.Setting.BACKUP_CLOSE_TIMEOUT_MILLIS, "120000")
                .backup().closeTimeout());
        assertEquals(Duration.ofMinutes(1), NodeConfig.fromMap(Map.of()).aeron().retentionInterval());
        assertEquals(Duration.ofMinutes(2), NodeConfig.fromMap(Map.of(
                NodeConfig.Setting.REPLICATION_TRANSPORT.key(), "aeron",
                NodeConfig.Setting.AERON_RETENTION_INTERVAL_MINUTES.key(), "2"))
                .aeron().retentionInterval());
        assertEquals(Duration.ofMinutes(1), NodeCollaborators.maintenanceInterval(
                NodeConfig.fromMap(Map.of()).aeron().retentionInterval(), 1));
    }

    @Test
    void parsesMaintenanceAndBackupOperationBounds() {
        assertEquals(NodeConfig.fromMap(Map.of()).operations(), NodeConfig.Operations.DEFAULT,
                "programmatic factory defaults must match the documented setting defaults");

        final NodeConfig.Operations operations = NodeConfig.fromMap(Map.of(
                NodeConfig.Setting.BACKUP_STOP_TIMEOUT_MILLIS.key(), "9000",
                NodeConfig.Setting.BACKUP_STOP_POLL_INTERVAL_MILLIS.key(), "25",
                NodeConfig.Setting.BACKUP_RETENTION_RETRY_ATTEMPTS.key(), "5",
                NodeConfig.Setting.BACKUP_RETENTION_RETRY_DELAY_MILLIS.key(), "20",
                NodeConfig.Setting.BACKUP_PUBLICATION_RETRY_ATTEMPTS.key(), "7",
                NodeConfig.Setting.MAINTENANCE_FAILURE_THRESHOLD.key(), "4",
                NodeConfig.Setting.MAINTENANCE_CLOSE_TIMEOUT_MILLIS.key(), "8000",
                NodeConfig.Setting.STORAGE_CHECK_CLOSE_TIMEOUT_MILLIS.key(), "6000"
        )).operations();

        assertEquals(Duration.ofSeconds(9), operations.backupStopTimeout());
        assertEquals(Duration.ofMillis(25), operations.backupStopPollInterval());
        assertEquals(5, operations.backupRetentionRetryAttempts());
        assertEquals(Duration.ofMillis(20), operations.backupRetentionRetryDelay());
        assertEquals(7, operations.backupPublicationRetryAttempts());
        assertEquals(4, operations.maintenanceFailureThreshold());
        assertEquals(Duration.ofSeconds(8), operations.maintenanceCloseTimeout());
        assertEquals(Duration.ofSeconds(6), operations.storageCheckCloseTimeout());
    }

    @Test
    void resolvesStandaloneAndAeronRolesOnce() {
        assertEquals(NodeRole.STANDALONE, NodeConfig.fromMap(Map.of()).role());
        assertEquals(NodeRole.WRITER, NodeConfig.fromMap(Map.of(
                NodeConfig.Setting.REPLICATION_TRANSPORT.key(), "aeron")).role());
        assertEquals(NodeRole.READER, NodeConfig.fromMap(Map.of(
                NodeConfig.Setting.REPLICATION_TRANSPORT.key(), "aeron",
                NodeConfig.Setting.REPLICATION_ROLE.key(), "reader")).role());
        assertThrows(IllegalArgumentException.class, () -> config(NodeConfig.Setting.REPLICATION_ROLE, "reader"));
    }

    @Test
    void aeronSettingsRequireTheAeronTransport() {
        assertThrows(IllegalArgumentException.class, () -> config(NodeConfig.Setting.AERON_TERM_LENGTH, "16777216"));
        assertDoesNotThrow(() -> config(NodeConfig.Setting.AERON_TERM_LENGTH, " "));
    }

    @Test
    void queueBackpressureThresholdCannotExceedItsHardCap() {
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig.Limits(
                1, 0L, 2L, 1L, 16_777_216, 1_408, 131_072, 67_108_864));
    }

    @ParameterizedTest
    @EnumSource(NodeConfig.Setting.class)
    void eachSettingHasOnePrefixedKeyAndAnExercisedDefault(final NodeConfig.Setting setting) {
        assertTrue(setting.key().startsWith("PERUNCS_"), setting.name());
        assertTrue(NodeConfig.settingsMarkdown().contains("| `" + setting.key() + "` |"), setting.name());
        if (setting.defaultValue() == null) return;

        final Map<String, String> values = new HashMap<>();
        if (setting == NodeConfig.Setting.REPLICATION_TRANSPORT) {
            values.put(setting.key(), setting.defaultValue());
        } else {
            values.put(NodeConfig.Setting.PROD_MODE.key(), "true");
            values.put(NodeConfig.Setting.REPLICATION_TRANSPORT.key(), "aeron");
            values.put(NodeConfig.Setting.REPLICATION_ROLE.key(), "writer");
            values.put(setting.key(), setting.defaultValue());
        }
        assertDoesNotThrow(() -> NodeConfig.fromMap(values), setting.name());
    }

    @Test
    void settingKeysAreUnique() {
        final Map<String, String> keys = new HashMap<>();
        for (final NodeConfig.Setting setting : NodeConfig.Setting.values()) {
            assertNull(keys.put(setting.key(), setting.name()), "duplicate key: " + setting.key());
        }
    }

    @Test
    void readmeContainsTheGeneratedSettingsTable() throws IOException {
        assertTrue(Files.readString(Path.of("README.md")).contains(NodeConfig.settingsMarkdown()));
    }

    private static NodeConfig config(final NodeConfig.Setting setting, final String value) {
        return NodeConfig.fromMap(Map.of(setting.key(), value));
    }
}
