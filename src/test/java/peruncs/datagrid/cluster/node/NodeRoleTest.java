package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies the single normalized role every decision point uses.
class NodeRoleTest {
    /// Verifies an explicitly configured role value wins and normalizes cleanly, including surrounding blanks and casing.
    @ParameterizedTest(name = "explicit={0} legacy={1} resolves {2}")
    @CsvSource({
            "writer,      false, WRITER",
            "reader,      false, READER",
            "backup-reader, false, BACKUP_READER",
            "backup-reader, true,  BACKUP_READER",
            " Writer ,    false, WRITER",
    })
    void explicitNewValueWins(final String configured, final boolean legacy, final NodeRole expected) {
        assertEquals(expected, NodeRole.resolve(configured, legacy));
    }

    /// Verifies an unconfigured or blank role inherits the legacy backup flag.
    @ParameterizedTest(name = "legacy={0} configured={1} resolves {2}")
    @CsvSource({
            "false, '',      WRITER",
            "true,  '',      BACKUP_READER",
            "false, reader,  READER",
    })
    void unconfiguredValueInheritsTheLegacyFlag(final boolean legacy, final String configured,
                                                final NodeRole expected) {
        final String value = configured.isEmpty() ? null : configured;
        assertEquals(expected, NodeRole.resolve(value, legacy));
    }

    /// Verifies conflicting legacy and new role settings plus unknown values are rejected with an error.
    @Test
    void conflictingLegacyAndNewValuesFail() {
        assertThrows(IllegalArgumentException.class, () -> NodeRole.resolve("writer", true));
        assertThrows(IllegalArgumentException.class, () -> NodeRole.resolve("reader", true));
        assertThrows(IllegalArgumentException.class, () -> NodeRole.resolve("archiver", false));
        assertThrows(IllegalArgumentException.class, () -> NodeRole.resolve("nonsense", false));
    }

        /// A node configured only as `backup-reader` resolves the backup role
    /// without the legacy flag — the inconsistency that once sent it down the
    /// storage path while its transport configured a backup reader.
    @Test
    void backupReaderNeedsNoLegacyFlag() {
        final var properties = new NodeSettingsSource.Env(Map.of(
                "ECLIPSE_DATAGRID_REPLICATION_ROLE", "backup-reader"));

        assertEquals(NodeRole.BACKUP_READER, properties.nodeRole());
    }

    /// Verifies the legacy backup-node flag alone still resolves to the backup-reader role without the new setting.
    @Test
    void legacyBackupNodeStillResolvesWithoutTheNewSetting() {
        final var properties = new NodeSettingsSource.Env(Map.of(
                "ECLIPSE_DATAGRID_IS_BACKUP_NODE", "true"));

        assertEquals(NodeRole.BACKUP_READER, properties.nodeRole());
    }
}
