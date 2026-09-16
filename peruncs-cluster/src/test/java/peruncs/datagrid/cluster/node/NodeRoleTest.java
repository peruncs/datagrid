package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies the single normalized role every decision point uses.
class NodeRoleTest {
    @ParameterizedTest(name = "explicit={0} legacy={1} resolves {2}")
    @CsvSource({
            "writer,      false, WRITER",
            "reader,      false, READER",
            "backup-reader, false, BACKUP_READER",
            "backup-reader, true,  BACKUP_READER",
            " Writer ,    false, WRITER",
    })
    void explicitNewValueWins(final String configured, final boolean legacy, final NodeRole expected) {
        assertEquals(expected, NodeRole.resolve(configured, true, legacy));
    }

    @ParameterizedTest(name = "legacy={0} default={1} resolves {2}")
    @CsvSource({
            "false, '',      WRITER",
            "true,  '',      BACKUP_READER",
            "false, reader,  READER",
            "true,  writer,  BACKUP_READER",
    })
    void unconfiguredValueInheritsTheLegacyFlag(final boolean legacy, final String configured,
                                                final NodeRole expected) {
        final String value = configured.isEmpty() ? null : configured;
        assertEquals(expected, NodeRole.resolve(value, false, legacy));
    }

    @Test
    void conflictingLegacyAndNewValuesFail() {
        assertThrows(IllegalArgumentException.class, () -> NodeRole.resolve("writer", true, true));
        assertThrows(IllegalArgumentException.class, () -> NodeRole.resolve("reader", true, true));
        assertThrows(IllegalArgumentException.class, () -> NodeRole.resolve("archiver", true, false));
        assertThrows(IllegalArgumentException.class, () -> NodeRole.resolve("  ", true, false));
        assertThrows(IllegalArgumentException.class, () -> NodeRole.resolve(null, true, false));
    }

        /// A node configured only as `backup-reader` resolves the backup role
    /// without the legacy flag — the inconsistency that once sent it down the
    /// storage path while its transport configured a backup reader.
    @Test
    void backupReaderNeedsNoLegacyFlag() {
        final var properties = new NodeLibraryPropertiesProvider.Env(Map.of(
                "ECLIPSE_DATAGRID_REPLICATION_ROLE", "backup-reader"));

        assertEquals(NodeRole.BACKUP_READER, properties.nodeRole());
    }

    @Test
    void legacyBackupNodeStillResolvesWithoutTheNewSetting() {
        final var properties = new NodeLibraryPropertiesProvider.Env(Map.of(
                "ECLIPSE_DATAGRID_IS_BACKUP_NODE", "true"));

        assertEquals(NodeRole.BACKUP_READER, properties.nodeRole());
    }
}
