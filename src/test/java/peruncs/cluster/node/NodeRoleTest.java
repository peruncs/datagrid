package peruncs.cluster.node;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import peruncs.cluster.api.NodeRole;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the single normalized role every decision point uses.
class NodeRoleTest {
    @ParameterizedTest
    @CsvSource({
            "writer, WRITER",
            "reader, READER",
            "backup-reader, BACKUP_READER",
            "' Writer ', WRITER"
    })
    void parsesConfiguredRole(final String configured, final NodeRole expected) {
        assertEquals(expected, NodeRole.of(configured));
    }

    @Test
    void defaultsToWriterAndIgnoresRemovedBackupFlag() {
        assertEquals(NodeRole.WRITER, NodeRole.of(null));
        assertEquals(NodeRole.WRITER, NodeRole.of(" "));
    }

    @Test
    void disabledTransportUsesStandaloneRole() {
        assertEquals(NodeRole.STANDALONE, NodeRole.resolve("none", null));
        assertEquals(NodeRole.STANDALONE, NodeRole.resolve(null, null));
        assertEquals(NodeRole.WRITER, NodeRole.resolve("aeron", "writer"));
        assertTrue(NodeRole.STANDALONE.canWrite());
        assertFalse(NodeRole.STANDALONE.isWriter());
    }

    @Test
    void rejectsUnknownConfiguredRole() {
        assertThrows(IllegalArgumentException.class, () -> NodeRole.of("nonsense"));
    }
}
