package peruncs.datagrid.cluster.node.aeron;

import io.aeron.archive.client.ArchiveEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Version-pins the string probe for Aeron's control-response disconnect.
///
/// [ArchiveEvent] exposes no error code, so the provider must match Aeron's
/// producer-owned text. If a future Aeron version changes the wording this
/// test fails and the probe must be updated with the version it targets.
class AeronControlWarningTest {
    /// The Aeron 1.53 message that identifies a terminal control-response disconnect.
    private static final String AERON_1_53_CONTROL_RESPONSE_DISCONNECTED =
            "ERROR - control response publication is not connected";

        /// Verifies the pinned Aeron control-response text classifies as terminal.
    @Test
    void controlResponseDisconnectIsTerminal() {
        assertTrue(AeronClusterReplicationTransportProvider.isTerminalArchiveWarning(
                new ArchiveEvent(AERON_1_53_CONTROL_RESPONSE_DISCONNECTED)));
    }

        /// Verifies unrelated warnings and non-ArchiveEvent failures stay non-terminal.
    @Test
    void unrelatedFailuresAreNotTerminal() {
        assertFalse(AeronClusterReplicationTransportProvider.isTerminalArchiveWarning(
                new ArchiveEvent("ERROR - some unrelated archive warning")));
        assertFalse(AeronClusterReplicationTransportProvider.isTerminalArchiveWarning(
                new IllegalStateException(AERON_1_53_CONTROL_RESPONSE_DISCONNECTED)));
    }
}
