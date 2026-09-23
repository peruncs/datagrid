package peruncs.datagrid.cluster.node.aeron;

import io.aeron.archive.client.ArchiveEvent;
import io.aeron.archive.client.ArchiveException;
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
        assertTrue(AeronArchiveFailures.terminalControlResponseWarning(
                new ArchiveEvent(AERON_1_53_CONTROL_RESPONSE_DISCONNECTED)));
    }

        /// Verifies unrelated warnings and non-ArchiveEvent failures stay non-terminal.
    @Test
    void unrelatedFailuresAreNotTerminal() {
        assertFalse(AeronArchiveFailures.terminalControlResponseWarning(
                new ArchiveEvent("ERROR - some unrelated archive warning")));
        assertFalse(AeronArchiveFailures.terminalControlResponseWarning(
                new IllegalStateException(AERON_1_53_CONTROL_RESPONSE_DISCONNECTED)));
    }

    @Test
    void unavailableArchiveFailureCanBeNestedWithoutMatchingUnrelatedMessages() {
        assertTrue(AeronArchiveFailures.unavailable(new IllegalStateException("writer failed",
                new ArchiveException("archive failed", ArchiveException.GENERIC))));
        assertFalse(AeronArchiveFailures.unavailable(new IllegalStateException("unrelated failure")));
    }
}
