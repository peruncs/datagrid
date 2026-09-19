package peruncs.datagrid.cluster.node.aeron;

import io.aeron.archive.client.ArchiveException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Version-pins the retention probe for Aeron's replay-in-progress detach.
///
/// Aeron 1.53 reports an active-recording purge with [ArchiveException#ACTIVE_RECORDING]
/// but reports a purge blocked by a live replay as [ArchiveException#GENERIC]
/// with a producer-owned text. If a future Aeron version changes the text or
/// adds an error code, this test fails and the probe must be updated.
class AeronArchiveRetentionClassificationTest {
        /// Verifies the active-recording code defers retention.
    @Test
    void activeRecordingCodeDefersRetention() {
        assertTrue(AeronArchiveRetention.isReplayInProgressDetach(
                new ArchiveException("active recording", ArchiveException.ACTIVE_RECORDING)));
    }

        /// Verifies the pinned Aeron 1.53 replay-in-progress text defers retention.
    @Test
    void replayInProgressTextDefersRetention() {
        assertTrue(AeronArchiveRetention.isReplayInProgressDetach(
                new ArchiveException("GENERIC: invalid detach: replay in progress", ArchiveException.GENERIC)));
    }

        /// Verifies unrelated GENERIC failures stay fatal.
    @Test
    void unrelatedGenericFailureStaysFatal() {
        assertFalse(AeronArchiveRetention.isReplayInProgressDetach(
                new ArchiveException("invalid detach: something else", ArchiveException.GENERIC)));
        assertFalse(AeronArchiveRetention.isReplayInProgressDetach(
                new ArchiveException("unrelated", ArchiveException.UNKNOWN_RECORDING)));
    }
}
