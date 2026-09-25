package peruncs.cluster.node.aeron;

import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.ReseedRequiredException;

import static org.junit.jupiter.api.Assertions.*;


/// Verifies that archive gaps and tails select the documented recovery policy.
class AeronCrashRecoveryPolicyTest {
    private static final AeronWriterBoundary BOUNDARY = new AeronWriterBoundary(1, 7, 100);

        /// Verifies exact archive prefix can be extended.
    @Test
    void exactArchivePrefixCanBeExtended() {
        assertDoesNotThrow(() -> BOUNDARY.validateArchiveStop(100));
    }

        /// Verifies archive ahead fails closed as reseed required.
    @Test
    void archiveAheadFailsClosedAsReseedRequired() {
        final ReseedRequiredException failure = assertThrows(ReseedRequiredException.class,
                () -> BOUNDARY.validateArchiveStop(101));
        assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED:"));
    }

        /// Verifies archive behind fails closed as reseed required.
    @Test
    void archiveBehindFailsClosedAsReseedRequired() {
        final ReseedRequiredException failure = assertThrows(ReseedRequiredException.class,
                () -> BOUNDARY.validateArchiveStop(99));
        assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED:"));
    }

        /// Verifies active recording cannot be extended from checkpoint.
    @Test
    void activeRecordingCannotBeExtendedFromCheckpoint() {
        final ReseedRequiredException failure = assertThrows(ReseedRequiredException.class,
                () -> BOUNDARY.validateArchiveStop(-1));
        assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED:"));
    }
}
