package peruncs.cluster.node.aeron;

import io.aeron.archive.client.ArchiveException;
import io.aeron.exceptions.AeronException;
import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.errors.ReseedRequiredException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

/// Verifies that only proof of an unusable recording requires a reseed.
class AeronArchiveFailureClassificationTest {
    @Test
    void aTimeoutOrUnreachableArchiveIsUnavailableNotReseed() {
        assertInstanceOf(ReplicationUnavailableException.class, AeronWriterTransport.classifyArchiveFailure(
                "inspect", new ArchiveException("timeout", ArchiveException.GENERIC)));
        assertInstanceOf(ReplicationUnavailableException.class, AeronWriterTransport.classifyArchiveFailure(
                "inspect", new AeronException("driver gone")));
        assertInstanceOf(ReplicationUnavailableException.class, AeronWriterTransport.classifyArchiveFailure(
                "extend", new IllegalStateException("Aeron recording is still active: 3")));
    }

    @Test
    void aMissingOrIncompatibleRecordingRequiresReseed() {
        assertInstanceOf(ReseedRequiredException.class, AeronWriterTransport.classifyArchiveFailure(
                "inspect", new ArchiveException("gone", ArchiveException.UNKNOWN_RECORDING)));
        assertInstanceOf(ReseedRequiredException.class, AeronWriterTransport.classifyArchiveFailure(
                "extend", new IllegalArgumentException("Aeron recording framing does not match")));
    }

    @Test
    void typedFailuresPassThroughUnchanged() {
        final ReseedRequiredException reseed = new ReseedRequiredException("history diverged");
        assertSame(reseed, AeronWriterTransport.classifyArchiveFailure("scan", reseed));
        final ReplicationUnavailableException unavailable = new ReplicationUnavailableException("replay timed out");
        assertSame(unavailable, AeronWriterTransport.classifyArchiveFailure("scan", unavailable));
    }

    @Test
    void anUnexpectedDefectIsNeitherReseedNorTransient() {
        final UnsupportedOperationException defect = new UnsupportedOperationException("bug");
        assertSame(defect, AeronWriterTransport.classifyArchiveFailure("scan", defect));
    }
}
