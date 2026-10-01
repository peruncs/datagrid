package peruncs.cluster.errors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Pins the recovery hint of every exported failure type.
class NodeExceptionOutcomeTest {
    @Test
    void mapsEveryFailureTypeToItsDocumentedRecoveryHint() {
        assertEquals(NodeException.Outcome.RETRYABLE, new WriteRejectedException("not persisted").outcome());
        assertEquals(NodeException.Outcome.PENDING, new ReplicationPendingException(1L, null).outcome());
        assertEquals(NodeException.Outcome.RESEED_REQUIRED, new ReseedRequiredException("cannot resume").outcome());

        assertEquals(NodeException.Outcome.TRANSIENT, new BackupBusyException("busy").outcome());
        assertEquals(NodeException.Outcome.TRANSIENT, new StorageLimitReachedException("full").outcome());
        assertEquals(NodeException.Outcome.TRANSIENT, new ReplicationUnavailableException("archive down").outcome());
        assertEquals(NodeException.Outcome.TRANSIENT, new GraphDrainTimeoutException("slow section").outcome());
        assertEquals(NodeException.Outcome.TRANSIENT,
                new ReplicationPositionUnavailableException("no boundary yet").outcome());

        assertEquals(NodeException.Outcome.FAILED, new CorruptReplicationDataException("bad frame").outcome());
        assertEquals(NodeException.Outcome.FAILED, new ReaderWriteRejectedException("reader").outcome());
        assertEquals(NodeException.Outcome.FAILED, new GraphInvalidatedException("latched", null).outcome());
        assertEquals(NodeException.Outcome.FAILED, new WrongRoleException("role").outcome());
        assertEquals(NodeException.Outcome.FAILED, new IncompleteArchiveException("truncated").outcome());
        assertEquals(NodeException.Outcome.FAILED, new NodeException("generic").outcome());
    }
}
