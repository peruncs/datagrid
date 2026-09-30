package peruncs.cluster.errors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NodeExceptionOutcomeTest {
    @Test
    void mapsOnlyDocumentedRecoveryGuarantees() {
        assertEquals(NodeException.Outcome.RETRYABLE, new WriteRejectedException("not persisted").outcome());
        assertEquals(NodeException.Outcome.PENDING, new ReplicationPendingException(1L, null).outcome());
        assertEquals(NodeException.Outcome.RESEED_REQUIRED, new ReseedRequiredException("cannot resume").outcome());
        assertEquals(NodeException.Outcome.FAILED, new CorruptReplicationDataException("bad frame").outcome());
    }
}
