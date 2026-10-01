package peruncs.cluster.node.aeron;

import io.aeron.archive.client.ArchiveException;
import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.errors.ReseedRequiredException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static peruncs.cluster.node.aeron.AeronWriterTransport.RecoveryOutcome.LATCH_FAILED;
import static peruncs.cluster.node.aeron.AeronWriterTransport.RecoveryOutcome.LATCH_RESEED;
import static peruncs.cluster.node.aeron.AeronWriterTransport.RecoveryOutcome.RETRY;
import static peruncs.cluster.node.aeron.AeronWriterTransport.recoveryOutcome;

/// Verifies the writer-recovery failure taxonomy and the consecutive-failure budget.
class AeronWriterRecoveryTest {
    private static final RuntimeException TRANSIENT = new ReplicationUnavailableException("archive down");

    @Test
    void writerRecoveryPreservesArchiveFailureCause() {
        final ArchiveException archive = new ArchiveException("control disconnected", ArchiveException.GENERIC);
        final ReplicationUnavailableException mapped = assertInstanceOf(ReplicationUnavailableException.class,
                AeronWriterTransport.classifyArchiveFailure("writer recovery", archive));
        assertSame(archive, mapped.getCause());
    }

    @Test
    void aTransientFailureIsRetriedUntilTheBudgetIsSpent() {
        assertEquals(RETRY, recoveryOutcome(TRANSIENT, true, 1, 3));
        assertEquals(RETRY, recoveryOutcome(TRANSIENT, true, 2, 3));
        assertEquals(LATCH_FAILED, recoveryOutcome(TRANSIENT, true, 3, 3));
        assertEquals(LATCH_FAILED, recoveryOutcome(TRANSIENT, true, 1, 1));
    }

    @Test
    void aFailedCloseLatchesOnTheFirstFailure() {
        assertEquals(LATCH_FAILED, recoveryOutcome(TRANSIENT, false, 1, 3));
    }

    @Test
    void aReseedAlwaysLatchesReseed() {
        final RuntimeException reseed = new ReseedRequiredException("recording missing");
        assertEquals(LATCH_RESEED, recoveryOutcome(reseed, true, 1, 3));
        assertEquals(LATCH_RESEED, recoveryOutcome(reseed, false, 1, 3));
    }

    @Test
    void aDefectLatchesFailedWithoutRetry() {
        assertEquals(LATCH_FAILED, recoveryOutcome(new IllegalArgumentException("bug"), true, 1, 3));
    }
}
