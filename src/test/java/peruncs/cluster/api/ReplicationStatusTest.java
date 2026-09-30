package peruncs.cluster.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationStatusTest {
    @Test
    void unknownSequenceBoundariesProduceUnknownLag() {
        final ReplicationStatus status = new ReplicationStatus(
                ReplicationState.LIVE,
                -1L,
                12L,
                -1L,
                -1L,
                -1L,
                -1L);

        assertFalse(status.hasLagTransactions());
        assertEquals(-1L, status.lagTransactions());
    }

    @Test
    void knownSequenceBoundariesProduceNonnegativeLag() {
        final ReplicationStatus status = new ReplicationStatus(
                ReplicationState.LIVE,
                7L,
                12L,
                -1L,
                -1L,
                -1L,
                -1L);

        assertTrue(status.hasLagTransactions());
        assertEquals(5L, status.lagTransactions());
    }

    @Test
    void rejectsValuesBelowUnknownSentinel() {
        assertThrows(IllegalArgumentException.class, () -> new ReplicationStatus(
                ReplicationState.LIVE,
                -2L,
                -1L,
                -1L,
                -1L,
                -1L,
                -1L));
    }

    @Test
    void notConfiguredStatusUsesUnknownSentinels() {
        final ReplicationStatus status = ReplicationStatus.notConfigured();

        assertEquals(ReplicationState.NOT_CONFIGURED, status.state());
        assertEquals(-1L, status.currentSequence());
        assertEquals(-1L, status.latestSequence());
        assertEquals(-1L, status.archiveUsableBytes());
        assertEquals(-1L, status.writerDurablePosition());
        assertEquals(-1L, status.writerDurableSequence());
        assertEquals(-1L, status.appliedSequence());
        assertFalse(status.hasLagTransactions());
    }
}
