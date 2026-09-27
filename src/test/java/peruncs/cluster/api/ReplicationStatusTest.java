package peruncs.cluster.api;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationStatusTest {
    @Test
    void unknownSequenceBoundariesProduceUnknownLag() {
        final ReplicationStatus status = new ReplicationStatus(
                ReplicationState.LIVE,
                OptionalLong.empty(),
                OptionalLong.of(12L),
                OptionalLong.empty(),
                new ReplicationStatus.WriterDurableBoundary(OptionalLong.empty(), OptionalLong.empty()),
                OptionalLong.empty());

        assertTrue(status.lagTransactions().isEmpty());
    }

    @Test
    void knownSequenceBoundariesProduceNonnegativeLag() {
        final ReplicationStatus status = new ReplicationStatus(
                ReplicationState.LIVE,
                OptionalLong.of(7L),
                OptionalLong.of(12L),
                OptionalLong.empty(),
                new ReplicationStatus.WriterDurableBoundary(OptionalLong.empty(), OptionalLong.empty()),
                OptionalLong.empty());

        assertEquals(OptionalLong.of(5L), status.lagTransactions());
    }
}
