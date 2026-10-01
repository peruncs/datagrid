package peruncs.cluster.node.aeron;

import org.junit.jupiter.api.Test;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Verifies the reader reports its restart boundary as one captured pair, never a live read of the mark.
class AeronReaderBoundaryTest {
    private static ReplicationMark mark() {
        return new ReplicationMark(UUID.randomUUID(), UUID.randomUUID(), 1L, 5L);
    }

    @Test
    void aMarkWithoutACommitIsReportedAsUnknown() {
        final AeronReaderTransport transport = new AeronReaderTransport(null);
        transport.captureBoundary(mark());
        assertEquals(-1L, transport.appliedBoundary().sequence());
        assertEquals(-1L, transport.appliedBoundary().position());
    }

    @Test
    void theBoundaryStaysAtTheCapturedPairWhileTheMarkAdvances() {
        final AeronReaderTransport transport = new AeronReaderTransport(null);
        final ReplicationMark mark = mark();
        mark.reserve(5L, 1L, 7L, 4_096L);
        transport.captureBoundary(mark);
        mark.reserve(5L, 1L, 8L, 8_192L);
        assertEquals(7L, transport.appliedBoundary().sequence());
        assertEquals(4_096L, transport.appliedBoundary().position());
        transport.captureBoundary(mark);
        assertEquals(8L, transport.appliedBoundary().sequence());
        assertEquals(8_192L, transport.appliedBoundary().position());
    }
}
