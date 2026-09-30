package peruncs.cluster.node.aeron;

import io.aeron.archive.client.ArchiveException;
import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.ReplicationUnavailableException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

/// Verifies Archive recovery failures retain their typed cause.
class AeronWriterRecoveryTest {
    @Test
    void writerRecoveryPreservesArchiveFailureCause() {
        final ArchiveException archive = new ArchiveException("control disconnected", ArchiveException.GENERIC);
        final ReplicationUnavailableException mapped = assertInstanceOf(ReplicationUnavailableException.class,
                AeronWriterTransport.writerRecoveryFailure(archive));
        assertSame(archive, mapped.getCause());
    }
}
