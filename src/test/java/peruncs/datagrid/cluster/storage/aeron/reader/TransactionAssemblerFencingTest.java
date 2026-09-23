package peruncs.datagrid.cluster.storage.aeron.reader;

import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelopeTestSupport;
import peruncs.datagrid.cluster.storage.binary.StorageBinaryDataReceiver;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the stale-token floor moves only after a frame is fully accepted.
///
/// A poison frame carrying a higher token but an invalid sequence or checksum
/// must fail without lifting the floor: the floor persists through
/// checkpointed cursors, so an early adoption would make the poison durable
/// across restarts. Data chunks adopt their token at commit time, when the
/// assembled transaction validates as a whole.
class TransactionAssemblerFencingTest {
    private static final UUID CLUSTER = UUID.randomUUID();
    private static final long EPOCH = 17;

    private static TransactionAssembler assembler(final StorageBinaryDataReceiver receiver) {
        return TransactionAssemblerTestSupport.New(
                AeronReplicationConfiguration.builder()/* direct-accept fixture: keep the barrier at one transaction */.readerBarrierMaxTransactions(1)                        .termLength(64 * 1024)
                        .chunkSize(256)
                        .maxTransactionBytes(1024)
                        .build(),
                CLUSTER,
                EPOCH,
                receiver);
    }

    private static byte[] frame(
            final AeronReplicationEnvelope.Kind kind,
            final long fencingToken,
            final long sequence,
            final byte[] payload,
            final int payloadLength,
            final int commitCrc32c
    ) {
        return AeronReplicationEnvelopeTestSupport.encode(
                CLUSTER, EPOCH, fencingToken, sequence, kind, payloadLength, 0, 1, 0, commitCrc32c, payload);
    }

    private static void accept(final TransactionAssembler assembler, final byte[] bytes) {
        assembler.onFragment(new UnsafeBuffer(bytes), 0, bytes.length, null);
    }

        /// Verifies a poison frame with a higher token fails without lifting the floor.
    @Test
    void poisonFrameWithHigherTokenDoesNotRaiseFloor() {
        final CountingReceiver receiver = new CountingReceiver();
        final TransactionAssembler assembler = assembler(receiver);
        try {
            assembler.startingFencingToken(5L);
            final byte[] data = {1, 2};
            final byte[] poison = frame(AeronReplicationEnvelope.Kind.STORE_BINARY, 9L, 5L,
                    data, data.length, 0);
            assertThrows(IllegalStateException.class, () -> accept(assembler, poison));
            assertNotNull(assembler.failure(), "poison frame must latch the terminal failure");
            assertEquals(5L, assembler.fencingToken(),
                    "a rejected frame must not lift the persisted token floor");
            assertEquals(0, receiver.dataCalls, "no Store delivery may happen for a rejected frame");
        } finally {
            assembler.dispose();
        }
    }

        /// Verifies a commit adopts its token only after the transaction validates.
    @Test
    void commitAdoptsHigherTokenAfterFullValidation() {
        final CountingReceiver receiver = new CountingReceiver();
        final TransactionAssembler assembler = assembler(receiver);
        try {
            assembler.startingFencingToken(5L);
            final byte[] data = {1, 2};
            accept(assembler, frame(AeronReplicationEnvelope.Kind.STORE_BINARY, 7L, 0L,
                    data, data.length, 0));
            assertEquals(5L, assembler.fencingToken(),
                    "buffered data must not lift the floor before its commit validates");
            accept(assembler, frame(AeronReplicationEnvelope.Kind.COMMIT, 7L, 0L,
                    new byte[0], data.length, AeronReplicationEnvelope.crc32c(data)));
            assertEquals(7L, assembler.fencingToken());
            assertEquals(1, receiver.dataCalls);
            final byte[] stale = frame(AeronReplicationEnvelope.Kind.STORE_BINARY, 5L, 1L,
                    data, data.length, 0);
            final var failure = assertThrows(IllegalStateException.class, () -> accept(assembler, stale));
            assertTrue(failure.getMessage().contains("stale writer fencing token"),
                    "stale token must fail closed, was: %s".formatted(failure.getMessage()));
        } finally {
            assembler.dispose();
        }
    }

    private static final class CountingReceiver implements StorageBinaryDataReceiver {
        private int dataCalls;

        @Override
        public void receiveData(final Binary value) {
            final ByteBuffer buffer = value.buffers()[0].duplicate();
            buffer.flip();
            final byte[] copy = new byte[buffer.remaining()];
            buffer.get(copy);
            this.dataCalls++;
        }

        @Override
        public void receiveTypeDictionary(final String value) {
        }
    }
}
