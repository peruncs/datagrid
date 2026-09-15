package peruncs.datagrid.cluster.nodelibrary.replication;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.storage.distributed.types.StorageBinaryDataPacket;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static peruncs.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType.DATA;

/** Verifies that the cluster acceptor forwards packets and lifecycle to the merger. */
class ClusterStorageBinaryDataPacketAcceptorTest {
    private static StorageBinaryDataPacket packet(final int index, final int count, final byte[] bytes) {
        return StorageBinaryDataPacket.New(DATA, 2, index, count, ByteBuffer.wrap(bytes));
    }

    /** A complete message reaches the merger. */
    @Test
    void forwardsCompleteMessageToMerger() {
        final MergerFake merger = new MergerFake();
        final ClusterStorageBinaryDataPacketAcceptor acceptor = ClusterStorageBinaryDataPacketAcceptor.New(merger);

        acceptor.accept(List.of(packet(0, 2, new byte[]{1})));
        assertEquals(0, merger.received.get());
        acceptor.accept(List.of(packet(1, 2, new byte[]{2})));
        assertEquals(1, merger.received.get());
    }

    /** Dispose releases the acceptor and the merger. */
    @Test
    void disposeDisposesMerger() {
        final MergerFake merger = new MergerFake();
        final ClusterStorageBinaryDataPacketAcceptor acceptor = ClusterStorageBinaryDataPacketAcceptor.New(merger);

        acceptor.accept(List.of(packet(0, 2, new byte[]{1})));
        acceptor.dispose();

        assertTrue(merger.disposed.get());
        acceptor.dispose();
    }

    /** Records merger callbacks. */
    private static final class MergerFake implements ClusterStorageBinaryDataMerger {
        private final AtomicInteger received = new AtomicInteger();
        private final AtomicBoolean disposed = new AtomicBoolean(false);

        @Override
        public void receiveData(final Binary data) {
            this.received.incrementAndGet();
        }

        @Override
        public void receiveTypeDictionary(final String typeDictionaryData) {
        }

        @Override
        public void dispose() {
            this.disposed.set(true);
        }
    }
}
