package peruncs.datagrid.cluster.storage.types;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static peruncs.datagrid.cluster.storage.types.StorageBinaryDataMessage.MessageType.DATA;

/// Verifies the packet boundary used by the logical-message commit policy.
class StorageBinaryDataPacketAcceptorTest {
    private static StorageBinaryDataPacket packet(final int index, final int count, final byte[] bytes) {
        return StorageBinaryDataPacket.New(DATA, 2, index, count, ByteBuffer.wrap(bytes));
    }

    @Test
    void reportsIncompleteMessageUntilItsLastPacketIsAccepted() {
        final StorageBinaryDataPacketAcceptor acceptor = StorageBinaryDataPacketAcceptor.New(
                new StorageBinaryDataReceiver() {
                    @Override
                    public void receiveData(final org.eclipse.serializer.persistence.binary.types.Binary data) {
                    }

                    @Override
                    public void receiveTypeDictionary(final String data) {
                    }
                });

        assertTrue(acceptor.isAtMessageBoundary());
        acceptor.accept(List.of(packet(0, 2, new byte[]{1})));
        assertFalse(acceptor.isAtMessageBoundary());
        acceptor.accept(List.of(packet(1, 2, new byte[]{2})));
        assertTrue(acceptor.isAtMessageBoundary());
    }

    @Test
    void disposeDiscardsRetainedMessage() {
        final StorageBinaryDataPacketAcceptor acceptor = StorageBinaryDataPacketAcceptor.New(
                new StorageBinaryDataReceiver() {
                    @Override
                    public void receiveData(final org.eclipse.serializer.persistence.binary.types.Binary data) {
                    }

                    @Override
                    public void receiveTypeDictionary(final String data) {
                    }
                });

        acceptor.accept(List.of(packet(0, 2, new byte[]{1})));
        assertFalse(acceptor.isAtMessageBoundary());
        acceptor.dispose();
        assertTrue(acceptor.isAtMessageBoundary());
        acceptor.dispose();
    }
}
