package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.typing.Disposable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/// The acceptor seam forwards complete-binary delivery to one receiver.
class StorageBinaryDataPacketAcceptorTest {
    @Test
    void forwardsCompleteBinaryDeliveryToReceiver() {
        final StubReceiver receiver = new StubReceiver();
        final StorageBinaryDataPacketAcceptor acceptor = StorageBinaryDataPacketAcceptor.New(receiver);
        final Binary binary = ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1, 2, 3}));

        acceptor.acceptData(binary);
        assertEquals(List.of("data"), receiver.calls);

        receiver.owned = true;
        assertTrue(acceptor.acceptDataOwned(binary));
        assertEquals(List.of("data", "owned"), receiver.calls);

        acceptor.acceptTypeDictionary("dictionary");
        assertEquals(List.of("data", "owned", "dictionary"), receiver.calls);

        acceptor.awaitApplied();
        assertEquals(List.of("data", "owned", "dictionary", "applied"), receiver.calls);

        assertNull(acceptor.failure());
        acceptor.dispose();
        assertTrue(receiver.disposed);
    }

    private static final class StubReceiver implements StorageBinaryDataReceiver, Disposable {
        private final List<String> calls = new ArrayList<>();
        private boolean owned;
        private boolean disposed;

        @Override
        public void receiveData(final Binary data) {
            this.calls.add("data");
        }

        @Override
        public boolean receiveDataOwned(final Binary data) {
            this.calls.add("owned");
            return this.owned;
        }

        @Override
        public boolean canReceiveDataOwned() {
            return this.owned;
        }

        @Override
        public void receiveTypeDictionary(final String typeDictionaryData) {
            this.calls.add("dictionary");
        }

        @Override
        public void awaitApplied() {
            this.calls.add("applied");
        }

        @Override
        public void dispose() {
            this.disposed = true;
        }
    }
}
