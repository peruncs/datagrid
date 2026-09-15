package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataIterator;
import org.eclipse.store.storage.types.StorageConnection;

import java.nio.ByteBuffer;

import static org.eclipse.serializer.memory.XMemory.getDirectByteBufferAddress;

/// Applies imported native Store binary buffers to the object graph.
final class StorageBinaryDataMaterializer {
    /* The default iterator keeps no per-batch state (all iteration state is
     * local), so one instance serves every batch. The ObjectMaterializer below
     * stays per-batch: upstream requires one PersistenceLoader per operation. */
    private static final BinaryEntityRawDataIterator ITERATOR = BinaryEntityRawDataIterator.New();

    private StorageBinaryDataMaterializer() {
    }

        /// Materializes all entities in the supplied buffers.
    ///
    /// @param storage Store connection owning the persistence manager
    /// @param buffers direct buffers imported into `storage`
    public static void materialize(final StorageConnection storage, final ByteBuffer[] buffers) {
        if (storage == null || buffers == null) throw new NullPointerException("storage and buffers");
        final ObjectMaterializer materializer = new ObjectMaterializer(storage.persistenceManager());
        for (final ByteBuffer buffer : buffers) {
            if (buffer == null || !buffer.isDirect() || buffer.position() != 0) {
                throw new StorageBinaryDataException("materializer requires direct buffers at position zero");
            }
            if (buffer.limit() == 0) continue;
            final long address = getDirectByteBufferAddress(buffer);
            ITERATOR.iterateEntityRawData(address, address + buffer.limit(), materializer);
        }
        materializer.materialize();
    }
}
