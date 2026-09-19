package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataIterator;
import org.eclipse.store.storage.types.StorageConnection;

import java.nio.ByteBuffer;
import java.util.Objects;

import static org.eclipse.serializer.memory.XMemory.getDirectByteBufferAddress;

/// Applies imported native Store binary buffers to the object graph.
final class StorageBinaryDataMaterializer {
    /* The default iterator keeps no per-batch state (all iteration state is
     * local), so one instance serves every batch. The ObjectMaterializer below
     * stays per-batch: upstream requires one PersistenceLoader per operation. */
    private static final BinaryEntityRawDataIterator ITERATOR = BinaryEntityRawDataIterator.New();

    private StorageBinaryDataMaterializer() {
    }

        /// Materializes all entities in the populated prefix of a scratch array.
    ///
    /// The merger drains each batch into a reused scratch array that is
    /// usually larger than the batch; only the first `length` slots hold the
    /// batch, so only that prefix is read. Slots past `length` are ignored.
    ///
    /// @param storage Store connection owning the persistence manager
    /// @param buffers scratch array with the batch in its prefix
    /// @param length  number of populated prefix slots
    static void materialize(final StorageConnection storage, final ByteBuffer[] buffers, final int length) {
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(buffers, "buffers");
        if (length < 0 || length > buffers.length) {
            throw new IllegalArgumentException("materializer length out of range: %s".formatted(length));
        }
        final ObjectMaterializer materializer = new ObjectMaterializer(storage.persistenceManager());
        for (int index = 0; index < length; index++) {
            final ByteBuffer buffer = buffers[index];
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
