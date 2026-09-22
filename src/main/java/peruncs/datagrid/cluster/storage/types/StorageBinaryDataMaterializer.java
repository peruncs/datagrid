package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.collections.types.XGettingCollection;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataIterator;
import org.eclipse.serializer.persistence.binary.types.BinaryLoader;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.binary.types.LoadItemsChain;
import org.eclipse.serializer.persistence.types.PersistenceIdSet;
import org.eclipse.serializer.persistence.types.PersistenceManager;
import org.eclipse.serializer.persistence.types.Persister;
import org.eclipse.serializer.persistence.types.PersistenceSource;
import org.eclipse.serializer.persistence.types.PersistenceSourceSupplier;
import org.eclipse.serializer.persistence.types.PersistenceTypeHandlerLookup;
import org.eclipse.serializer.util.X;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageConnectionFoundation;
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
    static void materialize(final BinaryPersistenceFoundation<?> foundation,
                            final StorageConnection storage, final ByteBuffer[] buffers, final int length) {
        materialize(foundation, storage, buffers, 0, length);
    }

    /// Materializes one transaction slice from a reusable batch array.
    static void materialize(final BinaryPersistenceFoundation<?> foundation,
                            final StorageConnection storage, final ByteBuffer[] buffers,
                            final int offset, final int length) {
        Objects.requireNonNull(foundation, "foundation");
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(buffers, "buffers");
        if (offset < 0 || length < 0 || offset > buffers.length - length) {
            throw new IllegalArgumentException(
                    "materializer range out of bounds: offset=%s, length=%s".formatted(offset, length));
        }
        final int end = offset + length;
        final PersistenceManager<?> rawManager = storage.persistenceManager();
        /* Merger state-machine tests use a deliberately unwired connection;
         * production Store connections always expose their manager. Keep the
         * native-buffer validation below active for those stand-ins. */
        final PersistenceManager<Binary> manager = rawManager == null ? null : binaryManager(rawManager);
        final ObjectMaterializer materializer = manager == null ? null : new ObjectMaterializer(manager);
        for (int index = offset; index < end; index++) {
            final ByteBuffer buffer = buffers[index];
            if (buffer == null || !buffer.isDirect() || buffer.position() != 0) {
                throw new StorageBinaryDataException("materializer requires direct buffers at position zero");
            }
            if (buffer.limit() == 0) continue;
            final long address = getDirectByteBufferAddress(buffer);
            if (materializer != null) {
                ITERATOR.iterateEntityRawData(address, address + buffer.limit(), materializer);
            }
        }
        if (materializer == null) return;
        /* Store deliberately leaves already-loaded instances unchanged when
         * importData replaces their entities. Reading those ids through the
         * manager immediately after import can therefore return the old cache
         * payload. Point a fresh BinaryLoader at the received bytes for its
         * first fetch; unresolved references fall back to Store's normal
         * source. This uses Serializer's public loader pipeline, preserving
         * create/update/complete ordering without reflective access to its
         * package-private BinaryLoadItem constructors. */
        final ByteBuffer[] batch = java.util.Arrays.copyOfRange(buffers, offset, end);
        final PersistenceSourceSupplier<Binary> source = new ImportedBinarySource(manager, batch);
        final PersistenceTypeHandlerLookup<Binary> handlers =
                binaryHandlers(foundation.getTypeHandlerManager());
        if (handlers == null) return;
        final Persister persister = foundation.getPersister() == null
                ? manager : foundation.getPersister();
        final LoadItemsChain loadItems = foundation instanceof EmbeddedStorageConnectionFoundation<?> embedded
                ? new LoadItemsChain.ChannelHashing(
                        embedded.getStorageSystem().channelCountProvider().getChannelCount())
                : new LoadItemsChain.Simple();
        final BinaryLoader loader = BinaryLoader.New(
                handlers,
                manager.objectRegistry(), persister, source, loadItems, foundation.isByteOrderMismatch());
        materializer.materialize(loader);
    }

    @SuppressWarnings("unchecked")
    private static PersistenceManager<Binary> binaryManager(final PersistenceManager<?> manager) {
        return (PersistenceManager<Binary>) manager;
    }

    @SuppressWarnings("unchecked")
    private static PersistenceTypeHandlerLookup<Binary> binaryHandlers(final Object handlers) {
        return (PersistenceTypeHandlerLookup<Binary>) handlers;
    }

    private static final class ImportedBinarySource implements PersistenceSourceSupplier<Binary> {
        private final PersistenceManager<Binary> fallback;
        private final PersistenceSource<Binary> source;

        private ImportedBinarySource(final PersistenceManager<Binary> fallback, final ByteBuffer[] buffers) {
            this.fallback = fallback;
            this.source = new PersistenceSource<>() {
                private boolean supplied;

                @Override
                public XGettingCollection<? extends Binary> read() {
                    return this.takeImported(null);
                }

                @Override
                public XGettingCollection<? extends Binary> readByObjectIds(final PersistenceIdSet[] ids) {
                    return this.takeImported(ids);
                }

                private XGettingCollection<? extends Binary> takeImported(final PersistenceIdSet[] ids) {
                    if (!this.supplied) {
                        this.supplied = true;
                        return X.Enum(ChunksWrapper.New(buffers));
                    }
                    return ids == null ? fallback.source().read() : fallback.source().readByObjectIds(ids);
                }
            };
        }

        @Override
        public Object getObject(final long objectId) {
            return this.fallback.getObject(objectId);
        }

        @Override
        public PersistenceSource<Binary> source() {
            return this.source;
        }
    }
}
