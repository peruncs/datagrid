package peruncs.datagrid.cluster.storage.binary;

import org.eclipse.serializer.collections.types.XGettingCollection;
import org.eclipse.serializer.persistence.binary.types.*;
import org.eclipse.serializer.persistence.types.*;
import org.eclipse.serializer.util.X;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageConnectionFoundation;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.errors.CorruptReplicationDataException;

import java.nio.ByteBuffer;
import java.util.Objects;

import static org.eclipse.serializer.memory.XMemory.getDirectByteBufferAddress;

/// Applies imported native Store binary buffers to the object graph.
final class StorageBinaryDataMaterializer {
    /* The merger calls this on one worker. BinaryLoader clears its load items
     * after each successful collect, so its source and scratch can be reused. */
    private static final BinaryEntityRawDataIterator ITERATOR = BinaryEntityRawDataIterator.New();
    private ByteBuffer[] batchViews = new ByteBuffer[0];
    private PersistenceManager<Binary> boundManager;
    private BinaryPersistenceFoundation<?> boundFoundation;
    private ObjectMaterializer objectMaterializer;
    private ImportedBinarySource importedSource;
    private BinaryLoader loader;

    StorageBinaryDataMaterializer() {
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
    void materialize(final BinaryPersistenceFoundation<?> foundation,
                            final StorageConnection storage, final ByteBuffer[] buffers, final int length) {
        materialize(foundation, storage, buffers, 0, length);
    }

    /// Materializes one transaction slice from a reusable batch array.
    void materialize(final BinaryPersistenceFoundation<?> foundation,
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
        if (manager != null && (manager != this.boundManager || foundation != this.boundFoundation)) {
            this.bind(foundation, manager);
        }
        final ObjectMaterializer materializer = manager == null ? null : this.objectMaterializer;
        if (materializer != null) materializer.clearCollected();
        for (int index = offset; index < end; index++) {
            final ByteBuffer buffer = buffers[index];
            if (buffer == null || !buffer.isDirect() || buffer.position() != 0) {
                throw new CorruptReplicationDataException("materializer requires direct buffers at position zero");
            }
            if (buffer.limit() == 0) continue;
            final long address = getDirectByteBufferAddress(buffer);
            if (materializer != null) {
                ITERATOR.iterateEntityRawData(address, address + buffer.limit(), materializer);
            }
        }
        if (materializer == null) return;
        if (this.loader == null) {
            materializer.clearCollected();
            return;
        }
        /* The imported bytes must be the loader's first source; Store's normal
         * source can still hold the old cached version of an updated object. */
        /* The imported source consumes the whole array, so the Store API pins
         * the views to the transaction's exact buffer count; a grow-only
         * array would smuggle a stale null tail into the import. */
        if (this.batchViews.length != length) this.batchViews = new ByteBuffer[length];
        final ByteBuffer[] batch = this.batchViews;
        System.arraycopy(buffers, offset, batch, 0, length);
        this.importedSource.begin(batch);
        try {
            materializer.materialize(this.loader);
        } finally {
            this.importedSource.end();
            java.util.Arrays.fill(batch, null);
        }
    }

    private void bind(final BinaryPersistenceFoundation<?> foundation, final PersistenceManager<Binary> manager) {
        final PersistenceTypeHandlerLookup<Binary> handlers = binaryHandlers(foundation.getTypeHandlerManager());
        this.objectMaterializer = new ObjectMaterializer(manager);
        this.boundManager = manager;
        this.boundFoundation = foundation;
        // State-machine fixtures have a manager but no configured Store loader.
        if (handlers == null) {
            this.loader = null;
            this.importedSource = null;
            return;
        }
        final Persister persister = foundation.getPersister() == null ? manager : foundation.getPersister();
        final LoadItemsChain loadItems = foundation instanceof EmbeddedStorageConnectionFoundation<?> embedded
                ? new LoadItemsChain.ChannelHashing(
                        embedded.getStorageSystem().channelCountProvider().getChannelCount())
                : new LoadItemsChain.Simple();
        this.importedSource = new ImportedBinarySource(manager);
        this.loader = BinaryLoader.New(handlers, manager.objectRegistry(), persister,
                this.importedSource, loadItems, foundation.isByteOrderMismatch());
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
        private ByteBuffer[] buffers;
        private boolean supplied;

        private ImportedBinarySource(final PersistenceManager<Binary> fallback) {
            this.fallback = fallback;
            this.source = new PersistenceSource<>() {
                @Override
                public XGettingCollection<? extends Binary> read() {
                    return ImportedBinarySource.this.takeImported(null);
                }

                @Override
                public XGettingCollection<? extends Binary> readByObjectIds(final PersistenceIdSet[] ids) {
                    return ImportedBinarySource.this.takeImported(ids);
                }
            };
        }

        private void begin(final ByteBuffer[] buffers) {
            this.buffers = buffers;
            this.supplied = false;
        }

        private void end() {
            this.buffers = null;
        }

        private XGettingCollection<? extends Binary> takeImported(final PersistenceIdSet[] ids) {
            if (!this.supplied) {
                this.supplied = true;
                return X.Enum(ChunksWrapper.New(this.buffers));
            }
            return ids == null ? this.fallback.source().read() : this.fallback.source().readByObjectIds(ids);
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
