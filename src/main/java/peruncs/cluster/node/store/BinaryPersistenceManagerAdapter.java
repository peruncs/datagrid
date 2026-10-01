package peruncs.cluster.node.store;

import org.eclipse.serializer.collections.Set_long;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.*;
import org.eclipse.serializer.persistence.types.PersistenceStorer.Creator;
import org.eclipse.serializer.reference.Swizzling;
import org.eclipse.store.storage.types.*;
import peruncs.cluster.errors.*;
import java.nio.ByteOrder;
import java.util.function.*;

/// Adapts the cluster manager to Store's binary persistence manager.
final class BinaryPersistenceManagerAdapter implements PersistenceManager<Binary> {
    private final GuardingStorageManager<?> owner;
    private final PersistenceManager<Binary> delegate;

    BinaryPersistenceManagerAdapter(final GuardingStorageManager<?> owner, final PersistenceManager<Binary> delegate) {
        this.owner = owner;
        this.delegate = delegate;
    }

    @Override
    public long ensureObjectId(final Object object) {
        return this.owner.isReadOnly() ? this.lookupRegisteredId(object)
                : this.delegate.ensureObjectId(object);
    }

    @Override
    public <U> long ensureObjectId(
            final U object,
            final PersistenceObjectIdRequestor<Binary> objectIdRequestor,
            final PersistenceTypeHandler<Binary, U> optionalHandler
    ) {
        return this.owner.isReadOnly() ? this.lookupRegisteredId(object)
                : this.delegate.ensureObjectId(object, objectIdRequestor, optionalHandler);
    }

    @Override
    public <U> long ensureObjectIdGuaranteedRegister(
            final U object,
            final PersistenceObjectIdRequestor<Binary> objectIdRequestor,
            final PersistenceTypeHandler<Binary, U> optionalHandler
    ) {
        return this.owner.isReadOnly() ? this.lookupRegisteredId(object)
                : this.delegate.ensureObjectIdGuaranteedRegister(object, objectIdRequestor, optionalHandler);
    }

    @Override
    public void consolidate() {
        this.rejectReaderMutation();
        this.delegate.consolidate();
    }

    @Override
    public boolean registerLocalRegistry(final PersistenceLocalObjectIdRegistry<Binary> localRegistry) {
        this.rejectReaderMutation();
        return this.delegate.registerLocalRegistry(localRegistry);
    }

    @Override
    public void mergeEntries(final PersistenceLocalObjectIdRegistry<Binary> localRegistry) {
        this.rejectReaderMutation();
        this.delegate.mergeEntries(localRegistry);
    }

    @Override
    public long lookupObjectId(final Object object) {
        return this.owner.read(() -> this.delegate.lookupObjectId(object));
    }

    @Override
    public Object lookupObject(final long objectId) {
        return this.owner.read(() -> this.delegate.lookupObject(objectId));
    }

    @Override
    public Object get() {
        return this.owner.read(this.delegate::get);
    }

    @Override
    public Object getObject(final long objectId) {
        return this.owner.read(() -> this.delegate.getObject(objectId));
    }

    @Override
    public <C extends Consumer<Object>> C collect(final C collector, final long... objectIds) {
        return this.owner.read(() -> this.delegate.collect(collector, objectIds));
    }

    @Override
    public <C extends Consumer<Object>> C collect(final C collector, final Set_long objectIds) {
        return this.owner.read(() -> this.delegate.collect(collector, objectIds));
    }

    @Override
    public long store(final Object instance) {
        /* Exclusive section like the facade's own store(): adapter writes
         * must not race application write sections, and a delegate failure
         * invalidates through the same latch. */
        return this.owner.persist(() -> {
            final ClusterPersistenceStorerAdapter storer =
                    new ClusterPersistenceStorerAdapter(this.owner, this.delegate.createStorer());
            final long objectId = storer.store(instance);
            storer.commitWithinWriteSection();
            return objectId;
        });
    }

    @Override
    public long[] storeAll(final Object... instances) {
        return this.owner.persist(() -> {
            final ClusterPersistenceStorerAdapter storer =
                    new ClusterPersistenceStorerAdapter(this.owner, this.delegate.createStorer());
            final long[] objectIds = storer.storeAll(instances);
            storer.commitWithinWriteSection();
            return objectIds;
        });
    }

    @Override
    public void storeAll(final Iterable<?> instances) {
        this.owner.persist(() -> {
            final ClusterPersistenceStorerAdapter storer =
                    new ClusterPersistenceStorerAdapter(this.owner, this.delegate.createStorer());
            storer.storeAll(instances);
            storer.commitWithinWriteSection();
        });
    }

    @Override
    public ByteOrder getTargetByteOrder() {
        return this.delegate.getTargetByteOrder();
    }

    @Override
    public PersistenceStorer createLazyStorer() {
        return new ClusterPersistenceStorerAdapter(this.owner, this.delegate.createLazyStorer());
    }

    @Override
    public PersistenceStorer createStorer() {
        return new ClusterPersistenceStorerAdapter(this.owner, this.delegate.createStorer());
    }

    @Override
    public PersistenceStorer createEagerStorer() {
        return new ClusterPersistenceStorerAdapter(this.owner, this.delegate.createEagerStorer());
    }

    @Override
    public PersistenceStorer createStorer(final Creator<Binary> storerCreator) {
        return new ClusterPersistenceStorerAdapter(this.owner, this.delegate.createStorer(storerCreator));
    }

    @Override
    public PersistenceLoader createLoader() {
        this.owner.ensureGraphValid();
        return this.delegate.createLoader();
    }

    @Override
    public PersistenceRegisterer createRegisterer() {
        this.rejectReaderMutation();
        return this.delegate.createRegisterer();
    }

    @Override
    public void updateMetadata(
            final PersistenceTypeDictionary typeDictionary,
            final long highestTypeId,
            final long highestObjectId
    ) {
        if (this.owner.replicationMark != null) {
            throw new UnsupportedOperationException("replicated Store metadata is node-owned");
        }
        this.owner.persist(() ->
                this.delegate.updateMetadata(typeDictionary, highestTypeId, highestObjectId));
    }

    @Override
    public PersistenceObjectRegistry objectRegistry() {
        this.rejectReaderMutation();
        return this.delegate.objectRegistry();
    }

    @Override
    public Object objectRegistryMonitor() {
        return this.delegate.objectRegistryMonitor();
    }

    @Override
    public PersistenceTypeDictionary typeDictionary() {
        return this.owner.read(this.delegate::typeDictionary);
    }

    private long lookupRegisteredId(final Object object) {
        return this.owner.read(() -> {
            final long objectId = this.delegate.lookupObjectId(object);
            if (Swizzling.isNotFoundId(objectId)) {
                throw new ReaderWriteRejectedException(
                        "node role is read-only; assigning a new object id would diverge from the writer");
            }
            return objectId;
        });
    }

    private void rejectReaderMutation() {
        if (this.owner.isReadOnly()) {
            throw new ReaderWriteRejectedException(
                    "node role is read-only; application registry changes are rejected");
        }
    }

    @Override
    public PersistenceRootsView viewRoots() {
        return this.owner.viewRoots();
    }

    @Override
    public long currentObjectId() {
        return this.delegate.currentObjectId();
    }

    @Override
    public PersistenceManager<Binary> updateCurrentObjectId(final long currentObjectId) {
        if (this.owner.replicationMark != null) {
            throw new UnsupportedOperationException("replicated Store object ids are node-owned");
        }
        this.owner.persist(() ->
                this.delegate.updateCurrentObjectId(currentObjectId));
        return this;
    }

    @Override
    public PersistenceSource<Binary> source() {
        return this.delegate.source();
    }

    @Override
    public PersistenceTarget<Binary> target() {
        if (this.owner.replicationMark != null) {
            throw new UnsupportedOperationException("replicated Store commits must use the storage manager");
        }
        return this.owner.gateTarget(this.delegate.target());
    }

    @Override
    public void close() {
        /* This adapter is a borrowed view of the Store's shared
         * persistence manager. The owning storage manager alone ends
         * that lifecycle. */
    }
}
