package peruncs.cluster.node.store;

import org.eclipse.serializer.persistence.types.*;
import org.eclipse.store.storage.types.*;
import peruncs.cluster.errors.*;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;
import java.util.function.*;

/// Delegates Store storer operations while preserving cluster checks.
class ClusterStorerAdapter implements Storer {
    final GuardingStorageManager<?> owner;
    private final Storer storer;

    ClusterStorerAdapter(final GuardingStorageManager<?> owner, final Storer storer) {
        this.owner = owner;
        this.storer = storer;
    }

    @Override
    public long store(final Object instance) {
        /* A cached storer must not keep accepting registrations after the
         * graph is latched or the node closed — buffering into a visible
         * write would otherwise still corrupt a later commit. */
        this.owner.validateState();
        return this.storer.store(instance);
    }

    @Override
    public long store(final Object instance, final long objectId) {
        this.owner.validateState();
        return this.storer.store(instance, objectId);
    }

    @Override
    public long[] storeAll(final Object... instances) {
        this.owner.validateState();
        return this.storer.storeAll(instances);
    }

    @Override
    public void storeAll(final Iterable<?> instances) {
        this.owner.validateState();
        this.storer.storeAll(instances);
    }

    @Override
    public Object commit() {
        /* Same exclusive persist section as store()/storeRoot():
         * admission before and after the lock, latch only on a delegate
         * persistence failure. */
        return this.owner.persist(this::commitWithinWriteSection);
    }

    Object commitWithinWriteSection() {
        if (this.owner.replicationMark != null) {
            final ReplicationMark mark = this.owner.replicationMark;
            try {
                this.owner.prepareReplicationCommit.accept(mark);
                this.storer.store(mark);
                return this.storer.commit();
            } finally {
                this.owner.cancelReplicationCommit.accept(mark);
            }
        }
        return this.storer.commit();
    }

    @Override
    public void clear() {
        this.storer.clear();
    }

    @Override
    public boolean skipMapped(final Object instance, final long objectId) {
        return this.storer.skipMapped(instance, objectId);
    }

    @Override
    public boolean skip(final Object instance) {
        return this.storer.skip(instance);
    }

    @Override
    public boolean skipNulled(final Object instance) {
        return this.storer.skipNulled(instance);
    }

    @Override
    public long size() {
        return this.storer.size();
    }

    @Override
    public long currentCapacity() {
        return this.storer.currentCapacity();
    }

    @Override
    public long maximumCapacity() {
        return this.storer.maximumCapacity();
    }

    @Override
    public Storer reinitialize() {
        this.storer.reinitialize();
        return this;
    }

    @Override
    public Storer reinitialize(final long initialCapacity) {
        this.storer.reinitialize(initialCapacity);
        return this;
    }

    @Override
    public Storer ensureCapacity(final long desiredCapacity) {
        this.storer.ensureCapacity(desiredCapacity);
        return this;
    }

    @Override
    public void registerCommitListener(final PersistenceCommitListener listener) {
        this.storer.registerCommitListener(listener);
    }

    @Override
    public boolean isEmpty() {
        return this.storer.isEmpty();
    }

    @Override
    public void registerRegistrationListener(final PersistenceObjectRegistrationListener listener) {
        this.storer.registerRegistrationListener(listener);
    }
}
