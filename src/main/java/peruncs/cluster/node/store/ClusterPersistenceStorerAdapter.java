package peruncs.cluster.node.store;

import org.eclipse.serializer.persistence.types.*;
import org.eclipse.store.storage.types.*;
import peruncs.cluster.errors.*;
import java.util.function.*;

/// Stores binary entities while applying the cluster's size rules.
final class ClusterPersistenceStorerAdapter extends ClusterStorerAdapter implements PersistenceStorer {
    private final PersistenceStorer delegate;

    ClusterPersistenceStorerAdapter(final GuardingStorageManager<?> owner, final PersistenceStorer delegate) {
        super(owner, delegate);
        this.delegate = delegate;
    }

    @Override
    public PersistenceStorer reinitialize() {
        this.delegate.reinitialize();
        return this;
    }

    @Override
    public PersistenceStorer reinitialize(final long initialCapacity) {
        this.delegate.reinitialize(initialCapacity);
        return this;
    }

    @Override
    public PersistenceStorer ensureCapacity(final long desiredCapacity) {
        this.delegate.ensureCapacity(desiredCapacity);
        return this;
    }
}
