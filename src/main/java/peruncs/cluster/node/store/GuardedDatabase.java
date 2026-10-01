package peruncs.cluster.node.store;

import org.eclipse.serializer.persistence.types.*;
import org.eclipse.store.storage.types.*;
import peruncs.cluster.errors.*;
import java.util.function.*;

/// A [Database] view over the guarded manager: every operation routes
/// through this facade, so import rejection, the boundary, and the node's
/// lifecycle/validity admission all still apply.
final class GuardedDatabase implements Database {
    private final GuardingStorageManager<?> owner;

    GuardedDatabase(final GuardingStorageManager<?> owner) {
        this.owner = owner;
    }

    @Override
    public String databaseName() {
        return this.owner.delegateDatabase.databaseName();
    }

    @Override
    public String toIdentifyingString() {
        return this.owner.delegateDatabase.toIdentifyingString();
    }

    @Override
    public StorageManager storage() {
        /* The facade itself: never the raw embedded manager. */
        return this.owner;
    }

    @Override
    public StorageManager setStorage(final StorageManager storage) {
        throw new UnsupportedOperationException(
                "a cluster node's Store is owned by its node lifecycle; setStorage is reserved");
    }

    @Override
    public boolean hasStorage() {
        return this.owner.isRunning();
    }

    @Override
    public Database guaranteeNoActiveStorage() {
        throw new UnsupportedOperationException(
                "a cluster node's Store lifecycle is owned by the node, not the application Database");
    }

    @Override
    public StorageManager guaranteeActiveStorage() {
        this.owner.ensureOpen();
        this.owner.ensureGraphValid();
        return this.owner;
    }

    @Override
    public Object getObject(final long objectId) {
        /* Object retrieval reads the managed graph: it joins the same
         * coordinated read section as every other read, so a failed or
         * draining Store never serves identifiers. */
        return this.owner.read(
                () -> this.owner.delegateDatabase.getObject(objectId));
    }

    @Override
    public long store(final Object instance) {
        return this.owner.store(instance);
    }

    @Override
    public long[] storeAll(final Object... instances) {
        return this.owner.storeAll(instances);
    }

    @Override
    public void storeAll(final Iterable<?> instances) {
        this.owner.storeAll(instances);
    }

    @Override
    public Storer createLazyStorer() {
        return this.owner.createLazyStorer();
    }

    @Override
    public Storer createStorer() {
        return this.owner.createStorer();
    }

    @Override
    public Storer createEagerStorer() {
        return this.owner.createEagerStorer();
    }
}
