package peruncs.cluster.node.store;

import org.eclipse.store.storage.types.StorageManager;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;

import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/// Builds facades for tests, filling in the replication hooks a Store-only test does not need.
public final class TestManagers {
    private TestManagers() {
    }

    /// Creates a write-gated facade without a replication mark.
    public static <T> ClusterStorageManager<T> guarding(final StorageManager delegate,
                                                        final BooleanSupplier storageLimitReached,
                                                        final NodeClose nodeClose,
                                                        final StorageGraphCoordinator coordinator) {
        return guarding(delegate, storageLimitReached, nodeClose, coordinator, null, ignored -> {
        });
    }

    /// Creates a write-gated facade whose commit hooks cannot be cancelled.
    public static <T> ClusterStorageManager<T> guarding(final StorageManager delegate,
                                                        final BooleanSupplier storageLimitReached,
                                                        final NodeClose nodeClose,
                                                        final StorageGraphCoordinator coordinator,
                                                        final ReplicationMark mark,
                                                        final Consumer<ReplicationMark> prepare) {
        return ClusterStorageManagers.guarding(delegate, storageLimitReached, nodeClose, coordinator, mark,
                prepare, ignored -> {
                });
    }

    /// Creates a read-only facade without a replication mark.
    public static <T> ClusterStorageManager<T> readOnly(final StorageManager delegate, final NodeClose nodeClose,
                                                        final StorageGraphCoordinator coordinator) {
        return ClusterStorageManagers.readOnly(delegate, nodeClose, coordinator, null);
    }

    /// Creates a read-only facade that hides the given replication mark.
    public static <T> ClusterStorageManager<T> readOnly(final StorageManager delegate, final NodeClose nodeClose,
                                                        final StorageGraphCoordinator coordinator,
                                                        final ReplicationMark mark) {
        return ClusterStorageManagers.readOnly(delegate, nodeClose, coordinator, mark);
    }
}
