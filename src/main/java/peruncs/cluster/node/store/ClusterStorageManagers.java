package peruncs.cluster.node.store;

import org.eclipse.store.storage.types.StorageManager;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.errors.GraphDrainTimeoutException;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.eclipse.serializer.util.X.notNull;

/// Internal construction of guarded cluster storage managers.
///
/// Construction policy is not application API: the node lifecycle is the only
/// caller and supplies the storage-limit validation, the node-close owner, and
/// the coordinator shared with replication. Public solely because the node
/// lifecycle lives in a sibling package.
public final class ClusterStorageManagers {
    private ClusterStorageManagers() {
    }

    /// Creates a write-gated manager for writer nodes.
    ///
    /// @param <T>                   root type
    /// @param delegate              started delegate Store manager
    /// @param storageLimitReached reports whether a writer limit is reached
    /// @param nodeClose             complete node teardown
    /// @param graphCoordinator      coordinator shared with replication
    /// @param replicationMark       mark to include in each write, or `null` for Store-only nodes
    /// @param prepareReplicationCommit updates the mark before serializer commit
    /// @return guarded manager
    public static <T> ClusterStorageManager<T> guarding(
            final StorageManager delegate,
            final BooleanSupplier storageLimitReached,
            final NodeClose nodeClose,
            final StorageGraphCoordinator graphCoordinator,
            final ReplicationMark replicationMark,
            final Consumer<ReplicationMark> prepareReplicationCommit) {
        return guarding(delegate, storageLimitReached, nodeClose, graphCoordinator,
                replicationMark, prepareReplicationCommit, ignored -> {
                });
    }

    /// Creates a writer facade whose Store commits own a cancelable replication sequence.
    public static <T> ClusterStorageManager<T> guarding(
            final StorageManager delegate,
            final BooleanSupplier storageLimitReached,
            final NodeClose nodeClose,
            final StorageGraphCoordinator graphCoordinator,
            final ReplicationMark replicationMark,
            final Consumer<ReplicationMark> prepareReplicationCommit,
            final Consumer<ReplicationMark> cancelReplicationCommit) {
        return new GuardingStorageManager<>(
                notNull(delegate), notNull(storageLimitReached),
                notNull(nodeClose), notNull(graphCoordinator), replicationMark,
                notNull(prepareReplicationCommit), notNull(cancelReplicationCommit));
    }

    /// Creates a Store-only guarded manager.
    ///
    /// @param <T> root type
    /// @param delegate started delegate Store manager
    /// @param storageLimitReached reports whether a writer limit is reached
    /// @param nodeClose complete node teardown
    /// @param graphCoordinator shared graph coordinator
    /// @return guarded manager
    public static <T> ClusterStorageManager<T> guarding(
            final StorageManager delegate,
            final BooleanSupplier storageLimitReached,
            final NodeClose nodeClose,
            final StorageGraphCoordinator graphCoordinator) {
        return guarding(delegate, storageLimitReached, nodeClose, graphCoordinator, null, ignored -> {
        });
    }

    /// Creates a read-only manager for reader and backup-reader nodes.
    ///
    /// @param <T>              root type
    /// @param delegate         started delegate Store manager
    /// @param nodeClose        complete node teardown
    /// @param graphCoordinator coordinator shared with replication
    /// @return read-only manager
    public static <T> ClusterStorageManager<T> readOnly(
            final StorageManager delegate,
            final NodeClose nodeClose,
            final StorageGraphCoordinator graphCoordinator) {
        return readOnly(delegate, nodeClose, graphCoordinator, null);
    }

    /// Creates a read-only facade that hides the transport's reserved root.
    public static <T> ClusterStorageManager<T> readOnly(
            final StorageManager delegate,
            final NodeClose nodeClose,
            final StorageGraphCoordinator graphCoordinator,
            final ReplicationMark replicationMark) {
        return new ReadOnlyStorageManager<>(
                notNull(delegate), notNull(nodeClose), notNull(graphCoordinator), replicationMark);
    }

    /// Waits for application calls to leave the facade before its Store closes.
    ///
    /// @param manager guarded node Store facade
    /// @param timeout maximum drain wait
    /// @throws IllegalArgumentException if the manager is not this module's facade
    /// @throws GraphDrainTimeoutException if application calls do not drain within the configured bound
    public static void awaitApplicationSections(final StorageManager manager, final Duration timeout) {
        if (!(notNull(manager) instanceof GuardingStorageManager<?> guarding)) {
            throw new IllegalArgumentException("node close requires a guarded cluster storage manager");
        }
        if (!guarding.awaitAppIdle(notNull(timeout))) {
            throw new GraphDrainTimeoutException("application Store sections did not drain before node close");
        }
    }
}
