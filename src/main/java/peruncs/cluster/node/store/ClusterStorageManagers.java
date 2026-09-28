package peruncs.cluster.node.store;

import org.eclipse.store.storage.types.StorageManager;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.errors.GraphDrainTimeoutException;
import peruncs.cluster.storage.StorageGraphCoordinator;

import java.time.Duration;

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
    /// @param storageSizeValidation storage limit gate
    /// @param nodeClose             complete node teardown
    /// @param graphCoordinator      coordinator shared with replication
    /// @return guarded manager
    public static <T> ClusterStorageManager<T> guarding(
            final StorageManager delegate,
            final StorageSizeValidation storageSizeValidation,
            final NodeClose nodeClose,
            final StorageGraphCoordinator graphCoordinator) {
        return new GuardingStorageManager<>(
                notNull(delegate), notNull(storageSizeValidation),
                notNull(nodeClose), notNull(graphCoordinator));
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
        return new ReadOnlyStorageManager<>(
                notNull(delegate), notNull(nodeClose), notNull(graphCoordinator));
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
