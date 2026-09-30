package peruncs.cluster.api;

import org.eclipse.store.storage.types.StorageManager;
import peruncs.cluster.errors.BackupBusyException;

import java.util.concurrent.CompletableFuture;

/// One owned cluster-node lifecycle and its guarded Store.
///
/// Closing this object closes every Store, Aeron, backup, and worker resource
/// it created. The implementation stays behind [ClusterStorage].
///
/// @param <T> root type
/// @since 1.0
public interface ClusterNode<T> extends AutoCloseable {
    /// Returns the guarded, Store-compatible storage manager owned by this node.
    ///
    /// The manager is a drop-in [StorageManager]:
    /// common Store interfaces remain available under the documented role,
    /// root, coordination, and lifecycle restrictions. Reads and mutations on
    /// the graph join [GraphBoundary]; its `shutdown()` performs the complete
    /// node teardown, as does closing this node.
    ///
    /// @return the guarded Store facade owned by this node's lifecycle
    ClusterStorageManager<T> storageManager();

    /// Starts periodic storage checks for this node's Store.
    void startStorageChecks();

    /// Creates a backup asynchronously at a resolved replication boundary.
    ///
    /// A concurrent request completes exceptionally with [BackupBusyException].
    /// Cancelling the future does not cancel the backup task.
    ///
    /// @param slot scheduled or retained manual slot
    /// @return completion of the backup
    /// @throws WrongRoleException unless this is a backup-reader
    CompletableFuture<BackupInfo> createBackup(BackupSlot slot);

    /// Returns one immutable status snapshot.
    ///
    /// @return the current role, readiness, and replication metrics
    NodeStatus status();

    /// Closes the node and every resource it created.
    ///
    /// Equivalent to `storageManager().shutdown()`: closing either entry point
    /// triggers the same ordered teardown. A close invoked inside a
    /// [GraphBoundary] section is rejected; unwind the section first.
    @Override
    void close();
}
