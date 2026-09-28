package peruncs.cluster.node.backup;

import peruncs.cluster.api.BackupInfo;
import peruncs.cluster.api.BackupSlot;
import peruncs.cluster.api.BackupStatus;
import peruncs.cluster.node.NodeAssembly;
import peruncs.cluster.node.StorageNodeControl;

import java.util.concurrent.CompletableFuture;

/// Protocol-neutral control view of a backup node manager.
///
/// A backup role does not own the ordinary storage control operations, so
/// this view deliberately uses composition over inheritance: [#storage()]
/// borrows the storage control view explicitly instead of letting a backup
/// boundary inherit operations it may not drive. Like [StorageNodeControl],
/// this view exposes exactly the operations a boundary needs and no
/// `close()`: the assembly owns the manager and closes it on
/// [NodeAssembly#close].
///
/// @since 1.0
public interface BackupNodeControl {
    /// Borrows the storage control view of this backup node.
    ///
    /// The returned view exposes readiness, health, and observability only;
    /// the backup-specific operations stay on this control.
    ///
    /// @return the storage control view owned by the same manager
    StorageNodeControl storage();

    /// Creates a storage backup asynchronously.
    ///
    /// @param slot scheduled or manual retention slot
    /// @return future completed with the published backup details
    CompletableFuture<BackupInfo> createStorageBackup(BackupSlot slot);

    /// Returns the latest backup outcome.
    BackupStatus backupStatus();

}
