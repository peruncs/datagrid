package peruncs.cluster.node.backup;

import peruncs.cluster.errors.NodeException;
import peruncs.cluster.node.NodeAssembly;
import peruncs.cluster.node.StorageNodeControl;

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

        /// Stops the reader at the latest safe message boundary.
    void stopReadingAtLatestMessage();

        /// Resumes the reader after backup work.
    ///
    /// @throws NodeException if the reader cannot resume
    void resumeReading() throws NodeException;

        /// Reports whether the reader is active.
    ///
    /// @return `true` when the reader is active
    boolean isReading();

        /// Creates a storage backup.
    ///
    /// @param useManualSlot whether to use the manual backup slot
    /// @throws NodeException if backup creation fails
    void createStorageBackup(final boolean useManualSlot) throws NodeException;

        /// Reports whether a backup is running.
    ///
    /// @return `true` when backup work is active
    boolean isBackupRunning();
}
