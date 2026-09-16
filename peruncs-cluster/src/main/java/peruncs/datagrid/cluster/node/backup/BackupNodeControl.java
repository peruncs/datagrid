package peruncs.datagrid.cluster.node.backup;

import peruncs.datagrid.cluster.node.ClusterFoundation;
import peruncs.datagrid.cluster.node.StorageNodeControl;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;

/// Protocol-neutral control view of a backup node manager.
///
/// Like [StorageNodeControl], this exposes exactly the operations a boundary
/// needs and no `close()`: the foundation owns the manager and closes it on
/// [ClusterFoundation#close].
///
/// @since 1.0
public interface BackupNodeControl extends StorageNodeControl {
        /// Stops the reader at the latest safe message boundary.
    void stopReadingAtLatestMessage();

        /// Resumes the reader after backup work.
    ///
    /// @throws NodeLibraryException if the reader cannot resume
    void resumeReading() throws NodeLibraryException;

        /// Reports whether the reader is active.
    ///
    /// @return `true` when the reader is active
    boolean isReading();

        /// Creates a storage backup.
    ///
    /// @param useManualSlot whether to use the manual backup slot
    /// @throws NodeLibraryException if backup creation fails
    void createStorageBackup(final boolean useManualSlot) throws NodeLibraryException;

        /// Reports whether a backup is running.
    ///
    /// @return `true` when backup work is active
    boolean isBackupRunning();
}
