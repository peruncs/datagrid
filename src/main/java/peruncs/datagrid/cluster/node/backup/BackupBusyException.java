package peruncs.datagrid.cluster.node.backup;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;

/// A backup was requested while another backup is still running.
///
/// Carried as a typed domain failure so the embedding application's boundary
/// can report `409 Conflict` instead of mapping a busy backup to a server
/// error. The type is part of the exported contract: `createStorageBackup`
/// throws it and the boundary catches it by name.
///
/// @since 1.0
public final class BackupBusyException extends NodeLibraryException {
        /// Creates a busy-backup failure.
    ///
    /// @param message failure message
    public BackupBusyException(final String message) {
        super(message);
    }

        /// Creates a busy-backup failure retaining the underlying cause.
    ///
    /// @param message failure message
    /// @param cause   underlying cause
    public BackupBusyException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
