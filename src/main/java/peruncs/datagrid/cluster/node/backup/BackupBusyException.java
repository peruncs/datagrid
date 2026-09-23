package peruncs.datagrid.cluster.node.backup;

import peruncs.datagrid.cluster.errors.NodeException;

/// A backup was requested while another backup is still running.
///
/// Carried as a typed domain failure so the embedding application's boundary
/// can report `409 Conflict` instead of mapping a busy backup to a server
/// error. The type is part of the exported contract: `createStorageBackup`
/// throws it and the boundary catches it by name.
///
/// @since 1.0
final class BackupBusyException extends NodeException {
        /// Creates a busy-backup failure.
    ///
    /// @param message failure message
    BackupBusyException(final String message) {
        super(message);
    }

        /// Creates a busy-backup failure retaining the underlying cause.
    ///
    /// @param message failure message
    /// @param cause   underlying cause
    BackupBusyException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
