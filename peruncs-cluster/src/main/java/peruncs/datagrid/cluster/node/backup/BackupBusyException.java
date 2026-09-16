package peruncs.datagrid.cluster.node.backup;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;

/// A backup was requested while another backup is still running.
///
/// Carried as a typed domain failure so the HTTP layer can report 409
/// Conflict instead of mapping a busy backup to a server error.
final class BackupBusyException extends NodeLibraryException {
        /// Creates a busy-backup failure.
    ///
    /// @param message failure message
    public BackupBusyException(final String message) {
        super(message);
    }
}
