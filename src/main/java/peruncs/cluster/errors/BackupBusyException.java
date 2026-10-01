package peruncs.cluster.errors;

/// A backup was requested while another backup is still running.
///
/// Exported so an embedding boundary can report a conflict (`409`) instead
/// of mapping a busy backup to a server error. `ClusterNode.createBackup`
/// completes its future exceptionally with this type.
public final class BackupBusyException extends NodeException {
    /// Creates a busy-backup failure.
    ///
    /// @param message failure message
    public BackupBusyException(final String message) {
        super(message);
    }

    /// Creates a busy-backup failure with a cause.
    ///
    /// @param message failure message
    /// @param cause   underlying failure
    public BackupBusyException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
