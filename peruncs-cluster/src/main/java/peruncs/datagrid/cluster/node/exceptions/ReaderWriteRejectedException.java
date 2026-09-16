package peruncs.datagrid.cluster.node.exceptions;

/// Rejects an application write on a node whose role is read-only.
///
/// Readers and backup-readers reproduce the writer's history through the
/// replication import path. A local mutation would persist without
/// publication and permanently diverge the node, so every application
/// write entry point fails with this exception instead.
public class ReaderWriteRejectedException extends NodeLibraryException {
        /// Creates an exception with a message.
    ///
    /// @param message error message
    public ReaderWriteRejectedException(final String message) {
        super(message);
    }

        /// Creates an exception with a message and cause.
    ///
    /// @param message error message
    /// @param cause   underlying cause
    public ReaderWriteRejectedException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
