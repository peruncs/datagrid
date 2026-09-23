package peruncs.datagrid.cluster.errors;

/// Signals a replication operation failure; catch the typed subclasses to choose the recovery action.
public class ReplicationException extends IllegalStateException {
    /// Creates the exception with a message.
    ///
    /// @param message diagnostic message
    public ReplicationException(final String message) {
        super(message);
    }

    /// Creates the exception with a message and the underlying cause.
    ///
    /// @param message diagnostic message
    /// @param cause   underlying cause
    public ReplicationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
