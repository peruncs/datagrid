package peruncs.datagrid.cluster.errors;

/// Signals a replication operation failure; catch the typed subclasses to choose the recovery action.
///
/// Extends [RuntimeException], not [IllegalStateException]: callers that
/// catch `IllegalStateException` to find genuine programming errors must not
/// swallow typed replication failures, and vice versa.
public class ReplicationException extends RuntimeException {
    /// Creates an exception with a message.
    ///
    /// @param message diagnostic message
    public ReplicationException(final String message) {
        super(message);
    }

    /// Creates an exception with a message and the underlying cause.
    ///
    /// @param message diagnostic message
    /// @param cause   underlying cause
    public ReplicationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
