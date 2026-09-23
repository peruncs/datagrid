package peruncs.datagrid.cluster.errors;

/// Reports a write attempted through a read-only cluster node.
public class ReaderWriteRejectedException extends ReplicationException {
    /// Creates the exception with a message.
    ///
    /// @param message diagnostic message
    public ReaderWriteRejectedException(final String message) { super(message); }
    /// Creates the exception with a message and the underlying cause.
    ///
    /// @param message diagnostic message
    /// @param cause   underlying cause
    public ReaderWriteRejectedException(final String message, final Throwable cause) { super(message, cause); }
}
