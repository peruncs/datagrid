package peruncs.datagrid.cluster.errors;

/// Reports a write refused because this process no longer owns the writer lease.
public class WriterFencedException extends ReplicationException {
    /// Creates the exception with a message.
    ///
    /// @param message diagnostic message
    public WriterFencedException(final String message) { super(message); }
    /// Creates the exception with a message and the underlying cause.
    ///
    /// @param message diagnostic message
    /// @param cause   underlying cause
    public WriterFencedException(final String message, final Throwable cause) { super(message, cause); }
}
