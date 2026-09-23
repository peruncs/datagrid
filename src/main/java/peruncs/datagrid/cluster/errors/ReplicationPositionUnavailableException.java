package peruncs.datagrid.cluster.errors;

/// Reports that the current node role cannot provide a writer position.
public final class ReplicationPositionUnavailableException extends NodeException {
        /// Creates an exception with a diagnostic message.
    ///
    /// @param message diagnostic message
    public ReplicationPositionUnavailableException(final String message) {
        super(message);
    }

    /// Creates an exception retaining the underlying cause.
    ///
    /// @param message diagnostic message
    /// @param cause   underlying cause
    public ReplicationPositionUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
