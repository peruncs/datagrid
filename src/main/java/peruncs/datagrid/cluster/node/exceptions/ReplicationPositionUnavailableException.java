package peruncs.datagrid.cluster.node.exceptions;

/// Reports that the current node role cannot provide a writer position.
public final class ReplicationPositionUnavailableException extends NodeLibraryException {
        /// Creates an exception with a diagnostic message.
    ///
    /// @param message diagnostic message
    public ReplicationPositionUnavailableException(final String message) {
        super(message);
    }

    /// Creates a position failure retaining the underlying cause.
    public ReplicationPositionUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
