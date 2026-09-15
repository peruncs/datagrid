package peruncs.datagrid.cluster.node.exceptions;

/// Reports that the current node role cannot provide a writer position.
public final class ReplicationPositionUnavailableException extends NodeLibraryException {
        /// Creates an exception with a diagnostic message.
    ///
    /// @param message diagnostic message
    public ReplicationPositionUnavailableException(final String message) {
        super(message);
    }
}
