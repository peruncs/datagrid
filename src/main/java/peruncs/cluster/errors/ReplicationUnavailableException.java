package peruncs.cluster.errors;

/// Reports a replication service that cannot currently make progress.
public final class ReplicationUnavailableException extends ReplicationException {
    /// Creates the exception with a message.
    ///
    /// @param message diagnostic message
    public ReplicationUnavailableException(final String message) {
        super(message);
    }

    /// Creates the exception with a message and the underlying cause.
    ///
    /// @param message diagnostic message
    /// @param cause   underlying cause
    public ReplicationUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
