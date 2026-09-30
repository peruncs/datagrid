package peruncs.cluster.errors;

/// Signals a replication operation failure; catch the typed subclasses to choose the recovery action.
///
/// Extends [NodeException] so every typed replication failure reaches the
/// same application-facing failure boundary as startup and storage errors.
public sealed class ReplicationException extends NodeException permits CorruptReplicationDataException,
        GraphInvalidatedException, ReaderWriteRejectedException, ReplicationUnavailableException,
        ReplicationPendingException, ReseedRequiredException, WriteRejectedException {
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
