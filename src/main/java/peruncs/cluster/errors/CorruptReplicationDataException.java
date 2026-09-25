package peruncs.cluster.errors;

/// Reports invalid replication bytes or metadata.
public class CorruptReplicationDataException extends ReplicationException {
    /// Creates the exception with a message.
    ///
    /// @param message diagnostic message
    public CorruptReplicationDataException(final String message) { super(message); }
    /// Creates the exception with a message and the underlying cause.
    ///
    /// @param message diagnostic message
    /// @param cause   underlying cause
    public CorruptReplicationDataException(final String message, final Throwable cause) { super(message, cause); }
}
