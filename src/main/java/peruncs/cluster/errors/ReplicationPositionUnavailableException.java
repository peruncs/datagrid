package peruncs.cluster.errors;

/// Reports that this node cannot establish a durable replication boundary.
///
/// Thrown by the node internals; applications normally meet it as a cause.
public final class ReplicationPositionUnavailableException extends NodeException {
    /// Creates an exception with a message.
    public ReplicationPositionUnavailableException(final String message) {
        super(message);
    }

    /// Creates an exception with a message and cause.
    public ReplicationPositionUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
