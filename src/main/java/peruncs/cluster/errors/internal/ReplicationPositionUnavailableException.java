package peruncs.cluster.errors.internal;

import peruncs.cluster.errors.NodeException;

/// Reports that this node cannot establish a durable replication boundary.
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
