package peruncs.cluster.errors;

import peruncs.cluster.errors.internal.ReplicationPositionUnavailableException;
import peruncs.cluster.node.backup.IncompleteArchiveException;

/// Signals a node lifecycle or storage failure that applications may catch at the boundary.
///
/// The type is deliberately dependency-free: it extends [RuntimeException]
/// directly so every consumer of this package can catch node failures
/// without inheriting a third-party exception hierarchy.
public sealed class NodeException extends RuntimeException permits BackupBusyException,
        GraphDrainTimeoutException, ReplicationException, ReplicationPositionUnavailableException,
        StorageLimitReachedException, WrongRoleException,
        IncompleteArchiveException {
    /// Broad recovery hint for application boundaries.
    public enum Outcome {
        /// The requested Store write did not begin local persistence and is safe to retry.
        RETRYABLE,
        /// The Store commit is durable locally; wait for replication completion or restart recovery.
        PENDING,
        /// This Store image cannot resume without reseeding.
        RESEED_REQUIRED,
        /// The operation failed and has no generic retry guarantee.
        FAILED
    }

    /// Creates an exception that preserves only the underlying cause.
    ///
    /// @param cause underlying cause
    public NodeException(final Throwable cause) {
        super(cause);
    }

    /// Creates an exception with a message.
    ///
    /// @param message error message
    public NodeException(final String message) {
        super(message);
    }

    /// Creates an exception with a message and cause.
    ///
    /// @param message error message
    /// @param cause   underlying cause
    public NodeException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /// Returns the recovery hint derived from this exception's concrete type.
    ///
    /// Inspect the exception subtype for details; this value does not replace
    /// the type-specific recovery contract.
    public final Outcome outcome() {
        if (this instanceof WriteRejectedException) return Outcome.RETRYABLE;
        if (this instanceof ReplicationPendingException) return Outcome.PENDING;
        if (this instanceof ReseedRequiredException) return Outcome.RESEED_REQUIRED;
        return Outcome.FAILED;
    }
}
