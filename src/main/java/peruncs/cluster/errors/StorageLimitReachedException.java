package peruncs.cluster.errors;

/// Reports that a node has reached its configured storage limit.
public final class StorageLimitReachedException extends NodeException {
        /// Creates an exception with a message.
    ///
    /// @param message error message
    public StorageLimitReachedException(final String message) {
        super(message);
    }

        /// Creates an exception with a message and cause.
    ///
    /// @param message error message
    /// @param cause   underlying cause
    public StorageLimitReachedException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
