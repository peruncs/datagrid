package peruncs.cluster.errors;

/// Signals that an archive is conclusively incomplete or corrupt.
///
/// Only truncation or corrupt ZIP structure may be reported through this type.
/// Callers may replace such a file; transient read errors must leave durable
/// archives untouched.
public final class IncompleteArchiveException extends NodeException {
    /// Creates an incomplete-archive failure.
    ///
    /// @param message failure message
    public IncompleteArchiveException(final String message) {
        super(message);
    }

    /// Creates an incomplete-archive failure with a cause.
    ///
    /// @param message failure message
    /// @param cause   evidence of truncation or corruption
    public IncompleteArchiveException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
