package peruncs.cluster.errors;

/// Signals a node lifecycle or storage failure that applications may catch at the boundary.
///
/// The type is deliberately dependency-free: it extends [RuntimeException]
/// directly so every consumer of this package can catch node failures
/// without inheriting a third-party exception hierarchy.
public class NodeException extends RuntimeException {
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
}
