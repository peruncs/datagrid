package peruncs.cluster.errors;

/// Thrown when this node cannot resume and must be restored from a compatible seed.
///
/// The stable message prefix makes the required operator action visible in
/// logs even when the exception class is hidden by a process or RPC boundary.
public class ReseedRequiredException extends ReplicationException {
    private static final String PREFIX = "RESEED_REQUIRED: ";

    /// Creates the exception with a message.
    ///
    /// @param message diagnostic message

    public ReseedRequiredException(final String message) { super(prefix(message)); }
    /// Creates the exception with a message and the underlying cause.
    ///
    /// @param message diagnostic message
    /// @param cause   underlying cause
    public ReseedRequiredException(final String message, final Throwable cause) { super(prefix(message), cause); }

    private static String prefix(final String message) {
        return message != null && message.startsWith(PREFIX) ? message : PREFIX + message;
    }
}
