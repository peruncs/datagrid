package peruncs.datagrid.cluster.errors;

/// Thrown when this node cannot resume and must be restored from a compatible seed.
///
/// The stable message prefix makes the required operator action visible in
/// logs even when the exception class is hidden by a process or RPC boundary.
public class ReseedRequiredException extends ReplicationException {
    private static final String PREFIX = "RESEED_REQUIRED: ";

    public ReseedRequiredException(final String message) { super(prefix(message)); }
    public ReseedRequiredException(final String message, final Throwable cause) { super(prefix(message), cause); }

    private static String prefix(final String message) {
        return message != null && message.startsWith(PREFIX) ? message : PREFIX + message;
    }
}
