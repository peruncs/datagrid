package peruncs.datagrid.cluster.errors;

/// Reports a replication service that cannot currently make progress.
public class ReplicationUnavailableException extends ReplicationException {
    /// Provider error code, or `0`.
    private final int errorCode;

    /// Creates the exception with a message.
    ///
    /// @param message diagnostic message
    public ReplicationUnavailableException(final String message) {
        this(message, null, 0);
    }

    /// Creates the exception with a message and the underlying cause.
    ///
    /// @param message diagnostic message
    /// @param cause   underlying cause
    public ReplicationUnavailableException(final String message, final Throwable cause) {
        this(message, cause, 0);
    }

    /// Creates the exception with a message, cause, and provider error code.
    ///
    /// @param message   diagnostic message
    /// @param cause     underlying cause
    /// @param errorCode provider error code, or `0`
    public ReplicationUnavailableException(final String message, final Throwable cause, final int errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /// Returns the provider error code, or `0`.
    ///
    /// @return provider error code
    public int errorCode() {
        return this.errorCode;
    }
}
