package peruncs.datagrid.cluster.nodelibrary.exceptions;

/// Reports that a node has reached its configured storage limit.
public class StorageLimitReachedException extends NodelibraryException {
        /// Creates an exception without a message.
    public StorageLimitReachedException() {
        super();
    }

        /// Creates an exception with a message.
    ///
    /// @param message error message
    public StorageLimitReachedException(final String message) {
        super(message);
    }

        /// Creates an exception with a cause.
    ///
    /// @param cause underlying cause
    public StorageLimitReachedException(final Throwable cause) {
        super(cause);
    }

        /// Creates an exception with a message and cause.
    ///
    /// @param message error message
    /// @param cause   underlying cause
    public StorageLimitReachedException(final String message, final Throwable cause) {
        super(message, cause);
    }

        /// Creates an exception with full throwable settings.
    ///
    /// @param message            error message
    /// @param cause              underlying cause
    /// @param enableSuppression  whether suppression is enabled
    /// @param writableStackTrace whether the stack trace may be written
    public StorageLimitReachedException(
            final String message,
            final Throwable cause,
            final boolean enableSuppression,
            final boolean writableStackTrace
    ) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
