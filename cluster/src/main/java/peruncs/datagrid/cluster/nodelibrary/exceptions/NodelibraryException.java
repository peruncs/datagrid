package peruncs.datagrid.cluster.nodelibrary.exceptions;


import org.eclipse.serializer.exceptions.BaseException;

/// Base runtime exception for node lifecycle and storage failures.
public class NodelibraryException extends BaseException {
        /// Creates an exception without a message.
    public NodelibraryException() {
        super();
    }

        /// Creates an exception with a message.
    ///
    /// @param message error message
    public NodelibraryException(final String message) {
        super(message);
    }

        /// Creates an exception with a cause.
    ///
    /// @param cause underlying cause
    public NodelibraryException(final Throwable cause) {
        super(cause);
    }

        /// Creates an exception with a message and cause.
    ///
    /// @param message error message
    /// @param cause   underlying cause
    public NodelibraryException(final String message, final Throwable cause) {
        super(message, cause);
    }

        /// Creates an exception with full throwable settings.
    ///
    /// @param message            error message
    /// @param cause              underlying cause
    /// @param enableSuppression  whether suppression is enabled
    /// @param writableStackTrace whether the stack trace may be written
    public NodelibraryException(
            final String message,
            final Throwable cause,
            final boolean enableSuppression,
            final boolean writableStackTrace
    ) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
