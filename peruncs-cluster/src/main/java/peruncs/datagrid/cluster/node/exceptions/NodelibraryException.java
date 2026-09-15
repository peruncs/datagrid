package peruncs.datagrid.cluster.node.exceptions;


import org.eclipse.serializer.exceptions.BaseException;

/// Base runtime exception for node lifecycle and storage failures.
public class NodeLibraryException extends BaseException {
        /// Creates an exception without a message.
    public NodeLibraryException() {
        super();
    }

        /// Creates an exception with a message.
    ///
    /// @param message error message
    public NodeLibraryException(final String message) {
        super(message);
    }

        /// Creates an exception with a cause.
    ///
    /// @param cause underlying cause
    public NodeLibraryException(final Throwable cause) {
        super(cause);
    }

        /// Creates an exception with a message and cause.
    ///
    /// @param message error message
    /// @param cause   underlying cause
    public NodeLibraryException(final String message, final Throwable cause) {
        super(message, cause);
    }

        /// Creates an exception with full throwable settings.
    ///
    /// @param message            error message
    /// @param cause              underlying cause
    /// @param enableSuppression  whether suppression is enabled
    /// @param writableStackTrace whether the stack trace may be written
    public NodeLibraryException(
            final String message,
            final Throwable cause,
            final boolean enableSuppression,
            final boolean writableStackTrace
    ) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
