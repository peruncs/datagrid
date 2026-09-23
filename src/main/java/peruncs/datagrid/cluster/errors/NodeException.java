package peruncs.datagrid.cluster.errors;


import org.eclipse.serializer.exceptions.BaseException;

/// Signals a node lifecycle or storage failure that applications may catch at the boundary.
public class NodeException extends BaseException {
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
