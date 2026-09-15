package peruncs.datagrid.cluster.node.exceptions;


import org.eclipse.serializer.exceptions.BaseException;

/// Base runtime exception for node lifecycle and storage failures.
public class NodeLibraryException extends BaseException {
        /// Creates an exception with a message.
    ///
    /// @param message error message
    public NodeLibraryException(final String message) {
        super(message);
    }

        /// Creates an exception with a message and cause.
    ///
    /// @param message error message
    /// @param cause   underlying cause
    public NodeLibraryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
