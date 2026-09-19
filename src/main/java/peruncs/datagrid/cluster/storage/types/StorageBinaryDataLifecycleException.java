package peruncs.datagrid.cluster.storage.types;

import java.io.Serial;

/// Signals that the local replication lifecycle could not reach a durable
/// boundary: a wait timed out, a wait or shutdown was interrupted, or the
/// materialization worker did not terminate.
///
/// This is not an invalid-message signal. [StorageBinaryDataException] is
/// reserved for corrupt or unusable assembled data; a lifecycle failure
/// instead means the local merger state is unsafe to continue with, and a
/// caller may retry disposal or fail the node based on its own policy.
public class StorageBinaryDataLifecycleException extends IllegalStateException {
    @Serial
    private static final long serialVersionUID = 1L;

        /// Creates an exception with a message.
    ///
    /// @param message diagnostic message
    public StorageBinaryDataLifecycleException(final String message) {
        super(message);
    }

        /// Creates an exception with an underlying wait or termination failure.
    ///
    /// @param message diagnostic message
    /// @param cause   underlying failure
    public StorageBinaryDataLifecycleException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
