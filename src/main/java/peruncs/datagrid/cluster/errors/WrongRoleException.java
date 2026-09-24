package peruncs.datagrid.cluster.errors;

/// Reports an operation requested by a node whose configured role does not support it.
///
/// Kept distinct from a closed node and from generic state failures: an
/// operator seeing this type knows the node itself is healthy and only the
/// requested operation belongs to another role.
public final class WrongRoleException extends NodeException {
    /// Creates the exception with a message.
    ///
    /// @param message diagnostic message
    public WrongRoleException(final String message) {
        super(message);
    }
}
