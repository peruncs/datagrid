package peruncs.datagrid.cluster.errors;

/// Base failure for replication operations.
public class ReplicationException extends IllegalStateException {
    public ReplicationException(final String message) {
        super(message);
    }

    public ReplicationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
