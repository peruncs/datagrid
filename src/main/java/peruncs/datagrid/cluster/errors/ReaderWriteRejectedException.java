package peruncs.datagrid.cluster.errors;

/// Reports a write attempted through a read-only cluster node.
public class ReaderWriteRejectedException extends ReplicationException {
    public ReaderWriteRejectedException(final String message) { super(message); }
    public ReaderWriteRejectedException(final String message, final Throwable cause) { super(message, cause); }
}
