package peruncs.datagrid.cluster.errors;

/// Reports a write refused because this process no longer owns the writer lease.
public class WriterFencedException extends ReplicationException {
    public WriterFencedException(final String message) { super(message); }
    public WriterFencedException(final String message, final Throwable cause) { super(message, cause); }
}
