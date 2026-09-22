package peruncs.datagrid.cluster.errors;

/// Reports durable local state that cannot safely resume replication.
public class ReseedRequiredException extends ReplicationException {
    public ReseedRequiredException(final String message) { super(message); }
    public ReseedRequiredException(final String message, final Throwable cause) { super(message, cause); }
}
