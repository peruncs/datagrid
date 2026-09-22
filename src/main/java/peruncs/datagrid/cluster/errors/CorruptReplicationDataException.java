package peruncs.datagrid.cluster.errors;

/// Reports invalid replication bytes or metadata.
public class CorruptReplicationDataException extends ReplicationException {
    public CorruptReplicationDataException(final String message) { super(message); }
    public CorruptReplicationDataException(final String message, final Throwable cause) { super(message, cause); }
}
