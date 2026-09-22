package peruncs.datagrid.cluster.errors;

/// Reports a replication service that cannot currently make progress.
public class ReplicationUnavailableException extends ReplicationException {
    private final int errorCode;

    public ReplicationUnavailableException(final String message, final Throwable cause) {
        this(message, cause, 0);
    }

    public ReplicationUnavailableException(final String message, final Throwable cause, final int errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public int errorCode() {
        return this.errorCode;
    }
}
