package peruncs.datagrid.cluster.nodelibrary.exceptions;

/** Reports that the current node role cannot provide a writer position. */
public final class ReplicationPositionUnavailableException extends NodelibraryException {
    /**
     * Creates an exception with a diagnostic message.
     *
     * @param message diagnostic message
     */
    public ReplicationPositionUnavailableException(final String message) {
        super(message);
    }
}
