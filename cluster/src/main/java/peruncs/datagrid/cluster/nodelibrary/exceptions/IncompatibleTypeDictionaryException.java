package peruncs.datagrid.cluster.nodelibrary.exceptions;

/** Reports that a remote type definition conflicts with the local definition. */
public final class IncompatibleTypeDictionaryException extends NodelibraryException {
    /**
     * Creates an exception with a diagnostic message.
     *
     * @param message diagnostic message
     */
    public IncompatibleTypeDictionaryException(final String message) {
        super(message);
    }
}
