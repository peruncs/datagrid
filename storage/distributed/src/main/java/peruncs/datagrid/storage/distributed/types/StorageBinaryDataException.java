package peruncs.datagrid.storage.distributed.types;

/** Signals an invalid or unusable assembled Store replication message. */
public class StorageBinaryDataException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates an exception with a message.
     *
     * @param message diagnostic message
     */
    public StorageBinaryDataException(final String message) {
        super(message);
    }

    /**
     * Creates an exception with an underlying wire or storage failure.
     *
     * @param message diagnostic message
     * @param cause   underlying failure
     */
    public StorageBinaryDataException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
