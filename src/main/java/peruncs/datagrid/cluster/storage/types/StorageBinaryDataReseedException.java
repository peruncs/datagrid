package peruncs.datagrid.cluster.storage.types;

import java.io.Serial;

/// Signals that a replication reader can no longer continue from its durable
/// cursor and must be reseeded.
///
/// Raised when the replication recording no longer covers the reader's cursor
/// (retention or a lost Archive catalog truncated history below it), or when
/// the reader's bounded reconnect budget expired while the Archive control
/// channel stayed down. This is not corrupt data: [StorageBinaryDataException]
/// covers invalid assembled content, whereas this type means the local state
/// is unrecoverable from the stream alone and the node must reseed from a
/// healthy peer or backup instead of retrying forever.
public class StorageBinaryDataReseedException extends IllegalStateException {
    @Serial
    private static final long serialVersionUID = 1L;

    /// Creates a reseed-required failure with a diagnostic message.
    ///
    /// @param message diagnostic message
    public StorageBinaryDataReseedException(final String message) {
        super(message);
    }

    /// Creates a reseed-required failure with the underlying transport cause.
    ///
    /// @param message diagnostic message
    /// @param cause   underlying transport failure
    public StorageBinaryDataReseedException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
