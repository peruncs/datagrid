package peruncs.datagrid.cluster.storage.aeron.wire;

import peruncs.datagrid.cluster.storage.types.StorageBinaryDataException;

/// Signals malformed or corrupted bytes received from an Aeron peer.
public final class ReplicationWireException extends StorageBinaryDataException {
        /// Creates a wire failure with a diagnostic message.
    public ReplicationWireException(final String message) {
        super(message);
    }
}
