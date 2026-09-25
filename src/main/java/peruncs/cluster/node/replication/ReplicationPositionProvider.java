package peruncs.cluster.node.replication;

import peruncs.cluster.errors.NodeException;
import peruncs.cluster.storage.ReplicationCursor;

/// Latest-position and provider-readiness contract used by neutral lifecycle code.
public interface ReplicationPositionProvider extends AutoCloseable {
        /// Initializes any provider client needed to resolve the current position.
    void init() throws NodeException;

        /// Returns the newest position that can be used as a backup/bootstrap boundary.
    ///
    /// @return latest replication cursor
    /// @throws NodeException when this role cannot obtain a writer latest
    ///                              boundary or the provider cannot read its position
    ReplicationCursor latest() throws NodeException;

    @Override
    void close();
}
