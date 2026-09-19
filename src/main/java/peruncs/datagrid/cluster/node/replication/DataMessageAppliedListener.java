package peruncs.datagrid.cluster.node.replication;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.storage.types.ReplicationCursor;

/// Callback invoked after a replicated data message has been applied.
///
/// The replication cursor is the commit point for follow-up bookkeeping.
/// The listener is also closeable so a reader can release any callback state
/// during shutdown.
public interface DataMessageAppliedListener extends AutoCloseable {
        /// Records that one replicated message has been applied.
    ///
    /// @param cursor applied replication cursor
    /// @throws NodeLibraryException if follow-up bookkeeping fails
    void onApplied(ReplicationCursor cursor) throws NodeLibraryException;

    @Override
    void close();
}
