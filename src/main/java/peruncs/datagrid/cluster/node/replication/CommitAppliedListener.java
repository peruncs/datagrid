package peruncs.datagrid.cluster.node.replication;

import peruncs.datagrid.cluster.errors.NodeException;
import peruncs.datagrid.cluster.storage.ReplicationCursor;

/// Callback invoked after a replicated data message has been applied.
///
/// The replication cursor is the commit point for follow-up bookkeeping.
/// The listener is also closeable so a reader can release any callback state
/// during shutdown.
public interface CommitAppliedListener extends AutoCloseable {
        /// Records that one replicated message has been applied.
    ///
    /// @param cursor applied replication cursor
    /// @throws NodeException if follow-up bookkeeping fails
    void onApplied(ReplicationCursor cursor) throws NodeException;

    @Override
    void close();
}
