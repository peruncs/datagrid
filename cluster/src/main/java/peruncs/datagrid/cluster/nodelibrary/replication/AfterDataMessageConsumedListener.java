package peruncs.datagrid.cluster.nodelibrary.replication;


import peruncs.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;

/// This listener runs after a replicated data message has been applied.
///
/// The replication cursor is the commit point for follow-up bookkeeping.
/// The listener is also closeable so a reader can release any callback state
/// during shutdown.
public interface AfterDataMessageConsumedListener extends AutoCloseable {
        /// Records that one replicated message has been applied.
    ///
    /// @param cursor applied replication cursor
    /// @throws NodelibraryException if follow-up bookkeeping fails
    void onApplied(ReplicationCursor cursor) throws NodelibraryException;

    @Override
    void close();
}
