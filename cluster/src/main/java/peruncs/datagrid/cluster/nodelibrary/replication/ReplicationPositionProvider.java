package peruncs.datagrid.cluster.nodelibrary.replication;

import peruncs.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;

/// Latest-position and provider-readiness contract used by neutral lifecycle code.
public interface ReplicationPositionProvider extends AutoCloseable {
        /// Initializes any provider client needed to resolve the current position.
    void init() throws NodelibraryException;

        /// Returns the newest position that can be used as a backup/bootstrap boundary.
    ///
    /// @return latest replication cursor
    /// @throws NodelibraryException when this role cannot obtain a writer latest
    ///                              boundary or the provider cannot read its position
    ReplicationCursor latest() throws NodelibraryException;

        /// Returns the sequence from [#latest()].
    ///
    /// @return latest logical sequence
    /// @throws NodelibraryException if the position cannot be read
    default long latestSequence() throws NodelibraryException {
        return this.latest().logicalSequence();
    }

    @Override
    void close();
}
