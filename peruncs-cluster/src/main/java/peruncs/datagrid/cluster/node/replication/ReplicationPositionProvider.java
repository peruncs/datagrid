package peruncs.datagrid.cluster.node.replication;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;

/// Latest-position and provider-readiness contract used by neutral lifecycle code.
public interface ReplicationPositionProvider extends AutoCloseable {
        /// Initializes any provider client needed to resolve the current position.
    void init() throws NodeLibraryException;

        /// Returns the newest position that can be used as a backup/bootstrap boundary.
    ///
    /// @return latest replication cursor
    /// @throws NodeLibraryException when this role cannot obtain a writer latest
    ///                              boundary or the provider cannot read its position
    ReplicationCursor latest() throws NodeLibraryException;

        /// Returns the sequence from [#latest()].
    ///
    /// @return latest logical sequence
    /// @throws NodeLibraryException if the position cannot be read
    default long latestSequence() throws NodeLibraryException {
        return this.latest().logicalSequence();
    }

    @Override
    void close();
}
