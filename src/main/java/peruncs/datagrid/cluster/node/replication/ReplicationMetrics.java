package peruncs.datagrid.cluster.node.replication;

import peruncs.datagrid.cluster.api.ReplicationState;

/// Point-in-time replication observability values for one node.
///
/// The record carries raw values only: no Prometheus exposition, no label
/// escaping, no Kubernetes metadata. The embedding application renders the
/// wire format its observability stack expects.
///
/// [#lagTransactions()] is derived from the two sequence boundaries, so a
/// caller can never construct a snapshot with an inconsistent lag.
///
/// Every `long` component uses [peruncs.datagrid.cluster.node.StorageNodeControl#MISSING]
/// for "unknown"; the exported status maps that sentinel to absence exactly
/// once. Aeron is the only transport, so no transport selector is carried.
///
/// @param currentSequence        last committed sequence applied locally
/// @param latestSequence         latest writer sequence observed, or MISSING when unknown
/// @param state                  provider lifecycle state
/// @param ready                  whether the node is ready
/// @param healthy                whether the node is healthy
/// @param archiveUsableSpaceBytes archive free bytes, or MISSING when unavailable
/// @param writerDurablePosition  last terminal recording position, or MISSING
/// @param writerDurableSequence  last terminal writer sequence, or MISSING
/// @param appliedSequence        last sequence applied by this node, or MISSING
public record ReplicationMetrics(
        long currentSequence,
        long latestSequence,
        ReplicationState state,
        boolean ready,
        boolean healthy,
        long archiveUsableSpaceBytes,
        long writerDurablePosition,
        long writerDurableSequence,
        long appliedSequence
) {
        /// Derives the lag from the two sequence boundaries.
    ///
    /// Lag is reported as `-1` while either boundary is unknown so an
    /// unknowable writer boundary never renders as a healthy zero lag;
    /// otherwise it is the non-negative distance from current to latest.
    ///
    /// @return transactions behind latest, or `-1` when either boundary is unknown
    public long lagTransactions() {
        if (this.currentSequence < 0L || this.latestSequence < 0L) {
            return -1L;
        }
        return Math.max(0L, this.latestSequence - this.currentSequence);
    }
}
