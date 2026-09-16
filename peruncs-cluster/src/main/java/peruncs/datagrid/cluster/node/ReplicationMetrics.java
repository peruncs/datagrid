package peruncs.datagrid.cluster.node;

import peruncs.datagrid.cluster.node.replication.ReplicationHealth;

/// Point-in-time replication observability values for one node.
///
/// The record carries raw values only: no Prometheus exposition, no label
/// escaping, no Kubernetes metadata. The embedding application renders the
/// wire format its observability stack expects.
///
/// @param currentSequence        last committed sequence applied locally
/// @param latestSequence         latest writer sequence observed, or -1 when unknown
/// @param lagTransactions        transactions behind latest, or -1 when the writer boundary is unknown
/// @param transport              replication transport id, unescaped
/// @param state                  provider lifecycle state
/// @param ready                  whether the node is ready
/// @param healthy                whether the node is healthy
/// @param archiveUsableSpaceBytes archive free bytes, or -1 when unavailable
/// @param writerDurablePosition  last terminal recording position, or -1
/// @param writerDurableSequence  last terminal writer sequence, or -1
/// @param appliedSequence        last sequence applied by this node, or -1
public record ReplicationMetrics(
        long currentSequence,
        long latestSequence,
        long lagTransactions,
        String transport,
        ReplicationHealth.State state,
        boolean ready,
        boolean healthy,
        long archiveUsableSpaceBytes,
        long writerDurablePosition,
        long writerDurableSequence,
        long appliedSequence
) {
        /// Assembles metrics while owning the `-1`-when-unknown lag rule.
    ///
    /// Lag is reported as `-1` while either boundary is unknown so an
    /// unknowable writer boundary never renders as a healthy zero lag;
    /// otherwise it is the non-negative distance from current to latest.
    ///
    /// @param currentSequence        last committed sequence applied locally
    /// @param latestSequence         latest writer sequence observed, or -1 when unknown
    /// @param transport              replication transport id, unescaped
    /// @param state                  provider lifecycle state
    /// @param ready                  whether the node is ready
    /// @param healthy                whether the node is healthy
    /// @param archiveUsableSpaceBytes archive free bytes, or -1 when unavailable
    /// @param writerDurablePosition  last terminal recording position, or -1
    /// @param writerDurableSequence  last terminal writer sequence, or -1
    /// @param appliedSequence        last sequence applied by this node, or -1
    /// @return raw replication metrics
    public static ReplicationMetrics of(
            final long currentSequence,
            final long latestSequence,
            final String transport,
            final ReplicationHealth.State state,
            final boolean ready,
            final boolean healthy,
            final long archiveUsableSpaceBytes,
            final long writerDurablePosition,
            final long writerDurableSequence,
            final long appliedSequence
    ) {
        final long lag = currentSequence < 0L || latestSequence < 0L
                ? -1L
                : Math.max(0L, latestSequence - currentSequence);
        return new ReplicationMetrics(
                currentSequence,
                latestSequence,
                lag,
                transport,
                state,
                ready,
                healthy,
                archiveUsableSpaceBytes,
                writerDurablePosition,
                writerDurableSequence,
                appliedSequence);
    }
}
