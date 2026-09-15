package peruncs.datagrid.cluster.node;

import peruncs.datagrid.cluster.node.replication.ReplicationHealth;

/// Point-in-time replication observability values for one node.
///
/// The record carries raw values only: no Prometheus exposition, no label
/// escaping, no Kubernetes metadata. The embedding application renders the
/// wire format its observability stack expects.
public record ReplicationMetrics(
        /// Last committed sequence applied locally.
        long currentSequence,
        /// Latest writer sequence observed, or -1 when unknown.
        long latestSequence,
        /// Transactions behind latest, or -1 when the writer boundary is unknown.
        long lagTransactions,
        /// Replication transport id, unescaped.
        String transport,
        /// Provider lifecycle state.
        ReplicationHealth.State state,
        /// Whether the node is ready.
        boolean ready,
        /// Whether the node is healthy.
        boolean healthy,
        /// Archive free bytes, or -1 when unavailable.
        long archiveUsableSpaceBytes,
        /// Last terminal recording position, or -1.
        long writerDurablePosition,
        /// Last terminal writer sequence, or -1.
        long writerDurableSequence,
        /// Last sequence applied by this node, or -1.
        long appliedSequence
) {
}
