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
}
