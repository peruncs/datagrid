package peruncs.cluster.api;

/// Tells an operator whether this node can serve traffic and keep up with replication.
///
/// The snapshot separates node role and readiness from replication
/// observability: [#replication()] is `null` when this node has no
/// replication configured, because no replication metric exists for such a
/// node and a placeholder value would render as a healthy zero.
///
/// `FAILED` means stop serving, keep the node down, and inspect the terminal
/// cause before restarting. `DEGRADED` means the node may still serve reads,
/// but an Archive or maintenance dependency needs attention before it
/// escalates. `RESEED_REQUIRED` means stop the node, restore a compatible
/// Store image plus its durable cursor, and only then restart; the node
/// will not recover on its own. Replication lag is reported as
/// [ReplicationStatus#lagTransactions()], the gap between the latest writer
/// sequence this node observed and the sequence it has durably applied.
///
/// @param writer whether this node owns the writer role
/// @param ready whether it may serve requests
/// @param healthy whether it has no terminal failure
/// @param storageChecksRunning whether periodic storage checks are active
/// @param storageBytes current Store size
/// @param replication replication observability, or `null` when this node
/// has no replication configured
public record NodeStatus(
        boolean writer,
        boolean ready,
        boolean healthy,
        boolean storageChecksRunning,
        long storageBytes,
        ReplicationStatus replication
) {
}
