package peruncs.cluster.api;

/// Tells an operator whether this node can serve traffic and keep up with replication.
///
/// The snapshot separates node role and readiness from replication
/// observability: [#replication()] reports `NOT_CONFIGURED` when this node
/// has no replication transport. Its metric boundaries stay empty instead
/// of rendering as healthy zero values.
///
/// `FAILED` means stop serving, keep the node down, and inspect the terminal
/// cause before restarting. `DEGRADED` means the node may still serve reads,
/// but an Archive or maintenance dependency needs attention before it
/// escalates. `RESEED_REQUIRED` means stop the node, restore a compatible
/// Store image plus its durable cursor, and only then restart; the node
/// will not recover on its own. Replication lag is reported as
/// [ReplicationStatus#lagTransactions()], the gap between the latest writer
/// sequence this node observed and the sequence it has durably applied. It
/// is empty when either sequence boundary is unknown.
///
/// @param writer whether this node owns the writer role
/// @param ready whether it may serve requests
/// @param healthy whether it has no terminal failure; a latched graph
/// invalidity — a store update section that failed mid-application — makes
/// the node unhealthy and not ready until it reloads or reseeds
/// @param storageChecksRunning whether periodic storage checks are active
/// @param storageBytes current Store size
/// @param replication replication observability, including an explicit
///                    `NOT_CONFIGURED` state
/// @param backup last backup outcome; backup and post-publication maintenance
///               failures do not change node readiness or replication health
public record NodeStatus(
        boolean writer,
        boolean ready,
        boolean healthy,
        boolean storageChecksRunning,
        long storageBytes,
        ReplicationStatus replication,
        BackupStatus backup
) {
}
