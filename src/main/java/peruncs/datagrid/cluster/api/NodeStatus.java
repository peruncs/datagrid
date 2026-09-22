package peruncs.datagrid.cluster.api;

/// Immutable operational snapshot of one node.
///
/// @param writer whether this node owns the writer role
/// @param ready whether it may serve requests
/// @param healthy whether it has no terminal failure
/// @param storageChecksRunning whether periodic storage checks are active
/// @param storageBytes current Store size
/// @param transport selected replication transport
/// @param replicationState replication lifecycle
/// @param currentSequence local resolved sequence
/// @param latestSequence latest observed writer sequence
/// @param archiveUsableBytes local Archive usable bytes, or `-1`
/// @param writerDurablePosition durable writer recording position, or `-1`
/// @param writerDurableSequence durable writer sequence, or `-1`
/// @param appliedSequence locally applied sequence, or `-1`
public record NodeStatus(
        boolean writer,
        boolean ready,
        boolean healthy,
        boolean storageChecksRunning,
        long storageBytes,
        String transport,
        ReplicationState replicationState,
        long currentSequence,
        long latestSequence,
        long archiveUsableBytes,
        long writerDurablePosition,
        long writerDurableSequence,
        long appliedSequence
) {
}
