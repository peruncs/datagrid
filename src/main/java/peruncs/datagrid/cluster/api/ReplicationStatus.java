package peruncs.datagrid.cluster.api;

import java.util.Objects;
import java.util.OptionalLong;

/// Reports one node's replication position and health at a moment in time.
///
/// Aeron is the only replication transport, so no transport selector is
/// exposed. Absence is the single documented sentinel: an [OptionalLong] is
/// empty when a metric does not apply to this node's role — for example, a
/// writer observes no reader-applied sequence, and a reader observes no local
/// Archive usable space on the publication path. No `-1` placeholder ever
/// leaks into this record.
///
/// [#lagTransactions()] is derived from the two sequence boundaries, so a
/// caller can never construct a snapshot with an inconsistent lag.
///
/// @param state replication lifecycle
/// @param currentSequence local resolved sequence
/// @param latestSequence latest observed writer sequence
/// @param archiveUsableBytes local Archive usable bytes, empty when
/// unavailable for this role
/// @param writerDurablePosition durable writer recording position, empty
/// when unavailable for this role
/// @param writerDurableSequence durable writer sequence, empty when
/// unavailable for this role
/// @param appliedSequence locally applied sequence, empty when unavailable
/// for this role
public record ReplicationStatus(
        ReplicationState state,
        long currentSequence,
        long latestSequence,
        OptionalLong archiveUsableBytes,
        OptionalLong writerDurablePosition,
        OptionalLong writerDurableSequence,
        OptionalLong appliedSequence
) {
    /// Validates the immutable replication status.
    public ReplicationStatus {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(archiveUsableBytes, "archiveUsableBytes");
        Objects.requireNonNull(writerDurablePosition, "writerDurablePosition");
        Objects.requireNonNull(writerDurableSequence, "writerDurableSequence");
        Objects.requireNonNull(appliedSequence, "appliedSequence");
    }

    /// Derives the replication lag as `latestSequence - currentSequence`:
    /// how many committed writer transactions this node has observed but not
    /// yet durably applied.
    ///
    /// @return non-negative transactions behind the latest writer sequence
    public long lagTransactions() {
        return Math.max(0L, this.latestSequence - this.currentSequence);
    }
}
