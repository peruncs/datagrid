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
/// @param writerDurableBoundary the writer's durable position and sequence
/// sampled together, empty components when unavailable for this role
/// @param appliedSequence locally applied sequence, empty when unavailable
/// for this role
public record ReplicationStatus(
        ReplicationState state,
        long currentSequence,
        long latestSequence,
        OptionalLong archiveUsableBytes,
        WriterDurableBoundary writerDurableBoundary,
        OptionalLong appliedSequence
) {
    /// Validates the immutable replication status.
    public ReplicationStatus {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(archiveUsableBytes, "archiveUsableBytes");
        Objects.requireNonNull(writerDurableBoundary, "writerDurableBoundary");
        Objects.requireNonNull(appliedSequence, "appliedSequence");
    }

    /// The writer's durable recording position and transaction sequence,
    /// sampled as one pair so a snapshot can never mix two samples.
    ///
    /// @param position durable writer recording position, empty when
    /// unavailable for this role
    /// @param sequence durable writer sequence, empty when unavailable for
    /// this role
    public record WriterDurableBoundary(
            OptionalLong position,
            OptionalLong sequence
    ) {
        /// Validates the immutable sampled pair.
        public WriterDurableBoundary {
            Objects.requireNonNull(position, "position");
            Objects.requireNonNull(sequence, "sequence");
        }
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
