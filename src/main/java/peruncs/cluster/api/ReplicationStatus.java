package peruncs.cluster.api;

import java.util.Objects;
import java.util.OptionalLong;

/// Reports one node's replication position and health at a moment in time.
///
/// Aeron is the only replication transport, so no transport selector is
/// exposed. An [OptionalLong] is empty when a metric does not apply to this
/// node's role or its boundary is unknown. No `-1` placeholder leaks into
/// this record.
///
/// [#lagTransactions()] is derived from the two sequence boundaries, so a
/// caller can never construct a snapshot with an inconsistent lag.
///
/// @param state replication lifecycle
/// @param currentSequence local resolved sequence, empty when unknown
/// @param latestSequence latest observed writer sequence, empty when unknown
/// @param archiveUsableBytes local Archive usable bytes, empty when
/// unavailable for this role
/// @param writerDurableBoundary the writer's durable position and sequence
/// sampled together, empty components when unavailable for this role
/// @param appliedSequence locally applied sequence, empty when unavailable
/// for this role
public record ReplicationStatus(
        ReplicationState state,
        OptionalLong currentSequence,
        OptionalLong latestSequence,
        OptionalLong archiveUsableBytes,
        WriterDurableBoundary writerDurableBoundary,
        OptionalLong appliedSequence
) {
    /// Validates the immutable replication status.
    public ReplicationStatus {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(currentSequence, "currentSequence");
        Objects.requireNonNull(latestSequence, "latestSequence");
        Objects.requireNonNull(archiveUsableBytes, "archiveUsableBytes");
        Objects.requireNonNull(writerDurableBoundary, "writerDurableBoundary");
        Objects.requireNonNull(appliedSequence, "appliedSequence");
        requireNonNegative("currentSequence", currentSequence);
        requireNonNegative("latestSequence", latestSequence);
        requireNonNegative("archiveUsableBytes", archiveUsableBytes);
        requireNonNegative("writerDurableBoundary.position", writerDurableBoundary.position());
        requireNonNegative("writerDurableBoundary.sequence", writerDurableBoundary.sequence());
        requireNonNegative("appliedSequence", appliedSequence);
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
            requireNonNegative("position", position);
            requireNonNegative("sequence", sequence);
        }
    }

    /// Derives the replication lag as `latestSequence - currentSequence`:
    /// how many committed writer transactions this node has observed but not
    /// yet durably applied.
    ///
    /// @return non-negative transactions behind the latest writer sequence,
    /// or empty when either boundary is unknown
    public OptionalLong lagTransactions() {
        if (this.currentSequence.isEmpty() || this.latestSequence.isEmpty()) return OptionalLong.empty();
        return OptionalLong.of(Math.max(0L, this.latestSequence.getAsLong() - this.currentSequence.getAsLong()));
    }

    private static void requireNonNegative(final String name, final OptionalLong value) {
        if (value.isPresent() && value.getAsLong() < 0L) {
            throw new IllegalArgumentException(name + " must be non-negative when present");
        }
    }
}
