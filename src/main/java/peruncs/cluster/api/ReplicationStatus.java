package peruncs.cluster.api;

import java.util.Objects;

/// Reports one node's replication position and health at a moment in time.
///
/// Aeron is the only replication transport, so no transport selector is
/// exposed. A metric is `-1` when it does not apply to this node's role or
/// its boundary is unknown; the matching `has…()` method reports availability.
///
/// [#lagTransactions()] is derived from the two sequence boundaries, so a
/// caller can never construct a snapshot with an inconsistent lag.
///
/// @param state replication lifecycle
/// @param currentSequence local resolved sequence, or -1 when unknown
/// @param latestSequence latest observed writer sequence, or -1 when unknown
/// @param archiveUsableBytes local Archive usable bytes, or -1 when unknown
/// @param writerDurablePosition durable writer position, or -1 when unknown
/// @param writerDurableSequence durable writer sequence, or -1 when unknown
/// @param appliedSequence locally applied sequence, or -1 when unavailable
///
/// @since 1.0
public record ReplicationStatus(
        ReplicationState state,
        long currentSequence,
        long latestSequence,
        long archiveUsableBytes,
        long writerDurablePosition,
        long writerDurableSequence,
        long appliedSequence
) {
    private static final long UNKNOWN = -1L;
    private static final ReplicationStatus NOT_CONFIGURED = new ReplicationStatus(
            ReplicationState.NOT_CONFIGURED, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN);

    /// Returns the shared status for a node without replication.
    public static ReplicationStatus notConfigured() {
        return NOT_CONFIGURED;
    }

    /// Validates the immutable replication status.
    public ReplicationStatus {
        Objects.requireNonNull(state, "state");
        requireKnownOrUnknown("currentSequence", currentSequence);
        requireKnownOrUnknown("latestSequence", latestSequence);
        requireKnownOrUnknown("archiveUsableBytes", archiveUsableBytes);
        requireKnownOrUnknown("writerDurablePosition", writerDurablePosition);
        requireKnownOrUnknown("writerDurableSequence", writerDurableSequence);
        requireKnownOrUnknown("appliedSequence", appliedSequence);
    }

    /// Whether the current local sequence is known.
    public boolean hasCurrentSequence() { return this.currentSequence >= 0L; }

    /// Whether the latest writer sequence is known.
    public boolean hasLatestSequence() { return this.latestSequence >= 0L; }

    /// Whether Archive usable bytes are known.
    public boolean hasArchiveUsableBytes() { return this.archiveUsableBytes >= 0L; }

    /// Whether the writer's durable recording position is known.
    public boolean hasWriterDurablePosition() { return this.writerDurablePosition >= 0L; }

    /// Whether the writer's durable sequence is known.
    public boolean hasWriterDurableSequence() { return this.writerDurableSequence >= 0L; }

    /// Whether an applied sequence is available.
    public boolean hasAppliedSequence() { return this.appliedSequence >= 0L; }

    /// Derives the replication lag as `latestSequence - currentSequence`:
    /// how many committed writer transactions this node has observed but not
    /// yet durably applied.
    ///
    /// @return transactions behind the latest writer sequence, or -1 when
    /// either boundary is unknown
    public long lagTransactions() {
        if (!this.hasLagTransactions()) return UNKNOWN;
        return this.latestSequence <= this.currentSequence ? 0L : this.latestSequence - this.currentSequence;
    }

    /// Whether a transaction lag can be calculated.
    public boolean hasLagTransactions() {
        return this.hasCurrentSequence() && this.hasLatestSequence();
    }

    private static void requireKnownOrUnknown(final String name, final long value) {
        if (value < UNKNOWN) throw new IllegalArgumentException(name + " must be -1 when unknown");
    }
}
