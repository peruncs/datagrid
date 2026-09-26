package peruncs.cluster.node.aeron;

import peruncs.cluster.errors.ReseedRequiredException;

/// Immutable writer terminal boundary published after its checkpoint is durable.
///
/// @param sequence    durable replication sequence
/// @param recordingId Archive recording containing the boundary
/// @param position    exact durable Archive position
record AeronWriterRecoveryBoundary(long sequence, long recordingId, long position) {
        /// Fails closed unless a stopped Archive ends exactly at this durable boundary.
    void validateArchiveStop(final long stopPosition) {
        if (stopPosition < 0)
            throw new ReseedRequiredException("recording is still active");

        if (stopPosition < this.position)
            throw new ReseedRequiredException("archive stop position precedes checkpoint");

        if (stopPosition > this.position)
            throw new ReseedRequiredException("archive contains an uncheckpointed tail");
    }
}
