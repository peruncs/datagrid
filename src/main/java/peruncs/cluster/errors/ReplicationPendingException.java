package peruncs.cluster.errors;

/// Reports a Store commit accepted locally whose COMMIT marker is still pending.
///
/// The graph remains valid, but writer admission stays suspended until the
/// marker is published or restart recovery resolves the Store mark.
public final class ReplicationPendingException extends ReplicationException {
    private final long sequence;

    /// Creates a pending-commit failure.
    public ReplicationPendingException(final long sequence, final Throwable cause) {
        super("local Store commit %s is waiting for its replication COMMIT marker".formatted(sequence), cause);
        if (sequence < 0L) throw new IllegalArgumentException("sequence must be non-negative");
        this.sequence = sequence;
    }

    /// Returns the sequence whose COMMIT marker is pending.
    public long sequence() {
        return this.sequence;
    }
}
