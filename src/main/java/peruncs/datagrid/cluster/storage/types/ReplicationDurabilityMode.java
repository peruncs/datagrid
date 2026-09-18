package peruncs.datagrid.cluster.storage.types;

/// Ordering contract between a local Store enqueue and a replication log.
/// Store 5.x exposes enqueue acceptance, not a durable-completion callback.
///
/// The two modes are distinct failure contracts, not performance tiers.
/// `ARCHIVE_FIRST` is the default: the prepared transaction is recorded
/// before the local Store enqueue, so a failure never leaves a local write
/// that readers cannot replay. `ENQUEUE_THEN_ARCHIVE` accepts the local
/// Store enqueue first and must be chosen deliberately (see its contract).
///
/// The codes are persisted in replication checkpoints, so they are stable:
/// never reorder the constants or reuse a code.
public enum ReplicationDurabilityMode {
        /// Record the prepared transaction before accepting the local Store enqueue.
    ARCHIVE_FIRST(1),
        /// Accept the local Store enqueue before recording the transaction.
    ///
    /// When preparation fails after the local write, the writer records a
    /// non-terminal `COMMITTING_UNCERTAIN` fence, consumes the sequence, and
    /// throws a reseed-required failure. The local write is durable but has
    /// no Archive copy, so readers can never replay it and the node must not
    /// keep serving it as replicated state.
    ///
    /// Operational recovery: stop the writer, reseed the node from a healthy
    /// peer or a backup (the uncertain fence forces restart recovery to
    /// refuse the sequence), then restart. Never clear the fence to resume
    /// in place: the local Store and the Archive would diverge silently.
    ENQUEUE_THEN_ARCHIVE(2);

    private final int code;

    ReplicationDurabilityMode(final int code) {
        this.code = code;
    }

        /// Returns the stable persisted code for this mode.
    ///
    /// @return stable persisted code
    public int code() {
        return this.code;
    }

        /// Resolves a persisted durability code.
    ///
    /// @param code persisted code
    /// @return durability mode
    public static ReplicationDurabilityMode fromCode(final int code) {
        return switch (code) {
            case 1 -> ARCHIVE_FIRST;
            case 2 -> ENQUEUE_THEN_ARCHIVE;
            default -> throw new IllegalArgumentException("unknown replication durability mode: %s".formatted(code));
        };
    }
}
