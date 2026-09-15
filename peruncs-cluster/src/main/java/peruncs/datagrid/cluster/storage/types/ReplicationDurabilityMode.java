package peruncs.datagrid.cluster.storage.types;

/// Ordering contract between a local Store enqueue and a replication log.
/// Store 5.x exposes enqueue acceptance, not a durable-completion callback.
///
/// The codes are persisted in replication checkpoints, so they are stable:
/// never reorder the constants or reuse a code.
public enum ReplicationDurabilityMode {
        /// Record the prepared transaction before accepting the local Store enqueue.
    ARCHIVE_FIRST(1),
        /// Accept the local Store enqueue before recording the transaction; recovery is conservative.
    ENQUEUE_THEN_ARCHIVE(2);

    private final int code;

    ReplicationDurabilityMode(final int code) {
        this.code = code;
    }

        /// Returns the stable persisted code for this mode.
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
