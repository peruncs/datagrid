package peruncs.datagrid.cluster.storage.types;

/// The only supported ordering between a Store write and its replication log.
///
/// The codes are persisted in replication checkpoints, so they are stable:
/// never reorder the constants or reuse a code.
public enum ReplicationDurabilityMode {
        /// Record the prepared transaction before accepting the local Store enqueue.
    ARCHIVE_FIRST(1);

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
    /// @throws StorageBinaryDataException if the persisted code is unknown,
    ///                                    which marks the file as corrupt
    public static ReplicationDurabilityMode fromCode(final int code) {
        return switch (code) {
            case 1 -> ARCHIVE_FIRST;
            default -> throw new StorageBinaryDataException("unknown replication durability mode: %s".formatted(code));
        };
    }
}
