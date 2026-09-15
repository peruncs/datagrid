package peruncs.datagrid.cluster.storage.types;

/// Shared hard limits for every replication frame.
public final class ReplicationLimits {
        /// Maximum bytes in one replicated message.
    public static final int MAX_MESSAGE_BYTES = 256 * 1024 * 1024;
        /// Maximum packets in one replicated message.
    public static final int MAX_PACKET_COUNT = 1_000_000;

    private ReplicationLimits() {
    }
}
