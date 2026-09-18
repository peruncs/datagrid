package peruncs.datagrid.cluster.node;


import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationHealth;

/// Protocol-neutral control and observability view of a node manager.
///
/// The foundation owns every manager's lifecycle, so borrowers — the
/// embedding application's boundary adapters — receive this view instead of
/// the manager: it exposes exactly the operations a boundary needs and no
/// `close()`. Closing a borrowed manager would double-dispose resources the
/// foundation still owns; only the foundation closes managers, on [ClusterFoundation#close].
///
/// @since 1.0
public interface StorageNodeControl {
    /// Reports whether this node owns the single writer role.
    ///
    /// @return `true` for the writer, `false` for readers and backup readers
    default boolean isDistributor() {
        return false;
    }

        /// Starts periodic storage checks.
    void startStorageChecks();

        /// Reports whether storage checks are running.
    ///
    /// @return `true` when checks are running
    boolean isRunningStorageChecks();

        /// Reports whether the node can serve requests.
    ///
    /// @return `true` when the node is ready
    /// @throws NodeLibraryException if readiness cannot be determined
    boolean isReady() throws NodeLibraryException;

        /// Reports whether the node and its transport are healthy.
    ///
    /// @return `true` when the node is healthy
    boolean isHealthy();

        /// Reads the current Store size.
    ///
    /// @return storage size in bytes
    /// @throws NodeLibraryException if the size cannot be read
    long readStorageSizeBytes() throws NodeLibraryException;

        /// Monitoring hook; nodes without a replication stream return `-1`.
    ///
    /// @return current applied sequence
    default long getCurrentSequence() {
        return -1;
    }

        /// Monitoring hook; nodes without a replication stream return `-1`.
    ///
    /// @return latest writer sequence
    default long getLatestSequence() {
        return -1;
    }

        /// Assembles the point-in-time replication observability values.
    ///
    /// @return raw replication metrics
    default ReplicationMetrics replicationMetrics() {
        return ReplicationMetrics.of(
                this.getCurrentSequence(),
                this.getLatestSequence(),
                this.getReplicationTransport(),
                this.getReplicationState(),
                this.isReady(),
                this.isHealthy(),
                this.getArchiveUsableSpaceBytes(),
                this.getWriterDurablePosition(),
                this.getWriterDurableSequence(),
                this.getAppliedSequence());
    }

        /// Monitoring hook for the selected provider.
    ///
    /// @return replication transport name
    default String getReplicationTransport() {
        return "none";
    }

        /// Monitoring hook for provider lifecycle state.
    ///
    /// @return replication state
    default ReplicationHealth.State getReplicationState() {
        return isHealthy() ? ReplicationHealth.State.LIVE : ReplicationHealth.State.STARTING;
    }

        /// Monitoring hook for the selected provider's Archive capacity.
    ///
    /// @return usable archive space in bytes
    default long getArchiveUsableSpaceBytes() {
        return -1L;
    }

        /// Monitoring hook for the writer's last durable recording position.
    ///
    /// @return durable recording position
    default long getWriterDurablePosition() {
        return -1L;
    }

        /// Monitoring hook for the writer's last durable sequence.
    ///
    /// @return durable sequence
    default long getWriterDurableSequence() {
        return -1L;
    }

        /// Monitoring hook for the reader's last applied sequence.
    ///
    /// @return applied sequence
    default long getAppliedSequence() {
        return -1L;
    }
}
