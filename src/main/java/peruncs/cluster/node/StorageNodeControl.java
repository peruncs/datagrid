package peruncs.cluster.node;

import peruncs.cluster.api.ReplicationState;
import peruncs.cluster.api.ReplicationStatus;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.node.replication.ReplicationMetrics;

/// Protocol-neutral control and observability view of a node manager.
///
/// The assembly owns every manager's lifecycle, so borrowers — the
/// embedding application's boundary adapters — receive this view instead of
/// the manager: it exposes exactly the operations a boundary needs and no
/// `close()`. Closing a borrowed manager would double-dispose resources the
/// assembly still owns; only the assembly closes managers, on [NodeAssembly#close].
///
/// @since 1.0
public interface StorageNodeControl {
    /// Internal unknown-value sentinel for the raw long metrics: the single
    /// convention every hook uses, mapped to an empty [java.util.OptionalLong]
    /// once at the exported [ReplicationStatus]
    /// boundary.
    long MISSING = -1L;

    /// Reports whether this node owns the single writer role.
    ///
    /// @return `true` for the writer, `false` for readers and backup readers
    default boolean isWriter() {
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
    /// @throws NodeException if readiness cannot be determined
    boolean isReady() throws NodeException;

        /// Reports whether the node and its transport are healthy.
    ///
    /// @return `true` when the node is healthy
    boolean isHealthy();

        /// Reads the current Store size.
    ///
    /// @return storage size in bytes
    /// @throws NodeException if the size cannot be read
    long readStorageSizeBytes() throws NodeException;

        /// Monitoring hook; nodes without a replication stream return `-1`.
    ///
    /// @return current applied sequence
    default long currentSequence() {
        return -1;
    }

        /// Monitoring hook; nodes without a replication stream return `-1`.
    ///
    /// @return latest writer sequence
    default long latestSequence() {
        return -1;
    }

        /// Assembles the point-in-time replication observability values.
    ///
    /// The values are sampled independently and may be torn across a
    /// transition; implementations that can snapshot their collaborators
    /// consistently should override this method. Returns `null` when this
    /// node runs without replication, so the exported status carries no
    /// placeholder metrics.
    ///
    /// @return raw replication metrics, or `null` without replication
    default ReplicationMetrics replicationMetrics() {
        return new ReplicationMetrics(
                this.currentSequence(),
                this.latestSequence(),
                this.replicationState(),
                this.isReady(),
                this.isHealthy(),
                this.archiveUsableSpaceBytes(),
                this.writerDurablePosition(),
                this.writerDurableSequence(),
                this.appliedSequence());
    }

        /// Monitoring hook for provider lifecycle state.
    ///
    /// @return replication state
    default ReplicationState replicationState() {
        if (this.isHealthy()) {
            return ReplicationState.LIVE;
        }
        return this.isReady() ? ReplicationState.STARTING : ReplicationState.FAILED;
    }

        /// Monitoring hook for the selected provider's Archive capacity.
    ///
    /// @return usable archive space in bytes
    default long archiveUsableSpaceBytes() {
        return -1L;
    }

        /// Monitoring hook for the writer's last durable recording position.
    ///
    /// @return durable recording position
    default long writerDurablePosition() {
        return -1L;
    }

        /// Monitoring hook for the writer's last durable sequence.
    ///
    /// @return durable sequence
    default long writerDurableSequence() {
        return -1L;
    }

        /// Monitoring hook for the reader's last applied sequence.
    ///
    /// @return applied sequence
    default long appliedSequence() {
        return -1L;
    }
}
