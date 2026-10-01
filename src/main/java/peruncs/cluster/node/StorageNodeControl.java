package peruncs.cluster.node;

import peruncs.cluster.api.ReplicationStatus;
import peruncs.cluster.errors.NodeException;

/// Protocol-neutral control and observability view of a node manager.
///
/// The assembly owns every manager's lifecycle, so borrowers — the
/// embedding application's boundary adapters — receive this view instead of
/// the manager: it exposes exactly the operations a boundary needs and no
/// `close()`. Closing a borrowed manager would double-dispose resources the
/// assembly still owns; only the assembly closes managers, on [NodeLifecycle#close].
///
/// @since 1.0
public interface StorageNodeControl {
    /// Reports whether this control is backed by the writer role.
    ///
    /// @return `true` for the writer
    boolean isWriter();

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

    /// Returns replication state and positions sampled for this node.
    ///
    /// @return one immutable status; nodes without replication return `NOT_CONFIGURED`
    ReplicationStatus replicationStatus();
}
