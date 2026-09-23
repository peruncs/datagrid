package peruncs.datagrid.cluster.node.replication;

import peruncs.datagrid.cluster.api.ReplicationState;
import peruncs.datagrid.cluster.errors.NodeException;

/// Reports whether replication on this node can accept and apply transactions.
///
/// This neutral contract keeps node readiness independent of the Aeron
/// implementation. Aeron is currently the only production provider, but the
/// node layer also needs a no-replication view and small test doubles.
/// Lifecycle states are reported through the exported
/// [ReplicationState]: the api enum is the single definition, so internal
/// health probes and the exported [peruncs.datagrid.cluster.api.NodeStatus]
/// can never drift apart.
public interface ReplicationHealth extends AutoCloseable {
        /// Returns true only when the node may serve the configured replication role.
    ///
    /// @return `true` when ready
    /// @throws NodeException if readiness cannot be checked
    boolean isReady() throws NodeException;

        /// Returns true when the provider is operating without a fatal condition.
    ///
    /// @return `true` when healthy
    boolean isHealthy();

        /// Returns local Archive usable bytes, or `-1` when not applicable/known.
    ///
    /// @return usable bytes
    default long archiveUsableSpaceBytes() {
        return -1L;
    }

        /// Returns the last durable writer position, or `-1` when unavailable.
    ///
    /// @return durable position
    default long writerDurablePosition() {
        return -1L;
    }

        /// Returns the last durable writer sequence, or `-1` when unavailable.
    ///
    /// @return durable sequence
    default long writerDurableSequence() {
        return -1L;
    }

        /// Returns the locally applied reader sequence, or `-1` when unavailable.
    ///
    /// @return applied sequence
    default long appliedSequence() {
        return -1L;
    }

        /// Returns the current health state.
    ///
    /// A provider whose readiness probe throws is reported as
    /// [ReplicationState#FAILED] and the failure is logged at debug level so
    /// a monitoring scrape never hides the failure cause completely.
    ///
    /// @return health state
    default ReplicationState state() {
        try {
            return isReady() ? ReplicationState.LIVE : ReplicationState.STARTING;
        } catch (final RuntimeException failure) {
            LOGGER.log(System.Logger.Level.DEBUG, "Replication readiness probe failed; reporting FAILED", failure);
            return ReplicationState.FAILED;
        }
    }

    @Override
    void close();

    /// Logger shared by the default health-state implementation.
    System.Logger LOGGER = System.getLogger(ReplicationHealth.class.getName());
}
