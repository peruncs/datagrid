package peruncs.datagrid.cluster.node.replication;

import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;

/// Replication-provider health.
///
/// This neutral contract keeps node readiness independent of the Aeron
/// implementation. Aeron is currently the only production provider, but the
/// node layer also needs a no-replication view and small test doubles.
public interface ReplicationHealth extends AutoCloseable {
        /// Returns true only when the node may serve the configured replication role.
    ///
    /// @return `true` when ready
    /// @throws NodeLibraryException if readiness cannot be checked
    boolean isReady() throws NodeLibraryException;

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
    /// A provider whose readiness probe throws is reported as [#State.FAILED]
    /// and the failure is logged at debug level so a monitoring scrape never
    /// hides the failure cause completely.
    ///
    /// @return health state
    default State state() {
        try {
            return isReady() ? State.LIVE : State.STARTING;
        } catch (final RuntimeException failure) {
            LOGGER.log(System.Logger.Level.DEBUG, "Replication readiness probe failed; reporting FAILED", failure);
            return State.FAILED;
        }
    }

    @Override
    void close();

    /// Logger shared by the default health-state implementation.
    System.Logger LOGGER = System.getLogger(ReplicationHealth.class.getName());

        /// States reported while a provider starts, runs, or requires recovery.
    enum State {
                /// Provider is starting.
        STARTING,
                /// Provider is replaying data.
        REPLAYING,
                /// Provider is live.
        LIVE,
                /// Archive access is degraded.
        DEGRADED_ARCHIVE,
                /// Provider needs a new seed.
        RESEED_REQUIRED,
                /// Provider has failed.
        FAILED
    }
}
