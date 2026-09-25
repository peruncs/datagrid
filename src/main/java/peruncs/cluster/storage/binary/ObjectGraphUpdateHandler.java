package peruncs.cluster.storage.binary;


import peruncs.cluster.storage.StorageGraphCoordinator;

import static org.eclipse.serializer.util.X.notNull;

/// Decides when a received storage update may touch the graph.
///
/// The built-in per-Store handler applies one update at a time on its
/// coordinator's write side. Framework integrations can provide a handler
/// that uses their own cluster lock. The method returns only after the
/// updater has finished, because replication cursor persistence depends on
/// that completion boundary.
@FunctionalInterface
public interface ObjectGraphUpdateHandler {
        /// Creates a handler that serializes updates on one Store's coordinator.
    ///
    /// The handler runs every materialization on the coordinator's write
    /// side, so ordinary application reads that use the coordinator's read
    /// side overlap each other but never observe a half-applied update. Each
    /// Store needs its own coordinator; sharing one across Stores needlessly
    /// serializes independent graphs.
    ///
    /// @param coordinator per-Store graph coordinator
    /// @return update handler scoped to one Store
    static ObjectGraphUpdateHandler PerStore(final StorageGraphCoordinator coordinator) {
        notNull(coordinator);
        return coordinator::write;
    }

        /// Runs an update when the object graph may be changed.
    ///
    /// The method must not return until the update has finished.
    /// If the update fails or times out, the caller must leave its replication
    /// cursor at the previous durable boundary and retry or reseed; the update
    /// result is never acknowledged merely because the update was scheduled
    /// elsewhere.
    ///
    /// @param updater update to run
    void objectGraphUpdateAvailable(Runnable updater);

}
