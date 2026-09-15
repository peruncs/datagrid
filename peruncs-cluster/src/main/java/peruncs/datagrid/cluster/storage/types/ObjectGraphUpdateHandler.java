package peruncs.datagrid.cluster.storage.types;


import org.eclipse.serializer.concurrency.XThreads;

import java.time.Duration;
import java.util.concurrent.StructuredTaskScope;

import static org.eclipse.serializer.util.X.notNull;

/// This handler decides when a received storage update may touch the graph.
///
/// The built-in synchronized handler applies one update at a time. Framework
/// integrations can provide a handler that uses their own cluster lock. The
/// method returns only after the updater has finished, because replication
/// cursor persistence depends on that completion boundary.
@FunctionalInterface
public interface ObjectGraphUpdateHandler {
        /// Creates a handler that serializes updates on the Store lock.
    ///
    /// @return synchronized update handler
    static ObjectGraphUpdateHandler Synchronized() {
        return updater -> XThreads.executeSynchronized(updater::updateObjectGraph);
    }

        /// Runs an update in a structured virtual-thread child.
    ///
    /// The child is joined before this method returns, so imported direct
    /// buffers cannot outlive the graph update that consumes them. The scope
    /// also inherits any scoped values established by the caller.
    ///
    /// @param handler update handler
    /// @param updater update to run
    /// @param timeout maximum time allowed for the structured update
    /// @throws InterruptedException if the owner is interrupted while joining
    /// @throws StructuredTaskScope.FailedException if the update fails
    /// @throws StructuredTaskScope.TimeoutException if the timeout expires
    static void runStructured(
            final ObjectGraphUpdateHandler handler,
            final ObjectGraphUpdater updater,
            final Duration timeout
    ) throws InterruptedException {
        notNull(handler);
        notNull(updater);
        notNull(timeout);
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("structured graph-update timeout must be positive");
        }
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.<Void>awaitAllSuccessfulOrThrow(),
                configuration -> configuration.withTimeout(timeout))) {
            scope.fork(() -> {
                handler.objectGraphUpdateAvailable(updater);
                return null;
            });
            scope.join();
        }
    }

        /// Runs an update when the object graph may be changed.
    ///
    /// The method must not return until the update has finished.
    /// If the update fails or times out, the caller must leave its replication
    /// cursor at the previous durable boundary and retry or reseed; the update
    /// result is never acknowledged merely because the child was forked.
    ///
    /// @param updater update to run
    void objectGraphUpdateAvailable(ObjectGraphUpdater updater);

}
