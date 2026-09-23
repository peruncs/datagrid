package peruncs.datagrid.cluster.node.store;

import org.eclipse.store.storage.types.StorageManager;

import static org.eclipse.serializer.util.X.notNull;

/// Owns startup and staged, idempotent shutdown for a cluster Store facade.
///
/// Shutdown runs the node-specific callback first and the Store shutdown
/// second, each at most once, so a failed stage can be retried without
/// repeating completed work. A failed stage rethrows its original exception
/// (an [Error] or [RuntimeException] as-is, anything else wrapped in an
/// [IllegalStateException]) while the other stage still runs.
final class ClusterStoreLifecycle {
    private static final System.Logger LOGGER = System.getLogger(ClusterStoreLifecycle.class.getName());

    private final ClusterStorageManager.ShutdownCallback shutdownCallback;
    private boolean callbackCompleted;
    private boolean storeShutdownCompleted;

    /// Creates a lifecycle running the given callback before Store shutdown.
    ///
    /// @param shutdownCallback shutdown callback
    ClusterStoreLifecycle(final ClusterStorageManager.ShutdownCallback shutdownCallback) {
        this.shutdownCallback = shutdownCallback;
    }

    /// Starts the Store.
    ///
    /// @param store Store manager
    void start(final StorageManager store) {
        notNull(store).start();
    }

    /// Shuts the callback and the Store down, once each and in that order.
    ///
    /// @param store Store manager
    /// @return `true` when this call shut the Store down; `false` when both
    /// stages were already completed
    synchronized boolean shutdown(final StorageManager store) {
        if (this.callbackCompleted && this.storeShutdownCompleted) {
            return false;
        }
        LOGGER.log(System.Logger.Level.INFO, "Shutting down ClusterStorageManager");
        Throwable failure = null;
        boolean result = false;
        if (!this.callbackCompleted) {
            try {
                this.shutdownCallback.onShutdown();
                this.callbackCompleted = true;
            } catch (final Throwable callbackFailure) {
                failure = callbackFailure;
            }
        }
        if (!this.storeShutdownCompleted) {
            try {
                result = store.shutdown();
                this.storeShutdownCompleted = true;
            } catch (final Throwable shutdownFailure) {
                if (failure == null) failure = shutdownFailure;
                else if (failure != shutdownFailure) failure.addSuppressed(shutdownFailure);
            }
        }
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure != null) throw new IllegalStateException("Cluster Store shutdown failed", failure);
        return result;
    }
}
