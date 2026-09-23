package peruncs.datagrid.cluster.node.store;

import org.eclipse.store.storage.types.StorageManager;
import peruncs.datagrid.cluster.errors.NodeException;
import peruncs.datagrid.cluster.storage.StorageGraphCoordinator;

import java.util.function.Function;

import static org.eclipse.serializer.util.X.notNull;

/// Guarded Store facade for application access on a cluster node.
///
/// The facade adapts Store's API for application access: it rejects writes
/// past the configured storage limit on a writer, rejects every application
/// write on a reader, and wraps the raw persistence target so a fluent binary
/// write cannot bypass the gate. Store import is deliberately unavailable
/// through this application-facing wrapper; reader roots are available only
/// through a coordinated read closure. Node-owned bootstrap and replication
/// code use their internal storage connection instead.
///
/// Startup and shutdown ordering are owned by the [ClusterStoreLifecycle]
/// behind each facade instance; this type carries no lifecycle state itself.
///
/// @param <T> root type
public interface ClusterStorageManager<T> extends StorageManager {
    /// Creates a manager with size validation and shutdown handling.
    ///
    /// @param <T>                   root type
    /// @param delegate              Store manager
    /// @param storageSizeValidation size validation policy
    /// @param shutdownCallback      shutdown callback
    /// @return cluster storage manager
    static <T> ClusterStorageManager<T> create(
            final StorageManager delegate,
            final StorageSizeValidation storageSizeValidation,
            final ShutdownCallback shutdownCallback
    ) {
        return create(delegate, storageSizeValidation, shutdownCallback, new StorageGraphCoordinator());
    }

    /// Creates a manager sharing the Store graph coordinator with replication.
    ///
    /// @param <T>                   root type
    /// @param delegate              Store manager
    /// @param storageSizeValidation size validation policy
    /// @param shutdownCallback      shutdown callback
    /// @param graphCoordinator      graph coordinator shared with replication
    /// @return cluster storage manager
    static <T> ClusterStorageManager<T> create(
            final StorageManager delegate,
            final StorageSizeValidation storageSizeValidation,
            final ShutdownCallback shutdownCallback,
            final StorageGraphCoordinator graphCoordinator
    ) {
        return new GuardingStorageManager<>(notNull(delegate), notNull(storageSizeValidation),
                notNull(shutdownCallback), notNull(graphCoordinator));
    }

    /// Creates a read-only manager for reader roles.
    ///
    /// Reads, maintenance, and restore keep working; every application write
    /// entry point — `store`, `storeAll`, `storeRoot`, `setRoot`, storers,
    /// raw persistence target, and public import methods —
    /// fails with [ReaderWriteRejectedException] so a reader can never
    /// persist an unreplicated local divergence.
    ///
    /// @param <T>              root type
    /// @param delegate         Store manager
    /// @param shutdownCallback shutdown callback
    /// @return read-only cluster storage manager
    static <T> ClusterStorageManager<T> ReadOnly(final StorageManager delegate, final ShutdownCallback shutdownCallback) {
        return ReadOnly(delegate, shutdownCallback, new StorageGraphCoordinator());
    }

    /// Creates a read-only manager sharing the Store graph coordinator.
    ///
    /// @param <T>              root type
    /// @param delegate         Store manager
    /// @param shutdownCallback shutdown callback
    /// @param graphCoordinator graph coordinator shared with replication
    /// @return read-only cluster storage manager
    static <T> ClusterStorageManager<T> ReadOnly(
            final StorageManager delegate,
            final ShutdownCallback shutdownCallback,
            final StorageGraphCoordinator graphCoordinator) {
        return new ReadOnlyStorageManager<>(notNull(delegate), notNull(shutdownCallback), notNull(graphCoordinator));
    }

    /// Reads the current root while excluding replication materialization.
    ///
    /// The inherited [#root()] method exposes Store's live lazy reference and
    /// cannot hold a lock across the caller's subsequent object-graph access,
    /// so it throws on readers: application code must use this closure, which
    /// covers the complete traversal with the coordinator's read side. The
    /// action must copy what it needs into an immutable or detached result and
    /// must not return any live graph object. Only a direct root return can be
    /// detected here; callers must also avoid returning nested mutable objects.
    ///
    /// @param <R>    result type
    /// @param action graph read; it receives the materialized root, or `null`
    /// @return action result, never the live graph
    <R> R readRoot(Function<? super T, ? extends R> action);

    /// Returns the graph coordinator guarding this Store.
    ///
    /// Use it to guard traversals that cannot go through [#readRoot], such as
    /// multi-call queries: `coordinator.read(() -> ...)`.
    ///
    /// @return graph coordinator for this Store
    StorageGraphCoordinator graphCoordinator();

    @Override
    ClusterStorageManager<T> start() throws NodeException;

    /// Runs node-specific work immediately before Store shuts down.
    interface ShutdownCallback {
        /// Creates a callback that does nothing.
        ///
        /// @return no-op callback
        static ShutdownCallback noOp() {
            return new NoOp();
        }

        /// Runs node-specific shutdown work.
        void onShutdown();

        /// A callback for applications that need no shutdown action.
        final class NoOp implements ShutdownCallback {
            private NoOp() {
            }

            @Override
            public void onShutdown() {
                // no-op
            }
        }
    }

    /// Reports whether the configured storage limit has been reached.
    interface StorageSizeValidation {
        /// Reports whether another Store write must be rejected.
        ///
        /// @return `true` when the limit is reached
        boolean isStorageLimitReached();

        /// Returns a validation that never rejects, for read-only managers.
        ///
        /// @return validation that never reports the limit as reached
        static StorageSizeValidation notReached() {
            return () -> false;
        }
    }
}
