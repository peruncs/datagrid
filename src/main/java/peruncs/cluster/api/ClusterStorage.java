package peruncs.cluster.api;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import peruncs.cluster.node.NodeLifecycle;

import java.util.function.Supplier;

/// Starts an Eclipse Store-compatible manager with the default node settings.
///
/// @since 1.0
public final class ClusterStorage {
    private ClusterStorage() {
    }

    /// Starts the node and returns its guarded Store manager.
    ///
    /// @param rootSupplier creates the root when the Store is empty
    /// @param <T> root type
    /// @return started Store manager
    public static <T> ClusterStorageManager<T> start(final Supplier<? extends T> rootSupplier) {
        return ClusterStorage.<T>Foundation().setRootSupplier(rootSupplier).start();
    }

    static <T> ClusterNode<T> startNode(
            final Supplier<? extends T> rootSupplier,
            final EmbeddedStorageFoundation<?> foundation,
            final NodeConfig config) {
        return NodeLifecycle.startClusterNode(rootSupplier, foundation, config);
    }

    /// Creates immutable startup options for callers that need the node control handle.
    ///
    /// @param <T> root type
    /// @return startup options
    public static <T> ClusterStorageFoundation<T> Foundation() {
        return new ClusterStorageFoundation<>();
    }

}
