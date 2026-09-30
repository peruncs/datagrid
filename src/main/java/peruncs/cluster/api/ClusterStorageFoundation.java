package peruncs.cluster.api;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import peruncs.cluster.errors.NodeException;

import java.util.Objects;
import java.util.function.Supplier;

/// Immutable startup options for a Store and its owning cluster node.
///
/// @param <T> root type
/// @since 1.0
public final class ClusterStorageFoundation<T> {
    private final Supplier<? extends T> rootSupplier;
    private final EmbeddedStorageFoundation<?> embeddedStorageFoundation;
    private final NodeConfig nodeConfig;

    ClusterStorageFoundation() {
        this(null, null, null);
    }

    private ClusterStorageFoundation(
            final Supplier<? extends T> rootSupplier,
            final EmbeddedStorageFoundation<?> embeddedStorageFoundation,
            final NodeConfig nodeConfig) {
        this.rootSupplier = rootSupplier;
        this.embeddedStorageFoundation = embeddedStorageFoundation;
        this.nodeConfig = nodeConfig;
    }

    /// Sets the supplier used when the Store has no root.
    ///
    /// @param supplier creates the root
    /// @return updated immutable options
    public ClusterStorageFoundation<T> setRootSupplier(final Supplier<? extends T> supplier) {
        return new ClusterStorageFoundation<>(Objects.requireNonNull(supplier, "supplier"),
                this.embeddedStorageFoundation, this.nodeConfig);
    }

    /// Sets custom Store tuning; the node still owns its live file provider.
    ///
    /// @param foundation Store foundation
    /// @return updated immutable options
    public ClusterStorageFoundation<T> setEmbeddedStorageFoundation(
            final EmbeddedStorageFoundation<?> foundation) {
        return new ClusterStorageFoundation<>(this.rootSupplier,
                Objects.requireNonNull(foundation, "foundation"), this.nodeConfig);
    }

    /// Sets immutable node configuration.
    ///
    /// @param config typed node configuration
    /// @return updated immutable options
    public ClusterStorageFoundation<T> setNodeConfig(final NodeConfig config) {
        return new ClusterStorageFoundation<>(this.rootSupplier, this.embeddedStorageFoundation,
                Objects.requireNonNull(config, "config"));
    }

    /// Starts the node and returns its Eclipse Store-compatible manager.
    ///
    /// @return started Store manager
    public ClusterStorageManager<T> start() {
        return this.startNode().storageManager();
    }

    /// Starts the node and returns its lifecycle and control handle.
    ///
    /// @return started node
    public ClusterNode<T> startNode() {
        if (this.rootSupplier == null) throw new NodeException("root supplier is required");
        return ClusterStorage.startNode(this.rootSupplier, this.embeddedStorageFoundation, this.nodeConfig);
    }
}
