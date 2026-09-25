package peruncs.cluster.api;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;

import java.util.Objects;
import java.util.function.Supplier;

/// Immutable application-owned inputs for opening one cluster node.
///
/// Transport, role, paths, security, retention, and operational limits are
/// read from the node settings source — environment-backed by default —
/// and validated before startup.
///
/// @param rootSupplier              creates a root for an empty Store
/// @param embeddedStorageFoundation optional Store foundation with custom
///                                  tuning, type handlers, or backup setup;
///                                  the node always derives the live file
///                                  provider from the configured storage path
/// @param nodeSettingsSource        optional programmatic settings source,
///                                  replacing environment variables
/// @param <T>                       root type
public record NodeOptions<T>(
        Supplier<? extends T> rootSupplier,
        EmbeddedStorageFoundation<?> embeddedStorageFoundation,
        NodeSettingsSource nodeSettingsSource) {

    /// Validates the immutable node options.
    public NodeOptions {
        Objects.requireNonNull(rootSupplier, "rootSupplier");
    }

    /// Creates options using the default Store foundation and environment settings.
    ///
    /// @param <T>          root type
    /// @param rootSupplier creates a root for an empty Store
    /// @return immutable node options
    public static <T> NodeOptions<T> of(final Supplier<? extends T> rootSupplier) {
        return new NodeOptions<>(rootSupplier, null, null);
    }

    /// Returns these options with a custom Store foundation.
    ///
    /// A supplied foundation belongs to one node startup: do not share a
    /// mutable foundation across concurrent nodes. The node replaces the live
    /// file provider with its configured storage path; the remaining Store
    /// tuning (channels, chunk checksumming, evaluators, type handlers,
    /// backup setup) is preserved.
    ///
    /// @param foundation Store foundation for node startup
    /// @return new options with the foundation set
    public NodeOptions<T> withEmbeddedStorageFoundation(final EmbeddedStorageFoundation<?> foundation) {
        return new NodeOptions<>(this.rootSupplier, foundation, this.nodeSettingsSource);
    }

    /// Returns these options with a programmatic settings source.
    ///
    /// @param settings programmatic node settings
    /// @return new options with the settings source set
    public NodeOptions<T> withNodeSettingsSource(final NodeSettingsSource settings) {
        return new NodeOptions<>(this.rootSupplier, this.embeddedStorageFoundation, settings);
    }
}
