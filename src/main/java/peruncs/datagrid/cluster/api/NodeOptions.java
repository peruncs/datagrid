package peruncs.datagrid.cluster.api;

import java.util.Objects;
import java.util.function.Supplier;

/// Immutable application-owned inputs for opening one cluster node.
///
/// Transport, role, paths, security, retention, and operational limits are
/// read from the node environment and validated before startup.
///
/// @param rootSupplier creates a root for an empty Store
/// @param asynchronousDistribution whether Store publication may be asynchronous
/// @param <T> root type
public record NodeOptions<T>(
        Supplier<? extends T> rootSupplier,
        boolean asynchronousDistribution
) {
    public NodeOptions {
        Objects.requireNonNull(rootSupplier, "rootSupplier");
    }

    /// Creates options using the default Store foundation and synchronous publication.
    public static <T> NodeOptions<T> of(final Supplier<? extends T> rootSupplier) {
        return new NodeOptions<>(rootSupplier, false);
    }
}
