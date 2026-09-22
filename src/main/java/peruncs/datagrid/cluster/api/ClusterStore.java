package peruncs.datagrid.cluster.api;

import peruncs.datagrid.cluster.node.store.ClusterStorageManager;

import java.util.Objects;
import java.util.function.Function;

/// Guarded Store access owned by a [ClusterNode].
///
/// Reader graph traversal must run inside [#read(Function)]. Writer persistence
/// is intentionally limited to the ordinary Store operations needed by an
/// application; raw imports, persistence targets, graph locks, and lifecycle
/// controls remain internal.
///
/// @param <T> root type
public final class ClusterStore<T> {
    private final ClusterStorageManager<T> storage;

    ClusterStore(final ClusterStorageManager<T> storage) {
        this.storage = Objects.requireNonNull(storage, "storage");
    }

    /// Reads the materialized root under the replication graph boundary.
    ///
    /// The callback and all traversal of live Store objects must finish before
    /// this method returns. Do not retain the root or another live graph object
    /// in the returned value; copy the data needed outside the boundary.
    ///
    /// @param <R> query result type
    /// @param query query evaluated inside the read boundary
    /// @return the query result
    public <R> R read(final Function<? super T, ? extends R> query) {
        return this.storage.readRoot(Objects.requireNonNull(query, "query"));
    }

    /// Persists one changed object on the writer; reader roles reject the call.
    ///
    /// A fenced writer or unavailable replication service rejects the write
    /// before local acceptance. An uncertain-commit failure is not safe to
    /// retry blindly: inspect node status and reconcile or reseed first.
    /// Capacity failures are retryable after Archive space is restored.
    ///
    /// @param value changed object to publish
    /// @return the publication sequence
    public long store(final Object value) {
        return this.storage.store(Objects.requireNonNull(value, "value"));
    }

    /// Persists changed objects on the writer; reader roles reject the call.
    ///
    /// The same fencing, capacity, and uncertain-commit rules as [#store]
    /// apply to the complete batch.
    ///
    /// @param values changed objects to publish
    /// @return publication sequences in input order
    public long[] storeAll(final Object... values) {
        Objects.requireNonNull(values, "values");
        return this.storage.storeAll(values);
    }

    /// Persists the current root on the writer; reader roles reject the call.
    ///
    /// The same fencing, capacity, and uncertain-commit rules as [#store]
    /// apply.
    ///
    /// @return the publication sequence
    public long storeRoot() {
        return this.storage.storeRoot();
    }
}
