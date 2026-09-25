package peruncs.cluster.api;

import peruncs.cluster.errors.ReaderWriteRejectedException;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.errors.StorageLimitReachedException;
import peruncs.cluster.errors.WriterFencedException;
import peruncs.cluster.node.store.ClusterStorageManager;

import java.util.Objects;
import java.util.function.Function;

/// Guarded Store access owned by a [ClusterNode].
///
/// Reader graph traversal must run inside [#withRootRead(Function)]. Writer persistence
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
    /// The query callback runs while the node holds the Store graph read
    /// boundary. The callback — and all traversal of live graph objects it
    /// reaches — must finish before this method returns: returned or retained
    /// live graph objects escape the boundary and may race replication or
    /// read a graph that is being mutated. Copy whatever data the caller
    /// needs into plain values or records before returning. The name states
    /// the discipline: the root is borrowed for the callback only.
    ///
    /// @param <R> query result type
    /// @param query query evaluated inside the read boundary
    /// @return the query result
    public <R> R withRootRead(final Function<? super T, ? extends R> query) {
        return this.storage.readRoot(Objects.requireNonNull(query, "query"));
    }

    /// Persists one changed object on the writer; reader roles reject the call.
    ///
    /// A reader node throws [ReaderWriteRejectedException]
    /// for every write. A writer that lost its fencing lease throws
    /// [WriterFencedException]; only the lease
    /// restart procedure may clear that state. A node at its configured
    /// storage limit throws
    /// [StorageLimitReachedException], which
    /// is retryable after Archive or Store space is restored.
    ///
    /// An uncertain commit — a failure reported after the transaction may
    /// already be durably recorded — is NOT safe to retry blindly: the retry
    /// could duplicate it. Inspect [#status()][ClusterNode#status()] and the
    /// durable writer sequence first; reconcile or reseed when the boundary is
    /// unclear. Only failures raised before local acceptance are safe to
    /// retry immediately.
    ///
    /// @param value changed object to publish
    /// @return the publication sequence
    /// @throws ReaderWriteRejectedException on
    /// a reader or backup-reader node
    /// @throws WriterFencedException when this
    /// node no longer holds the writer lease
    /// @throws StorageLimitReachedException
    /// when the configured storage limit is reached
    /// @throws ReplicationUnavailableException
    /// when replication is unavailable — a failed MediaDriver, exhausted
    /// Archive capacity, or an offer deadline — or the node is closing
    public long store(final Object value) {
        return this.storage.store(Objects.requireNonNull(value, "value"));
    }

    /// Persists changed objects on the writer; reader roles reject the call.
    ///
    /// The same reader-rejection, fencing, capacity, availability, and
    /// uncertain-commit rules as [#store(Object)] apply to the complete batch.
    ///
    /// @param values changed objects to publish
    /// @return publication sequences in input order
    public long[] storeAll(final Object... values) {
        Objects.requireNonNull(values, "values");
        return this.storage.storeAll(values);
    }

    /// Persists the current root on the writer; reader roles reject the call.
    ///
    /// The same reader-rejection, fencing, capacity, availability, and
    /// uncertain-commit rules as [#store(Object)] apply.
    ///
    /// @return the publication sequence
    public long storeRoot() {
        return this.storage.storeRoot();
    }
}
