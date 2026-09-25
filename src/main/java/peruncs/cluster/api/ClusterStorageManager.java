package peruncs.cluster.api;

import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.storage.types.StorageManager;

/// Guarded Store facade for application access on a cluster node.
///
/// An application-facing `StorageManager` with explicit cluster restrictions.
/// The ordinary `PersistenceStoring`/`Persister` surface, storers, and the
/// exported persistence-manager adapter are available as usual; role, root,
/// coordination, and lifecycle rules still apply:
///
/// - reader and backup-reader nodes reject every durable application write
///   with [peruncs.cluster.errors.ReaderWriteRejectedException], so a reader
///   can never persist an unreplicated local divergence;
/// - a writer at its configured storage limit rejects writes with
///   [peruncs.cluster.errors.StorageLimitReachedException] until space is
///   restored;
/// - application imports are reserved for node-owned replication and
///   bootstrap paths and throw `UnsupportedOperationException`;
/// - a persistence failure with an uncertain durable outcome latches the
///   graph: later coordinated sections fail with
///   [peruncs.cluster.errors.GraphInvalidatedException] until the node
///   reloads or reseeds.
///
/// # Root and coordination
///
/// The root is always a [org.eclipse.serializer.reference.Lazy] reference;
/// materialize it with `root().get()`. On readers, the whole traversal must
/// run inside [GraphBoundary#read]; a writer's mutation and its explicit
/// persistence belong together inside [GraphBoundary#write]. The boundary
/// persists nothing by itself.
///
/// # Lifecycle
///
/// [#shutdown()] performs the complete node teardown (replication, backups,
/// Store) in the owning node's order and is idempotent; closing the owning
/// [ClusterNode] is equivalent.
///
/// @param <T> root type
public interface ClusterStorageManager<T> extends StorageManager {
    /// Returns the live root reference of this node's graph.
    ///
    /// The root is registered as a `Lazy<T>`; materialize it with
    /// `root().get()`. On reader nodes the whole traversal must run inside
    /// [GraphBoundary#read] — see [#graphBoundary()].
    ///
    /// @return live lazy root reference
    @Override
    Lazy<T> root();

    /// Starts the manager.
    ///
    /// The manager handed out by [ClusterNode#open] is already started, so
    /// this is an idempotent check-and-return: on a closed, closing, or
    /// invalidated node it fails instead of resurrecting its Store.
    ///
    /// @return this manager
    @Override
    ClusterStorageManager<T> start();

    /// Returns the graph coordination boundary shared with replication.
    ///
    /// @return boundary for coordinated reads and mutation sections
    GraphBoundary graphBoundary();
}
