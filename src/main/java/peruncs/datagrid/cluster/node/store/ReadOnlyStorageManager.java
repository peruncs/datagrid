package peruncs.datagrid.cluster.node.store;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceRootsView;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.storage.types.StorageManager;
import peruncs.datagrid.cluster.errors.ReaderWriteRejectedException;
import peruncs.datagrid.cluster.storage.StorageGraphCoordinator;

/// Read-only facade for reader roles: it rejects application writes while
/// keeping reads, maintenance, and restore working.
///
/// Replication uses the unwrapped Store connection owned by the node;
/// exposing import through this application-facing view would let a reader
/// manufacture an unreplicated local image.
///
/// The write gate always throws [ReaderWriteRejectedException], which flows
/// through the inherited `store`, `storeAll`, `storeRoot`, `setRoot`, storer
/// `commit`, raw-target `write`, and import paths.
///
/// Object-ID assignment (`createRegisterer`, `ensureObjectId*`,
/// `lookupObjectId`) is deliberately ungated: it only hands out in-memory
/// identifiers from the local registry and persists nothing by itself. A
/// divergent Store image can only be persisted through a gated write entry
/// point, so ID assignment stays read-safe while every durable mutation is
/// rejected.
///
/// @param <T> root type
final class ReadOnlyStorageManager<T> extends GuardingStorageManager<T> {
    ReadOnlyStorageManager(final StorageManager delegate,
                           final ClusterStorageManager.ShutdownCallback shutdownCallback,
                           final StorageGraphCoordinator graphCoordinator) {
        super(delegate, StorageSizeValidation.notReached(), shutdownCallback, graphCoordinator);
    }

    @Override
    void validateState() {
        throw new ReaderWriteRejectedException(
                "node role is read-only; application writes are rejected because they would diverge from the writer");
    }

    @Override
    void rejectApplicationImport() {
        throw new ReaderWriteRejectedException(
                "node role is read-only; application imports are reserved for the node-owned paths");
    }

    @Override
    PersistenceTarget<Binary> gateTarget(final PersistenceTarget<Binary> raw) {
        return RejectingPersistenceTarget.create(raw);
    }

    @Override
    public Lazy<T> root() {
        /* The merger applies batches on the coordinator write side; a live
         * reference returned here would be traversed after the read lock is
         * gone. Application readers must use readRoot(...) or
         * graphCoordinator().read(...). */
        throw new UnsupportedOperationException(
                "use readRoot(...) for a coherent graph read; root() cannot retain the coordinator read lock");
    }

    @Override
    public PersistenceRootsView viewRoots() {
        throw new UnsupportedOperationException(
                "use readRoot(...) for a coherent graph read; viewRoots() exposes live roots");
    }
}
