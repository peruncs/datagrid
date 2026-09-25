package peruncs.cluster.node.store;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.types.StorageManager;
import peruncs.cluster.errors.ReaderWriteRejectedException;
import peruncs.cluster.storage.StorageGraphCoordinator;

/// Read-only facade for reader roles: it rejects durable application writes
/// while keeping reads, maintenance, and restore working.
///
/// Replication uses the unwrapped Store connection owned by the node;
/// exposing import through this application-facing view would let a reader
/// manufacture an unreplicated local image.
///
/// The write gate always throws [ReaderWriteRejectedException], which flows
/// through the inherited `store`, `storeAll`, `storeRoot`, `setRoot`, storer
/// `commit`, raw-target `write`, and import paths, and through the
/// application boundary's write sections.
///
/// Roots are readable on readers too: `root()` returns the live `Lazy`
/// reference and `viewRoots()` delegates — readers must run the traversal
/// inside `graphBoundary().read(...)`, never outside it.
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
                           final NodeClose nodeClose,
                           final StorageGraphCoordinator graphCoordinator) {
        super(delegate, StorageSizeValidation.notReached(), nodeClose, graphCoordinator);
    }

    @Override
    void validateState() {
        /* The shared lifecycle and graph-validity checks still run; reader
         * rejection is layered on top and must not bypass them. */
        this.ensureGraphValid();
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
}
