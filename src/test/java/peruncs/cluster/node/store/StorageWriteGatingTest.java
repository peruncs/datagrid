package peruncs.cluster.node.store;

import org.eclipse.serializer.util.X;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.errors.ReaderWriteRejectedException;
import peruncs.cluster.errors.StorageLimitReachedException;
import peruncs.cluster.api.ClusterStorageManager;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/// The storage limit gates only the write entry points.
///
/// Reads, maintenance, registration, and restore must keep working on a full
/// disk so the node can drain, back up, or recover instead of failing every
/// operation.
class StorageWriteGatingTest {
    /// Verifies core write entry points are rejected once the storage limit is reached.
    @Test
    void writesAreRejectedWhenLimitReached(@TempDir final Path dir) {
        try (EmbeddedStorageManager delegate = start(dir)) {
            final ClusterStorageManager<Object> manager =
                    ClusterStorageManagers.guarding(delegate, () -> true, () -> false, new peruncs.cluster.storage.StorageGraphCoordinator());

            assertThrows(StorageLimitReachedException.class, () -> manager.store(new Payload("a")));
            assertThrows(StorageLimitReachedException.class, () -> manager.storeAll(new Payload("b")));
            assertThrows(StorageLimitReachedException.class, manager::storeRoot);
            assertThrows(StorageLimitReachedException.class, () -> manager.createStorer().commit());
        }
    }

    /// Verifies every fluent storer and raw-target write path is gated once the storage limit is reached.
    @Test
    void everyFluentStorerPathIsGatedWhenLimitReached(@TempDir final Path dir) {
        try (EmbeddedStorageManager delegate = start(dir)) {
            final ClusterStorageManager<Object> manager =
                    ClusterStorageManagers.guarding(delegate, () -> true, () -> false, new peruncs.cluster.storage.StorageGraphCoordinator());

            assertThrows(StorageLimitReachedException.class, () -> {
                final var storer = manager.createStorer().reinitialize();
                storer.store(new Payload("a"));
                storer.commit();
            });
            assertThrows(StorageLimitReachedException.class, () -> {
                final var storer = manager.createStorer().reinitialize(16L);
                storer.store(new Payload("b"));
                storer.commit();
            });
            assertThrows(StorageLimitReachedException.class, () -> {
                final var storer = manager.createStorer().ensureCapacity(16L);
                storer.store(new Payload("c"));
                storer.commit();
            });
            assertThrows(StorageLimitReachedException.class, () -> {
                final var storer = manager.createEagerStorer().reinitialize();
                storer.store(new Payload("d"));
                storer.commit();
            });
            assertThrows(StorageLimitReachedException.class, () -> {
                final var storer = manager.createLazyStorer().ensureCapacity(16L);
                storer.store(new Payload("e"));
                storer.commit();
            });
            assertThrows(StorageLimitReachedException.class,
                    () -> manager.persistenceManager().createStorer().reinitialize().commit());
            assertThrows(StorageLimitReachedException.class,
                    () -> manager.persistenceManager().createEagerStorer().commit());
            assertThrows(StorageLimitReachedException.class,
                    () -> manager.persistenceManager().createLazyStorer().commit());
            assertThrows(StorageLimitReachedException.class,
                    () -> manager.persistenceManager().target().write(null));
            assertThrows(StorageLimitReachedException.class, () -> manager.setRoot(new Payload("f")));
        }
    }

    /// Verifies a read-only manager rejects every mutation API while keeping reads, registration, and type-dictionary access working.
    @Test
    void readOnlyManagerRejectsEveryMutationApi(@TempDir final Path dir) {
        try (EmbeddedStorageManager delegate = start(dir)) {
            delegate.setRoot(org.eclipse.serializer.reference.Lazy.Reference(new Payload("root")));
            delegate.storeRoot();
            final ClusterStorageManager<Object> manager =
                    ClusterStorageManagers.readOnly(delegate, () -> false, new peruncs.cluster.storage.StorageGraphCoordinator());

            assertThrows(ReaderWriteRejectedException.class, () -> manager.store(new Payload("a")));
            assertThrows(ReaderWriteRejectedException.class, () -> manager.storeAll(new Payload("c")));
            assertThrows(ReaderWriteRejectedException.class, () -> manager.storeAll(java.util.List.of(new Payload("d"))));
            assertThrows(ReaderWriteRejectedException.class, manager::storeRoot);
            assertThrows(ReaderWriteRejectedException.class, () -> manager.setRoot(new Payload("e")));
            assertThrows(ReaderWriteRejectedException.class, () -> manager.createStorer().commit());
            assertThrows(ReaderWriteRejectedException.class, () -> {
                final var storer = manager.createStorer().reinitialize();
                storer.store(new Payload("f"));
                storer.commit();
            });
            assertThrows(ReaderWriteRejectedException.class,
                    () -> manager.createEagerStorer().ensureCapacity(16L).commit());
            assertThrows(ReaderWriteRejectedException.class, () -> manager.createLazyStorer().commit());
            assertThrows(ReaderWriteRejectedException.class,
                    () -> manager.persistenceManager().createStorer().commit());
            assertThrows(ReaderWriteRejectedException.class,
                    () -> manager.persistenceManager().target().write(null));
            assertThrows(ReaderWriteRejectedException.class,
                    () -> manager.persistenceManager().updateCurrentObjectId(10L));
            assertThrows(ReaderWriteRejectedException.class,
                    () -> manager.persistenceManager().updateMetadata(null, 0L, 0L));
            assertTrue(manager.persistenceManager().target().isWritable(),
                    "read-only raw target must report writable so the Store routes writes into the rejecting write() instead of skipping them silently");

            assertThrows(ReaderWriteRejectedException.class, () -> manager.importData(X.Enum()));
            assertThrows(ReaderWriteRejectedException.class, () -> manager.importFiles(X.Enum()));
            /* D2: readers do get their live lazy root and roots view — the
             * traversal discipline lives in the graph boundary, not in
             * root access denial. Graph-boundary writes stay rejected. */
            assertDoesNotThrow(manager::root);
            assertDoesNotThrow(manager::viewRoots);
            assertDoesNotThrow(() -> manager.persistenceManager().viewRoots());
            assertNotNull(manager.graphBoundary());
            assertThrows(peruncs.cluster.errors.ReaderWriteRejectedException.class,
                    () -> manager.graphBoundary().write(() -> {
                    }), "reader boundary writes are rejected before the callback");
            assertThrows(peruncs.cluster.errors.ReaderWriteRejectedException.class,
                    () -> manager.graphBoundary().write(() -> "x"),
                    "reader boundary writes are rejected before the callback");
            assertDoesNotThrow(manager::typeDictionary);
            assertDoesNotThrow(() -> manager.persistenceManager().ensureObjectId(new Payload("g")));
            assertTrue(manager.isRunning(), "read-only manager must stay usable for reads");
        }
    }

    /// Verifies maintenance and object-id registration still work when the storage limit is reached.
    @Test
    void maintenanceAndRegistrationWorkWhenLimitReached(@TempDir final Path dir) {
        try (EmbeddedStorageManager delegate = start(dir)) {
            final ClusterStorageManager<Object> manager =
                    ClusterStorageManagers.guarding(delegate, () -> true, () -> false, new peruncs.cluster.storage.StorageGraphCoordinator());

            assertThrows(UnsupportedOperationException.class, () -> manager.importData(X.Enum()));
            assertDoesNotThrow(() -> manager.persistenceManager().ensureObjectId(new Payload("c")));
            assertDoesNotThrow(() -> manager.persistenceManager().consolidate());
        }
    }

    /// Closing the borrowed persistence view must not close the live Store.
    @Test
    void persistenceManagerViewDoesNotOwnTheStore(@TempDir final Path dir) {
        try (EmbeddedStorageManager delegate = start(dir)) {
            final ClusterStorageManager<Object> manager =
                    ClusterStorageManagers.guarding(delegate, () -> false, () -> false, new peruncs.cluster.storage.StorageGraphCoordinator());

            manager.persistenceManager().close();
            /* The borrowed target view is non-owning one level down as well:
             * closing it through the persistence manager must not release
             * the shared live target. */
            manager.persistenceManager().target().closeTarget();

            assertTrue(manager.isRunning());
            assertDoesNotThrow(() -> manager.store(new Payload("still-open")));
        }
    }

    private static EmbeddedStorageManager start(final Path dir) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(dir))
                .setChannelCountProvider(Storage.ChannelCountProvider(1))
                .createConfiguration();
        return EmbeddedStorage.start(configuration);
    }

    static final class Payload {
        @SuppressWarnings("FieldCanBeLocal") // retained as a stored field for Eclipse Store serialization
        private final String value;

        Payload(final String value) {
            this.value = value;
        }
    }
}
