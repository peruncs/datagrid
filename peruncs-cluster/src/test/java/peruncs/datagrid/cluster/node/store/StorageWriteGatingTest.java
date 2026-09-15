package peruncs.datagrid.cluster.node.store;

import org.eclipse.serializer.util.X;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.node.exceptions.StorageLimitReachedException;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// The storage limit gates only the write entry points.
///
/// Reads, maintenance, registration, and restore must keep working on a full
/// disk so the node can drain, back up, or recover instead of failing every
/// operation.
class StorageWriteGatingTest {
    @Test
    void writesAreRejectedWhenLimitReached(@TempDir final Path dir) {
        try (EmbeddedStorageManager delegate = start(dir)) {
            final ClusterStorageManager<Object> manager =
                    ClusterStorageManager.New(delegate, () -> true, ClusterStorageManager.ShutdownCallback.NoOp());

            assertThrows(StorageLimitReachedException.class, () -> manager.store(new Payload("a")));
            assertThrows(StorageLimitReachedException.class, () -> manager.storeAll(new Payload("b")));
            assertThrows(StorageLimitReachedException.class, manager::storeRoot);
            assertThrows(StorageLimitReachedException.class, () -> manager.createStorer().commit());
        }
    }

    @Test
    void restoreAndRegistrationWorkWhenLimitReached(@TempDir final Path dir) {
        try (EmbeddedStorageManager delegate = start(dir)) {
            final ClusterStorageManager<Object> manager =
                    ClusterStorageManager.New(delegate, () -> true, ClusterStorageManager.ShutdownCallback.NoOp());

            assertDoesNotThrow(() -> manager.importData(X.Enum()));
            assertDoesNotThrow(() -> manager.persistenceManager().ensureObjectId(new Payload("c")));
            assertDoesNotThrow(() -> manager.persistenceManager().consolidate());
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
        private final String value;

        Payload(final String value) {
            this.value = value;
        }
    }
}
