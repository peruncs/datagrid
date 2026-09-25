package peruncs.cluster.api;

import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.errors.GraphInvalidatedException;
import peruncs.cluster.errors.ReaderWriteRejectedException;
import peruncs.cluster.errors.StorageLimitReachedException;
import peruncs.cluster.node.store.ClusterStorageManager;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the atomic writer mutation boundary [ClusterStore#withRootWrite]:
/// mutation and persistence hold exclusive graph ownership, concurrent writers
/// cannot lose each other's updates, read closures stay read-only contracts,
/// and a failed mutation invalidates the writer's mutable view until recovery.
class ClusterStoreWriteBoundaryTest {
    @TempDir
    Path dir;

    static final class Root {
        long count;
    }

    private static EmbeddedStorageManager start(final Path dir) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(dir))
                .setChannelCountProvider(Storage.ChannelCountProvider(1))
                .createConfiguration();
        return EmbeddedStorage.start(new Root(), configuration);
    }

    private static EmbeddedStorageManager reopen(final Path dir) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(dir))
                .setChannelCountProvider(Storage.ChannelCountProvider(1))
                .createConfiguration();
        return EmbeddedStorage.start(configuration);
    }

    private static ClusterStorageManager<Root> manager(final EmbeddedStorageManager delegate) {
        return ClusterStorageManager.create(delegate, () -> false, ClusterStorageManager.ShutdownCallback.noOp());
    }

    /// A write closure mutates and persists the root as one boundary: the
    /// reader view observes the committed state, and a restart reloads it.
    @Test
    void writeRootMutatesAndPersists() {
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            final ClusterStore<Root> store = new ClusterStore<>(manager(delegate));
            final long count = store.withRootWrite(root -> {
                root.count++;
                return root.count;
            });
            assertEquals(1, count);
            assertEquals(1, (long) store.withRootRead(root -> root.count));
        }
        try (EmbeddedStorageManager delegate = reopen(this.dir)) {
            assertEquals(1, ((Root) delegate.root()).count,
                    "the mutation must be durable after reopening the Store");
        }
    }

    /// Two racing writer threads must not lose updates: the exclusive boundary
    /// serializes mutation through persistence.
    @Test
    void concurrentWriteRootMutationsDoNotLoseUpdates() throws Exception {
        final int threads = 4;
        final int increments = 100;
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            final ClusterStore<Root> store = new ClusterStore<>(manager(delegate));
            final CountDownLatch ready = new CountDownLatch(threads);
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(threads);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            for (int thread = 0; thread < threads; thread++) {
                Thread.ofVirtual().start(() ->
                {
                    ready.countDown();
                    try {
                        assertTrue(start.await(10, TimeUnit.SECONDS));
                        for (int step = 0; step < increments; step++) {
                            store.withRootWrite(root ->
                            {
                                root.count++;
                                return root.count;
                            });
                        }
                    } catch (final Throwable error) {
                        failure.compareAndSet(null, error);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            assertTrue(done.await(60, TimeUnit.SECONDS), "writers did not finish");
            assertNull(failure.get(), failure.get() == null ? null : failure.get().toString());
            assertEquals((long) threads * increments, (long) store.withRootRead(root -> root.count),
                    "concurrent writeRoot calls must not lose updates");
        }
    }

    /// A throwing mutation invalidates the writer's mutable view: reads and
    /// further writes through the coordinator fail closed until recovery.
    @Test
    void failedWriteRootInvalidatesTheMutableView() {
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            final ClusterStore<Root> store = new ClusterStore<>(manager(delegate));
            final IllegalStateException boom = new IllegalStateException("mutation exploded");
            final IllegalStateException thrown = assertThrows(IllegalStateException.class, () ->
                    store.withRootWrite(root -> {
                        throw boom;
                    }));
            assertSame(boom, thrown, "the mutation failure must propagate");
            assertThrows(GraphInvalidatedException.class, () -> store.withRootRead(root -> root.count),
                    "reads must fail closed after a potentially half-applied mutation");
            assertThrows(GraphInvalidatedException.class,
                    () -> store.withRootWrite(root -> root.count),
                    "further mutations must fail closed until the node reloads or reseeds");
        }
    }

    /// The storage-limit gate rejects before the exclusive section, so a
    /// rejected write does not invalidate a healthy graph.
    @Test
    void storageLimitRejectionDoesNotInvalidate() {
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            final ClusterStorageManager<Root> manager =
                    ClusterStorageManager.create(delegate, () -> true, ClusterStorageManager.ShutdownCallback.noOp());
            final ClusterStore<Root> store = new ClusterStore<>(manager);
            assertThrows(StorageLimitReachedException.class, () -> store.withRootWrite(root -> root.count));
            assertDoesNotThrow(() -> store.withRootRead(root -> root.count),
                    "a limit rejection is not graph corruption");
        }
    }

    /// Reader roles reject the mutation boundary outright.
    @Test
    void readerRoleRejectsWriteRoot() {
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            final ClusterStorageManager<Root> manager =
                    ClusterStorageManager.ReadOnly(delegate, ClusterStorageManager.ShutdownCallback.noOp());
            final ClusterStore<Root> store = new ClusterStore<>(manager);
            assertThrows(ReaderWriteRejectedException.class,
                    () -> store.withRootWrite(root -> root.count));
            assertEquals(0, (long) store.withRootRead(root -> root.count));
        }
    }
}
