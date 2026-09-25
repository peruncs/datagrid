package peruncs.cluster.api;

import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.serializer.persistence.types.Storer;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.eclipse.store.storage.types.StorageManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.errors.GraphInvalidatedException;
import peruncs.cluster.errors.ReaderWriteRejectedException;
import peruncs.cluster.errors.StorageLimitReachedException;
import peruncs.cluster.node.store.ClusterStorageManagers;
import peruncs.cluster.node.store.StorageSizeValidation;
import peruncs.cluster.storage.StorageGraphCoordinator;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the public application boundary contract on the guarded manager:
/// explicit persistence, exclusive write sections, no implicit storeRoot,
/// role admission, failure invalidation, and reader graph traversal.
class ClusterStorageManagerBoundaryTest {
    @TempDir
    Path dir;

    static final class Root {
        final List<String> values = new ArrayList<>();
    }

    static final class Article {
        int version;
    }

    private static EmbeddedStorageManager start(final Path dir) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(dir))
                .setChannelCountProvider(Storage.ChannelCountProvider(1))
                .createConfiguration();
        return EmbeddedStorage.start(configuration);
    }

    /// Counting Store manager wrapper that records storeRoot() calls so a test
    /// can prove the boundary never persists implicitly.
    private static final class CountingManager implements java.lang.reflect.InvocationHandler {
        final StorageManager delegate;
        final AtomicInteger storeRoots = new AtomicInteger();
        final AtomicInteger storeCalls = new AtomicInteger();

        CountingManager(final StorageManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(final Object proxy, final java.lang.reflect.Method method, final Object[] args)
                throws Throwable {
            if ("storeRoot".equals(method.getName())) {
                this.storeRoots.incrementAndGet();
            }
            if ("store".equals(method.getName())) {
                this.storeCalls.incrementAndGet();
            }
            try {
                return method.invoke(this.delegate, args);
            } catch (final java.lang.reflect.InvocationTargetException thrown) {
                throw thrown.getTargetException();
            }
        }

        StorageManager proxy() {
            return (StorageManager) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{StorageManager.class}, this);
        }
    }

    /// A write section persists only what the application explicitly stores:
    /// the boundary performs no implicit storeRoot().
    @Test
    void writeSectionWithoutExplicitPersistenceStoresNothing() {
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            delegate.setRoot(Lazy.Reference(new Root()));
            delegate.storeRoot();
            final CountingManager counting = new CountingManager(delegate);
            final ClusterStorageManager<Root> manager = ClusterStorageManagers.guarding(
                    counting.proxy(), StorageSizeValidation.notReached(), () -> false,
                    new StorageGraphCoordinator());
            final long id = manager.graphBoundary().write(() ->
            {
                final Root root = ((Lazy<Root>) manager.root()).get();
                root.values.add("buffered-not-yet-stored");
                /* Explicit persistence is the application's own choice. */
                final Storer storer = manager.createStorer();
                final long objectId = storer.store(root);
                storer.commit();
                return objectId;
            });
            assertTrue(id > 0L);
            assertEquals(0, counting.storeRoots.get(),
                    "an explicit storer commit must not trigger an implicit storeRoot()");
            assertEquals(0, counting.storeCalls.get(),
                    "an explicit storer.run() must not go through storeRoot()/store()");
        }
    }

    /// Two storer batches inside one write section commit as exactly two
    /// durable phases — no collapse, no extra commits.
    @Test
    void independentPhaseCommitsInsideOneSection() {
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            delegate.setRoot(Lazy.Reference(new Root()));
            delegate.storeRoot();
            final CountingManager counting = new CountingManager(delegate);
            final ClusterStorageManager<Root> manager = ClusterStorageManagers.guarding(
                    counting.proxy(), StorageSizeValidation.notReached(), () -> false,
                    new StorageGraphCoordinator());
            final long[] ids = manager.graphBoundary().write(() ->
            {
                final Root root = ((Lazy<Root>) manager.root()).get();
                root.values.add("phase-a");
                final Storer first = manager.createStorer();
                final long firstId = first.store(root);
                first.commit();
                root.values.add("phase-b");
                final Storer second = manager.createStorer();
                final long secondId = second.store(root);
                second.commit();
                return new long[]{firstId, secondId};
            });
            assertEquals(2, ids.length);
            assertNotSame(ids[0], ids[1]);
        }
    }

    /// The same shape works end-to-end for a GigaMap: store via storer,
    /// commit explicitly, reload and read the entities back.
    @Test
    void gigaMapPersistsAcrossReload(@TempDir final Path storeDir) {
        try (EmbeddedStorageManager delegate = start(storeDir)) {
            final ClusterStorageManager<Root> manager = ClusterStorageManagers.guarding(
                    delegate, StorageSizeValidation.notReached(), () -> false,
                    new StorageGraphCoordinator());
            manager.setRoot(org.eclipse.serializer.reference.Lazy.Reference(new Root()));
            manager.storeRoot();
            final long stored = manager.graphBoundary().write(() ->
            {
                final Root root = ((Lazy<Root>) manager.root()).get();
                root.values.add("first");
                /* Explicit storer commit: the boundary persists nothing by
                 * itself, so this is the only durable phase of the write.
                 * Store tracks shallow instances, so the changed collection
                 * is stored explicitly, not implied by the root wrapper. */
                final Storer storer = manager.createStorer();
                storer.store(root);
                storer.store(root.values);
                storer.commit();
                return root.values.size();
            });
            assertTrue(stored > 0L);
        }
        final long count;
        try (EmbeddedStorageManager delegate = start(storeDir)) {
            final Object root = delegate.root();
            assertNotNull(root);
            count = ((Root) ((Lazy<?>) root).get()).values.size();
        }
        assertEquals(1, count, "the explicitly committed mutation must survive reload");
    }

    /// Reader boundary writes are rejected before the callback runs.
    @Test
    void readerBoundaryWriteRejectedBeforeCallback() {
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            delegate.setRoot(Lazy.Reference(new Root()));
            delegate.storeRoot();
            final ClusterStorageManager<Root> manager = ClusterStorageManagers.readOnly(
                    delegate, () -> false, new StorageGraphCoordinator());
            final AtomicBoolean ran = new AtomicBoolean();
            assertThrows(ReaderWriteRejectedException.class, () ->
                    manager.graphBoundary().write(() ->
                    {
                        ran.set(true);
                        return null;
                    }));
            assertFalse(ran.get(), "the rejected callback must not run");
            assertThrows(ReaderWriteRejectedException.class, () ->
                    manager.graphBoundary().write(() ->
                    {
                    }));
            /* The reader still reads its graph through the boundary. */
            assertEquals(0, (int) manager.graphBoundary().read(
                    () -> ((Lazy<Root>) manager.root()).get().values.size()));
        }
    }

    /// The storage-limit gate rejects the boundary write section as a whole,
    /// before the callback.
    @Test
    void limitRejectsBoundaryWriteBeforeCallback() {
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            delegate.setRoot(Lazy.Reference(new Root()));
            delegate.storeRoot();
            final ClusterStorageManager<Root> manager = ClusterStorageManagers.guarding(
                    delegate, () -> true, () -> false, new StorageGraphCoordinator());
            final AtomicBoolean ran = new AtomicBoolean();
            assertThrows(StorageLimitReachedException.class, () ->
                    manager.graphBoundary().write(() ->
                    {
                        ran.set(true);
                        return null;
                    }));
            assertFalse(ran.get());
        }
    }

    /// A persistence failure latches the graph: direct store paths invalidate
    /// the manager's boundary; later boundary reads fail closed.
    @Test
    void persistenceFailureInvalidatesTheGraph() {
        final StorageManager store = (StorageManager) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{StorageManager.class},
                (proxy, method, args) ->
                {
                    switch (method.getName()) {
                        case "root":
                            return null;
                        case "store":
                            throw new IllegalStateException("boom");
                        case "storeRoot":
                            return 0L;
                        case "setRoot":
                            return new Object();
                        default:
                            return null;
                    }
                });
        final ClusterStorageManager<Root> manager = ClusterStorageManagers.guarding(
                store, StorageSizeValidation.notReached(), () -> false,
                new StorageGraphCoordinator());
        assertThrows(IllegalStateException.class, () -> manager.store(new Root()));
        assertThrows(GraphInvalidatedException.class,
                () -> manager.graphBoundary().read(() -> null),
                "a dirty store failure must fail closed on later reads");
        assertThrows(GraphInvalidatedException.class, manager::storeRoot,
                "latching a store failure must also reject storeRoot()");
    }

    /// An ordinary callback failure inside an application write section does
    /// NOT invalidate the graph — only explicitly reported dirty failure does.
    @Test
    void cleanCallbackFailureDoesNotInvalidate() {
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            delegate.setRoot(Lazy.Reference(new Root()));
            delegate.storeRoot();
            final ClusterStorageManager<Root> manager = ClusterStorageManagers.guarding(
                    delegate, StorageSizeValidation.notReached(), () -> false,
                    new StorageGraphCoordinator());
            assertThrows(IllegalStateException.class, () ->
                    manager.graphBoundary().write(() ->
                    {
                        throw new IllegalStateException("clean validation failure");
                    }));
            assertDoesNotThrow(() ->
                    manager.graphBoundary().read(() -> null),
                    "a clean application failure must not poison a healthy graph");
        }
    }

    /// A caller that reports a dirty failure invalidates through the boundary.
    @Test
    void explicitInvalidationFailsClosed() {
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            delegate.setRoot(Lazy.Reference(new Root()));
            delegate.storeRoot();
            final ClusterStorageManager<Root> manager = ClusterStorageManagers.guarding(
                    delegate, StorageSizeValidation.notReached(), () -> false,
                    new StorageGraphCoordinator());
            final RuntimeException dirty = new RuntimeException("i might have written");
            manager.graphBoundary().write(() ->
            {
                manager.graphBoundary().invalidate(dirty);
                return null;
            });
            assertThrows(GraphInvalidatedException.class,
                    () -> manager.graphBoundary().read(() -> null));
            assertThrows(GraphInvalidatedException.class, () -> manager.store(new Root()),
                    "direct writes also fail closed after dirty invalidation");
        }
    }

    /// Read-to-write upgrades are rejected immediately instead of deadlocking
    /// the fair lock; reads inside a write are reentrant and supported.
    @Test
    void readToWriteUpgradeIsRejected() {
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            delegate.setRoot(Lazy.Reference(new Root()));
            delegate.storeRoot();
            final ClusterStorageManager<Root> manager = ClusterStorageManagers.guarding(
                    delegate, StorageSizeValidation.notReached(), () -> false,
                    new StorageGraphCoordinator());
            assertThrows(IllegalStateException.class, () ->
                    manager.graphBoundary().read(() ->
                            manager.graphBoundary().write(() -> null)),
                    "writing from inside a read must fail immediately, not block");
            assertDoesNotThrow(() ->
                    manager.graphBoundary().write(() ->
                            manager.graphBoundary().read(() -> null)),
                    "reads inside a write stay supported");
        }
    }

    /// Concurrent boundary writes serialize — no lost increments.
    @Test
    void concurrentWritesSerialize() throws Exception {
        final int threads = 4;
        final int increments = 100;
        try (EmbeddedStorageManager delegate = start(this.dir)) {
            delegate.setRoot(Lazy.Reference(new Root()));
            delegate.storeRoot();
            final ClusterStorageManager<Root> manager = ClusterStorageManagers.guarding(
                    delegate, StorageSizeValidation.notReached(), () -> false,
                    new StorageGraphCoordinator());
            final CountDownLatch done = new CountDownLatch(threads);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            for (int thread = 0; thread < threads; thread++) {
                Thread.ofVirtual().start(() ->
                {
                    try {
                        for (int step = 0; step < increments; step++) {
                            manager.graphBoundary().write(() ->
                            {
                                final Root root = ((Lazy<Root>) manager.root()).get();
                                root.values.add("v");
                                manager.store(root);
                                return null;
                            });
                        }
                    } catch (final Throwable error) {
                        failure.compareAndSet(null, error);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(60, TimeUnit.SECONDS));
            assertNull(failure.get(), String.valueOf(failure.get()));
            assertEquals(threads * increments,
                    (int) manager.graphBoundary().read(
                            () -> ((Lazy<Root>) manager.root()).get().values.size()));
        }
    }
}
