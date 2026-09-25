package peruncs.cluster.storage.binary;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceManager;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.storage.StorageGraphCoordinator;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/// Proves the merger's batch section holds the coordinator's write side.
///
/// View retirement, materialization, validation, and index refresh run as one
/// write section, so joined application reads observe either the pre-batch or
/// the post-batch boundary — never a materialized graph with stale search
/// views. The section is pinned mid-flight by intercepting `viewRoots`, so
/// both exclusion properties are observed deterministically instead of
/// inferred from timing. (Only the type-dictionary conflict scan still joins
/// the read side; see [StorageGraphCoordinator].)
class StorageBinaryDataMergerReadSideTest {
    private static final long TIMEOUT_MS = 10_000L;

    /// Verifies the merger batch section holds the write side so joined reads and writes wait until validation releases.
    @Test
    void batchSectionExcludesReadsAndWrites(@TempDir final Path readerRoot) throws Exception {
        final CountDownLatch validationEntered = new CountDownLatch(1);
        final CountDownLatch releaseValidation = new CountDownLatch(1);
        try (EmbeddedStorageManager reader = EmbeddedStorage.start(new Root(), readerRoot)) {
            final StorageConnection realConnection = reader.createConnection();
            final PersistenceManager<?> realManager = realConnection.persistenceManager();
            final PersistenceManager<?> managers = (PersistenceManager<?>) Proxy.newProxyInstance(
                    PersistenceManager.class.getClassLoader(),
                    new Class<?>[]{PersistenceManager.class},
                    (proxy, method, args) ->
                    {
                        if (method.getName().equals("viewRoots")) {
                            validationEntered.countDown();
                            try {
                                releaseValidation.await(30, TimeUnit.SECONDS);
                            } catch (final InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        return method.invoke(realManager, args);
                    });
            final StorageConnection connection = (StorageConnection) Proxy.newProxyInstance(
                    StorageConnection.class.getClassLoader(),
                    new Class<?>[]{StorageConnection.class},
                    (proxy, method, args) ->
                    {
                        if (method.getName().equals("persistenceManager")) return managers;
                        /* The batch carries no entities; only the validation
                         * scan matters here, so the import itself is a no-op. */
                        if (method.getName().equals("importData")) return null;
                        return method.invoke(realConnection, args);
                    });

            final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
            final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(StorageBinaryDataMergerTestSupport.configuration(StorageBinaryDataMergerTestSupport.foundation(), connection, ObjectGraphUpdateHandler.PerStore(coordinator), 0L, 1_000_000L, 60_000L, coordinator));
            try {
                assertSame(coordinator, merger.graphCoordinator(),
                        "the merger must expose the coordinator node read paths join through");

                final Binary empty = ChunksWrapper.New(ByteBuffer.allocateDirect(8));
                assertTrue(merger.receiveDataOwned(empty), "the empty batch must be accepted");
                assertTrue(validationEntered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS),
                        "the worker never reached the validation scan");

                /* A joined read stays out while the batch section holds the
                 * write side: overlapping it could observe a retired view
                 * over not-yet-materialized entities. */
                final AtomicBoolean readRan = new AtomicBoolean();
                final Thread reading = Thread.ofVirtual().start(
                        () -> coordinator.read(() -> readRan.set(true)));
                reading.join(500L);
                assertTrue(reading.isAlive(),
                        "a read overlapped the merger batch section");
                assertFalse(readRan.get());

                /* A write stays out while the merger's scan holds the write side. */
                final AtomicBoolean writeRan = new AtomicBoolean();
                final Thread writing = Thread.ofVirtual().start(
                        () -> coordinator.write(() -> writeRan.set(true)));
                writing.join(500L);
                assertTrue(writing.isAlive(),
                        "a write entered while the merger batch section held the write side");
                assertFalse(writeRan.get());

                releaseValidation.countDown();
                merger.awaitApplied();
                reading.join(TIMEOUT_MS);
                assertFalse(reading.isAlive(), "the read never finished after the scan released");
                assertTrue(readRan.get(), "the read never ran after the scan released");
                writing.join(TIMEOUT_MS);
                assertFalse(writing.isAlive(), "the write never finished after the scan released");
                assertTrue(writeRan.get(), "the write never ran after the scan released");
                assertNull(merger.failure(), "the coordinated batch must not fail the merger");
            } finally {
                releaseValidation.countDown();
                merger.dispose();
            }
        }
    }

    /// Verifies a batch that fails inside its write section invalidates the
    /// graph before the write lock releases: coordinated reads and later
    /// writes fail closed until reload/reseed instead of serving a
    /// potentially half-materialized graph.
    @Test
    void failedBatchLeavesTheGraphFailClosed(@TempDir final Path readerRoot) throws Exception {
        try (EmbeddedStorageManager reader = EmbeddedStorage.start(new Root(), readerRoot)) {
            final StorageConnection realConnection = reader.createConnection();
            final PersistenceManager<?> realManager = realConnection.persistenceManager();
            final IllegalStateException scanBoom = new IllegalStateException("index scan blew up mid-batch");
            final PersistenceManager<?> managers = (PersistenceManager<?>) Proxy.newProxyInstance(
                    PersistenceManager.class.getClassLoader(),
                    new Class<?>[]{PersistenceManager.class},
                    (proxy, method, args) ->
                    {
                        if (method.getName().equals("viewRoots")) {
                            throw scanBoom;
                        }
                        return method.invoke(realManager, args);
                    });
            final StorageConnection connection = (StorageConnection) Proxy.newProxyInstance(
                    StorageConnection.class.getClassLoader(),
                    new Class<?>[]{StorageConnection.class},
                    (proxy, method, args) ->
                    {
                        if (method.getName().equals("persistenceManager")) return managers;
                        /* The batch carries no entities; only the failing
                         * validation section matters here, so the import
                         * itself is a no-op. */
                        if (method.getName().equals("importData")) return null;
                        return method.invoke(realConnection, args);
                    });

            final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
            final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(StorageBinaryDataMergerTestSupport.configuration(StorageBinaryDataMergerTestSupport.foundation(), connection, ObjectGraphUpdateHandler.PerStore(coordinator), 0L, 1_000_000L, 60_000L, coordinator));
            try {
                final Binary empty = ChunksWrapper.New(ByteBuffer.allocateDirect(8));
                assertTrue(merger.receiveDataOwned(empty), "the batch must be admitted");

                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_MS);
                while (coordinator.graphFailure() == null && System.nanoTime() < deadline) {
                    Thread.sleep(10L);
                }
                assertInstanceOf(peruncs.cluster.errors.GraphInvalidatedException.class, coordinator.graphFailure(),
                        "a failed batch must latch graph invalidity before the write lock releases");
                assertSame(scanBoom, coordinator.graphFailure().getCause(),
                        "the latched invalidity must name the failed section");
                assertThrows(peruncs.cluster.errors.GraphInvalidatedException.class,
                        () -> coordinator.read(() -> {
                        }), "coordinated reads must fail closed on a torn graph");
                assertNotNull(merger.failure(), "the failed batch must also latch the merger failure");
                final RuntimeException latched = merger.failure();
                assertNotNull(latched);
                assertSame(scanBoom, lastCause(latched),
                        "the merger failure must name the failing update section");
                assertThrows(RuntimeException.class, () -> merger.receiveDataOwned(ChunksWrapper.New(ByteBuffer.allocateDirect(8))),
                        "a failed merger must refuse further batches with its latched failure");
                /* A failed batch must not keep half-planned index scratch
                 * pinned behind the terminal failure. */
                assertScratchCleared(field(field(field(merger, "worker"), "indexMaintenance"), "scratch"));
            } finally {
                merger.dispose();
            }
        }
    }

    private static Object field(final Object owner, final String name) {
        try {
            final java.lang.reflect.Field field = owner.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return field.get(owner);
        } catch (final ReflectiveOperationException failure) {
            throw new AssertionError("cannot read " + name, failure);
        }
    }

    private static void assertScratchCleared(final Object scratch) {
        for (final String name : new String[]{"vectorGroups", "rebuiltGroups", "vectorModCounts",
                "vectorProbes", "vectorIndexes", "dirtyVectorIndexes", "groups", "maps"}) {
            final Object value = field(scratch, name);
            switch (value) {
                case java.util.Map<?, ?> map -> assertTrue(map.isEmpty(), name + " must be cleared after a failed batch");
                case java.util.Collection<?> list -> assertTrue(list.isEmpty(), name + " must be cleared after a failed batch");
                default -> throw new AssertionError("unexpected scratch field type: " + name);
            }
        }
    }

    private static Throwable lastCause(final Throwable failure) {
        Throwable cursor = failure;
        while (cursor.getCause() != null) {
            cursor = cursor.getCause();
        }
        return cursor;
    }

    /// Verifies a merger built without a coordinator reports none so callers run scans directly.
    @Test
    void unwiredMergerExposesNoCoordinator() {
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(StorageBinaryDataMergerTestSupport.configuration(StorageBinaryDataMergerTestSupport.foundation(), StorageBinaryDataMergerTestSupport.connection(), ObjectGraphUpdateHandler.PerStore(new StorageGraphCoordinator()), 0L, 1L, 60_000L));
        try {
            assertNull(merger.graphCoordinator(),
                    "a merger built without a coordinator must report none so callers run scans directly");
        } finally {
            merger.dispose();
        }
    }

    public static final class Root {
    }
}
