package peruncs.datagrid.cluster.storage.binary;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceManager;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.storage.StorageGraphCoordinator;

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
