package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistence;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceManager;
import org.eclipse.serializer.persistence.types.PersistenceRootsView;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.errors.ReplicationUnavailableException;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the merger's disposal and terminal-timeout contracts.
///
/// The Store connection and persistence foundation are proxies: these cases
/// exercise the merger's own state machine, not Store behaviour, and the
/// blocking handler stops before any persistence call is reached.
class StorageBinaryDataMergerTest {
    private static Binary binary(final int bufferCount) {
        final ByteBuffer[] buffers = new ByteBuffer[bufferCount];
        for (int index = 0; index < bufferCount; index++) {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES);
            buffer.putLong(0x0102030405060708L + index);
            buffers[index] = buffer;
        }
        return ChunksWrapper.New(buffers);
    }

    private static BinaryPersistenceFoundation<?> foundation() {
        return (BinaryPersistenceFoundation<?>) Proxy.newProxyInstance(
                BinaryPersistenceFoundation.class.getClassLoader(),
                new Class<?>[]{BinaryPersistenceFoundation.class},
                (proxy, method, args) -> defaultValue(method.getReturnType()));
    }

    private static StorageConnection connection() {
        return (StorageConnection) Proxy.newProxyInstance(
                StorageConnection.class.getClassLoader(),
                new Class<?>[]{StorageConnection.class},
                (proxy, method, args) -> defaultValue(method.getReturnType()));
    }

    private static Object defaultValue(final Class<?> type) {
        if (type == boolean.class) return false;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == double.class) return 0d;
        if (type == float.class) return 0f;
        if (type == short.class) return (short) 0;
        if (type == byte.class) return (byte) 0;
        if (type == char.class) return (char) 0;
        return null;
    }

        /// A connection whose root scan always passes: the post-batch index
    /// validation runs for every batch (even when a test handler skips the
    /// materializer), so these cases need viewRoots to resolve instead of
    /// returning `null`.
    private static StorageConnection tolerantConnection() {
        final PersistenceRootsView emptyView = (PersistenceRootsView) Proxy.newProxyInstance(
                PersistenceRootsView.class.getClassLoader(),
                new Class<?>[]{PersistenceRootsView.class},
                (proxy, method, args) -> method.getName().equals("iterateEntries") ? args[0]
                        : defaultValue(method.getReturnType()));
        final PersistenceManager<?> managers = (PersistenceManager<?>) Proxy.newProxyInstance(
                PersistenceManager.class.getClassLoader(),
                new Class<?>[]{PersistenceManager.class},
                (proxy, method, args) -> method.getName().equals("viewRoots") ? emptyView
                        : defaultValue(method.getReturnType()));
        return (StorageConnection) Proxy.newProxyInstance(
                StorageConnection.class.getClassLoader(),
                new Class<?>[]{StorageConnection.class},
                (proxy, method, args) -> method.getName().equals("persistenceManager") ? managers
                        : defaultValue(method.getReturnType()));
    }

        /// A disposed merger refuses both data and dictionary updates.
    @Test
    void disposedMergerRejectsDataAndDictionary() {
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(StorageBinaryDataMergerTestSupport.configuration(foundation(), connection(), ObjectGraphUpdateHandler.PerStore(new StorageGraphCoordinator()), 0L, 1L, 60_000L));

        merger.dispose();

        assertThrows(IllegalStateException.class, () -> merger.receiveData(binary(1)));
        assertThrows(IllegalStateException.class, () -> merger.receiveTypeDictionary("{}"));
    }

        /// One materialization timeout latches a terminal failure: the merger
        /// then refuses data, dictionary updates, and further waits.
    @Test
    void applyTimeoutLatchesTerminalFailure() throws Exception {
        final CountDownLatch handlerEntered = new CountDownLatch(1);
        final CountDownLatch releaseHandler = new CountDownLatch(1);
        /* The handler deliberately does not run the updater: this case exercises
         * the merger's wait/retry budget, not the Store materializer. The
         * post-batch validation still runs afterwards, hence the tolerant
         * connection. */
        final ObjectGraphUpdateHandler blockingHandler = updater ->
        {
            handlerEntered.countDown();
            try {
                releaseHandler.await(30, TimeUnit.SECONDS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        };
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(StorageBinaryDataMergerTestSupport.configuration(foundation(), tolerantConnection(), blockingHandler, 0L, 1L, 50L));
        try {
            assertThrows(IllegalStateException.class, () -> merger.receiveData(binary(2)),
                    "a materialization timeout must fail the delivery call");
            assertTrue(handlerEntered.await(10, TimeUnit.SECONDS), "the worker never entered the handler");
            assertNotNull(merger.failure(), "the timeout must latch a terminal merger failure");
            assertInstanceOf(ReplicationUnavailableException.class, merger.failure(),
                    "a wait timeout is a lifecycle failure, not corrupt assembled data");
            assertTrue(merger.failure().getMessage().contains("Timed out"),
                    "a timeout must say timed out: " + merger.failure().getMessage());
            assertFalse(merger.failure().getMessage().contains("or failed"),
                    "a timeout must not be mislabeled as a genuine failure: " + merger.failure().getMessage());
            assertThrows(IllegalStateException.class, merger::awaitApplied);
            assertThrows(IllegalStateException.class, () -> merger.receiveData(binary(1)));
            assertThrows(IllegalStateException.class, () -> merger.receiveTypeDictionary("{}"));
        } finally {
            releaseHandler.countDown();
            merger.dispose();
        }
    }

        /// A genuine materialization failure says failed, never timed out.
    @Test
    void genuineFailureSaysFailedNotTimedOut() throws Exception {
        final IllegalStateException boom = new IllegalStateException("boom");
        final ObjectGraphUpdateHandler failingHandler = updater ->
        {
            throw boom;
        };
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(StorageBinaryDataMergerTestSupport.configuration(foundation(), tolerantConnection(), failingHandler, 0L, 1_000_000L, 60_000L));
        try {
            merger.receiveDataOwned(binary(1));
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
            while (merger.failure() == null && System.nanoTime() < deadline) {
                Thread.sleep(50L);
            }
            assertNotNull(merger.failure(), "a throwing handler must latch a terminal merger failure");
            assertTrue(merger.failure().getMessage().contains("failed"),
                    "a genuine failure must say failed: " + merger.failure().getMessage());
            assertFalse(merger.failure().getMessage().contains("Timed out"),
                    "a genuine failure must not be mislabeled as a timeout: " + merger.failure().getMessage());
        } finally {
            merger.dispose();
        }
    }

        /// A materialization that finishes within the retry budget must not fail
        /// the merger: one slow batch is not a terminal condition.
    @Test
    void applyTimeoutRetriesBeforeFailing() throws Exception {
        final CountDownLatch handlerEntered = new CountDownLatch(1);
        final CountDownLatch releaseHandler = new CountDownLatch(1);
        final java.util.concurrent.atomic.AtomicReference<Throwable> deliveryFailure =
                new java.util.concurrent.atomic.AtomicReference<>();
        /* The handler deliberately does not run the updater: this case exercises
         * the merger's wait/retry budget, not the Store materializer. The
         * post-batch validation still runs afterwards, hence the tolerant
         * connection. */
        final ObjectGraphUpdateHandler slowHandler = updater ->
        {
            handlerEntered.countDown();
            try {
                releaseHandler.await(30, TimeUnit.SECONDS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        };
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(StorageBinaryDataMergerTestSupport.configuration(foundation(), tolerantConnection(), slowHandler, 0L, 1L, 200L));
        try {
            final Thread delivering = Thread.ofVirtual().start(() ->
            {
                try {
                    merger.receiveData(binary(2));
                } catch (final Throwable failure) {
                    deliveryFailure.set(failure);
                }
            });
            assertTrue(handlerEntered.await(10, TimeUnit.SECONDS), "the worker never entered the handler");
            /* Wait past the first bounded wait, then let the worker finish while
             * the retry budget is still available. */
            Thread.sleep(300L);
            releaseHandler.countDown();
            delivering.join(TimeUnit.SECONDS.toMillis(10));

            assertFalse(delivering.isAlive(), "delivery did not finish after the worker was released");
            assertNull(deliveryFailure.get(), "a slow-but-progressing materialization must not fail the merger");
            assertNull(merger.failure(), "the retry budget must absorb one slow batch");
        } finally {
            releaseHandler.countDown();
            merger.dispose();
        }
    }

        /// A dispose racing an accept must fail cleanly: the merger either
        /// accepts the batch or refuses it, but never double-releases the
        /// native buffers.
    @Test
    void disposeRacingAcceptFailsCleanly() throws Exception {
        for (int iteration = 0; iteration < 8; iteration++) {
            final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(StorageBinaryDataMergerTestSupport.configuration(foundation(), connection(), ObjectGraphUpdateHandler.PerStore(new StorageGraphCoordinator()), 0L, 1L, 60_000L));
            final AtomicReference<Throwable> unexpected = new AtomicReference<>();
            final CountDownLatch start = new CountDownLatch(1);
            final Thread disposing = Thread.ofVirtual().start(() ->
            {
                await(start);
                merger.dispose();
            });
            final Thread delivering = Thread.ofVirtual().start(() ->
            {
                await(start);
                try {
                    merger.receiveData(binary(2));
                } catch (final IllegalStateException expected) {
                    /* Disposed or failed: a clean refusal, not a corruption. */
                } catch (final Throwable failure) {
                    unexpected.set(failure);
                }
            });
            start.countDown();
            disposing.join(TimeUnit.SECONDS.toMillis(10));
            delivering.join(TimeUnit.SECONDS.toMillis(10));

            assertNull(unexpected.get(),
                    "iteration %s: a dispose race must fail cleanly: %s".formatted(iteration, unexpected.get()));
        }
    }

        /// Owned delivery on a disposed merger releases the transferred buffers
        /// exactly once and reports the refusal.
    @Test
    void receiveDataOwnedOnDisposedMergerFailsCleanly() {
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(StorageBinaryDataMergerTestSupport.configuration(foundation(), connection(), ObjectGraphUpdateHandler.PerStore(new StorageGraphCoordinator()), 0L, 1L, 60_000L));

        merger.dispose();

        final IllegalStateException failure = assertThrows(
                IllegalStateException.class, () -> merger.receiveDataOwned(binary(2)));
        assertTrue(failure.getMessage().contains("disposed"),
                "an owned delivery after disposal must report the disposal: " + failure.getMessage());
        assertEquals(0, failure.getSuppressed().length,
                "a clean refusal must not carry a cleanup failure: " + java.util.Arrays.toString(failure.getSuppressed()));
    }

        /// The dictionary merge and the Store import must never overlap: both
        /// mutate the same persistence state under the materialization lock.
    @Test
    void dictionaryMergeAndDataImportAreMutuallyExclusive(@TempDir final Path root) throws Exception {
        final EmbeddedStorageManager storage = startStorage(root);
        try {
            final PersistenceManager<Binary> persistenceManager = storage.createConnection().persistenceManager();
            final CountDownLatch dictionaryEntered = new CountDownLatch(1);
            final CountDownLatch releaseDictionary = new CountDownLatch(1);
            final AtomicBoolean dictionaryInProgress = new AtomicBoolean();
            final AtomicBoolean importDuringDictionary = new AtomicBoolean();
            final StorageConnection connection = (StorageConnection) Proxy.newProxyInstance(
                    StorageConnection.class.getClassLoader(),
                    new Class<?>[]{StorageConnection.class},
                    (proxy, method, args) ->
                    {
                        switch (method.getName()) {
                            case "persistenceManager":
                                dictionaryInProgress.set(true);
                                dictionaryEntered.countDown();
                                await(releaseDictionary);
                                dictionaryInProgress.set(false);
                                return persistenceManager;
                            case "importData":
                                if (dictionaryInProgress.get()) importDuringDictionary.set(true);
                                return null;
                            default:
                                return defaultValue(method.getReturnType());
                        }
                    });
            final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(StorageBinaryDataMergerTestSupport.configuration(foundationWithDictionaryLoader(), connection, updater ->
            {
                /* The synthetic batch carries no valid entity header; this case
                 * observes the import/dictionary exclusion, not materialization. */
            }, 0L, 1L, 60_000L));
            final AtomicReference<Throwable> dictionaryFailure = new AtomicReference<>();
            final AtomicReference<Throwable> dataFailure = new AtomicReference<>();
            final Thread dictionary = Thread.ofVirtual().start(() ->
            {
                try {
                    merger.receiveTypeDictionary("");
                } catch (final Throwable failure) {
                    dictionaryFailure.set(failure);
                }
            });
            try {
                assertTrue(dictionaryEntered.await(10, TimeUnit.SECONDS), "the dictionary merge never entered");
                final Thread data = Thread.ofVirtual().start(() ->
                {
                    try {
                        merger.receiveData(binary(1));
                    } catch (final Throwable failure) {
                        dataFailure.set(failure);
                    }
                });
                Thread.sleep(300L);
                assertFalse(importDuringDictionary.get(),
                        "the Store import must not run while the dictionary merge holds the materialization lock");

                releaseDictionary.countDown();
                dictionary.join(TimeUnit.SECONDS.toMillis(10));
                data.join(TimeUnit.SECONDS.toMillis(10));
                assertNull(dictionaryFailure.get(), "dictionary merge failed: " + dictionaryFailure.get());
                assertNull(dataFailure.get(), "data import failed: " + dataFailure.get());
            } finally {
                releaseDictionary.countDown();
                merger.dispose();
            }
        } finally {
            storage.shutdown();
        }
    }

    private static void await(final CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

        /// The dictionary merge must run through the update handler, not just
    /// the materialization lock: the lock alone cannot exclude application
    /// reads that joined the coordinator's read side.
    @Test
    void dictionaryMergeRunsThroughTheUpdateHandler(
            @TempDir final Path writerRoot, @TempDir final Path readerRoot) {
        /* Both stores share the root class so no per-store type id collides.
         * The field is declared as Object: the reader's null never registers
         * the runtime type, while the writer's stored Extra does — so only
         * the writer dictionary knows it. */
        final String dictionary;
        final Root writerInstance = new Root();
        writerInstance.extra = new Extra();
        try (EmbeddedStorageManager writer = EmbeddedStorage.start(writerInstance, writerRoot)) {
            writer.storeRoot();
            dictionary = PersistenceTypeDictionaryAssembler.New().assemble(writer.typeDictionary());
        }
        final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
        final AtomicBoolean handlerUsed = new AtomicBoolean();
        final ObjectGraphUpdateHandler recording = updater ->
        {
            handlerUsed.set(true);
            coordinator.write(updater);
        };
        try (EmbeddedStorageManager reader = EmbeddedStorage.start(new Root(), readerRoot)) {
            final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(StorageBinaryDataMergerTestSupport.configuration(foundationWithDictionaryLoader(), reader.createConnection(), recording, 0L, 1L, 60_000L, coordinator));
            try {
                merger.receiveTypeDictionary(dictionary);
                assertTrue(handlerUsed.get(),
                        "registering unknown writer types must run through the update handler");
                assertNull(merger.failure(), "the dictionary merge must not fail the merger");
            } finally {
                merger.dispose();
            }
        }
    }

        /// A dictionary merge in progress excludes application reads: the
    /// registration runs on the coordinator's write side.
    @Test
    void dictionaryMergeExcludesApplicationReads(
            @TempDir final Path writerRoot, @TempDir final Path readerRoot) throws Exception {
        final String dictionary;
        final Root writerInstance = new Root();
        writerInstance.extra = new Extra();
        try (EmbeddedStorageManager writer = EmbeddedStorage.start(writerInstance, writerRoot)) {
            writer.storeRoot();
            dictionary = PersistenceTypeDictionaryAssembler.New().assemble(writer.typeDictionary());
        }
        final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
        final CountDownLatch handlerEntered = new CountDownLatch(1);
        final ObjectGraphUpdateHandler recording = updater ->
        {
            handlerEntered.countDown();
            coordinator.write(updater);
        };
        try (EmbeddedStorageManager reader = EmbeddedStorage.start(new Root(), readerRoot)) {
            final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(StorageBinaryDataMergerTestSupport.configuration(foundationWithDictionaryLoader(), reader.createConnection(), recording, 0L, 1L, 60_000L, coordinator));
            try {
                final CountDownLatch readHeld = new CountDownLatch(1);
                final CountDownLatch releaseRead = new CountDownLatch(1);
                final Thread holder = Thread.ofVirtual().start(() -> coordinator.read(() ->
                {
                    readHeld.countDown();
                    await(releaseRead);
                }));
                assertTrue(readHeld.await(10, TimeUnit.SECONDS), "the application read never started");

                final AtomicBoolean mergeDone = new AtomicBoolean();
                final AtomicReference<Throwable> mergeFailure = new AtomicReference<>();
                final Thread merging = Thread.ofVirtual().start(() ->
                {
                    try {
                        merger.receiveTypeDictionary(dictionary);
                        mergeDone.set(true);
                    } catch (final Throwable failure) {
                        mergeFailure.set(failure);
                    }
                });
                /* The read-phase plan shares the read side and proceeds; the
                 * registration must then block on the write side. */
                assertTrue(handlerEntered.await(10, TimeUnit.SECONDS),
                        "the merge never reached the update handler");
                Thread.sleep(300L);
                assertFalse(mergeDone.get(),
                        "the dictionary merge entered the write side while an application read was held");
                assertNull(mergeFailure.get(), "the blocked merge failed: " + mergeFailure.get());

                releaseRead.countDown();
                merging.join(TimeUnit.SECONDS.toMillis(10));
                holder.join(TimeUnit.SECONDS.toMillis(10));
                assertTrue(mergeDone.get(), "the merge never finished after the read released");
                assertNull(mergeFailure.get(), "the merge failed: " + mergeFailure.get());
                assertNull(merger.failure(), "the merge must not fail the merger");
            } finally {
                merger.dispose();
            }
        }
    }

    public static final class Extra {
        public String value = "writer-only";
    }

    public static final class Root {
        public Object extra;
    }

        /// A stream of multi-buffer commits coalesces by queued payload bytes
    /// instead of by buffer count: three hundred small channel buffers must
    /// not force one synchronous materialization per commit.
    @Test
    void multiBufferCommitsCoalesceByBytesNotBufferCount()  {
        final AtomicInteger handlerCalls = new AtomicInteger();
        final ObjectGraphUpdateHandler counting = updater -> handlerCalls.incrementAndGet();
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(
                StorageBinaryDataMergerTestSupport.configuration(
                        foundation(), connection(), counting, 60_000L, 64L << 20, 60_000L));
        try {
            for (int commit = 0; commit < 100; commit++) {
                merger.receiveDataOwned(binary(3));
            }
            merger.awaitApplied();

            assertNull(merger.failure(), "coalesced delivery must not fail the merger");
            assertTrue(handlerCalls.get() >= 1, "the queued data must still be materialized");
            assertTrue(handlerCalls.get() <= 2,
                    "a multi-buffer commit stream must coalesce into one batch; saw "
                            + handlerCalls.get() + " materializations");
        } finally {
            merger.dispose();
        }
    }

        /// A bare foundation lacks the dictionary loader and storer that its
    /// type-handler manager needs; the dictionary path also needs real ones.
    private static BinaryPersistenceFoundation<?> foundationWithDictionaryLoader() {
        final BinaryPersistenceFoundation<?> foundation = BinaryPersistence.Foundation();
        foundation.setTypeDictionaryLoader(() -> "");
        foundation.setTypeDictionaryStorer(ignored -> {
        });
        return foundation;
    }

    private static EmbeddedStorageManager startStorage(final Path root) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(root))
                .setChannelCountProvider(Storage.ChannelCountProvider(4))
                .createConfiguration();
        return EmbeddedStorage.Foundation(configuration).start();
    }
}
