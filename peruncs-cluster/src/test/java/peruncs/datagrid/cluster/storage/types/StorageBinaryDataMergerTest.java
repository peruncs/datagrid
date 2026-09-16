package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistence;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceManager;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

        /// A disposed merger refuses both data and dictionary updates.
    @Test
    void disposedMergerRejectsDataAndDictionary() {
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.New(
                foundation(), connection(), ObjectGraphUpdateHandler.Synchronized(), 0L, 1L, 60_000L);

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
        final ObjectGraphUpdateHandler blockingHandler = updater ->
        {
            handlerEntered.countDown();
            try {
                releaseHandler.await(30, TimeUnit.SECONDS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            updater.run();
        };
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.New(
                foundation(), connection(), blockingHandler, 0L, 1L, 50L);
        try {
            assertThrows(IllegalStateException.class, () -> merger.receiveData(binary(2)),
                    "a materialization timeout must fail the delivery call");
            assertTrue(handlerEntered.await(10, TimeUnit.SECONDS), "the worker never entered the handler");
            assertNotNull(merger.failure(), "the timeout must latch a terminal merger failure");
            assertThrows(IllegalStateException.class, merger::awaitApplied);
            assertThrows(IllegalStateException.class, () -> merger.receiveData(binary(1)));
            assertThrows(IllegalStateException.class, () -> merger.receiveTypeDictionary("{}"));
        } finally {
            releaseHandler.countDown();
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
         * the merger's wait/retry budget, not the Store materializer. */
        final ObjectGraphUpdateHandler slowHandler = updater ->
        {
            handlerEntered.countDown();
            try {
                releaseHandler.await(30, TimeUnit.SECONDS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        };
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.New(
                foundation(), connection(), slowHandler, 0L, 1L, 200L);
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
            final StorageBinaryDataMerger merger = StorageBinaryDataMerger.New(
                    foundation(), connection(), ObjectGraphUpdateHandler.Synchronized(), 0L, 1L, 60_000L);
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
        final StorageBinaryDataMerger merger = StorageBinaryDataMerger.New(
                foundation(), connection(), ObjectGraphUpdateHandler.Synchronized(), 0L, 1L, 60_000L);

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
            final PersistenceManager persistenceManager = storage.createConnection().persistenceManager();
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
            final StorageBinaryDataMerger merger = StorageBinaryDataMerger.New(
                    foundationWithDictionaryLoader(), connection, ObjectGraphUpdateHandler.Synchronized(),
                    0L, 1L, 60_000L);
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
