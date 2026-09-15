package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Tests storage binary data distributor behavior.
class StorageBinaryDataDistributorTest {
    private static Thread thread(
            final String name,
            final StorageBinaryDataDistributor distributor,
            final String dictionary,
            final CountDownLatch ready,
            final CountDownLatch release
    ) {
        return new Thread(() ->
        {
            distributor.distributeTypeDictionary(dictionary);
            ready.countDown();
            try {
                release.await();
                distributor.distributeData(null);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, name);
    }

        /// Verifies preservation of concurrent type dictionaries with their committing thread.
    @Test
    void keepsConcurrentTypeDictionariesWithTheirCommittingThread()
            throws Exception {
        final Map<String, String> dictionaries = new ConcurrentHashMap<>();
        final StorageBinaryDataDistributor delegate = new StorageBinaryDataDistributor() {
            private final ThreadLocal<String> pending = new ThreadLocal<>();

            @Override
            public void distributeData(final Binary ignored) {
                dictionaries.put(Thread.currentThread().getName(), this.pending.get());
            }

            @Override
            public void distributeTypeDictionary(final String value) {
                this.pending.set(value);
            }

            @Override
            public void dispose() {
            }
        };
        final StorageBinaryDataDistributor.Caching caching =
                (StorageBinaryDataDistributor.Caching) StorageBinaryDataDistributor.Caching(delegate);
        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread first = thread("writer-1", caching, "dictionary-1", ready, release);
        final Thread second = thread("writer-2", caching, "dictionary-2", ready, release);
        first.start();
        second.start();
        ready.await();
        release.countDown();
        first.join();
        second.join();
        assertEquals("dictionary-1", dictionaries.get("writer-1"));
        assertEquals("dictionary-2", dictionaries.get("writer-2"));
    }

        /// Verifies that the dictionary is cleared after delegate failure.
    @Test
    void clearsDictionaryAfterDelegateFailure() {
        final AtomicInteger dictionaryCalls = new AtomicInteger();
        final StorageBinaryDataDistributor delegate = new StorageBinaryDataDistributor() {
            public void distributeData(final Binary ignored) {
            }

            public void distributeTypeDictionary(final String value) {
                dictionaryCalls.incrementAndGet();
                throw new IllegalStateException("dictionary failure");
            }

            public void dispose() {
            }
        };
        final StorageBinaryDataDistributor caching = StorageBinaryDataDistributor.Caching(delegate);
        caching.distributeTypeDictionary("stale");
        assertThrows(IllegalStateException.class, () -> caching.distributeData(null));
        assertEquals(1, dictionaryCalls.get());
        assertNull(caching.consumeTypeDictionary());
    }
}
