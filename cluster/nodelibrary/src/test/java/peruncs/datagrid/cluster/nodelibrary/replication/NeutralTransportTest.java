package peruncs.datagrid.cluster.nodelibrary.replication;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests neutral transport behavior. */
class NeutralTransportTest {
    private static Thread dictionaryThread(
            final String name,
            final ClusterStorageBinaryDataDistributor distributor,
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
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }, name);
    }

    /** Verifies no op transport keeps core usable without any provider dependency. */
    @Test
    void noOpTransportKeepsCoreUsableWithoutAnyProviderDependency() {
        final ClusterReplicationTransport transport = ClusterReplicationTransport.noOp();
        final ClusterStorageBinaryDataDistributor distributor = transport.distributor("stream", false);
        distributor.messageIndex(12);
        distributor.ignoreDistribution(true);
        assertEquals(12, distributor.messageIndex());
        assertTrue(distributor.ignoreDistribution());
        assertEquals("none", transport.id());
        distributor.dispose();
        transport.close();
    }

    /** Verifies caching distributor keeps dictionaries associated with writing threads. */
    @Test
    void cachingDistributorKeepsDictionariesAssociatedWithWritingThreads() throws Exception {
        final Map<String, String> dictionaries = new ConcurrentHashMap<>();
        final ClusterStorageBinaryDataDistributor delegate = new ClusterStorageBinaryDataDistributor() {
            private final ThreadLocal<String> pending = new ThreadLocal<>();

            public void messageIndex(final long value) {
            }

            public long messageIndex() {
                return -1;
            }

            public void ignoreDistribution(final boolean value) {
            }

            public boolean ignoreDistribution() {
                return false;
            }

            public void distributeTypeDictionary(final String value) {
                this.pending.set(value);
            }

            public void distributeData(final org.eclipse.serializer.persistence.binary.types.Binary value) {
                dictionaries.put(Thread.currentThread().getName(), this.pending.get());
            }

            public void dispose() {
            }
        };
        final ClusterStorageBinaryDataDistributor caching = ClusterStorageBinaryDataDistributor.Caching(delegate);
        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread first = dictionaryThread("cluster-writer-1", caching, "dictionary-1", ready, release);
        final Thread second = dictionaryThread("cluster-writer-2", caching, "dictionary-2", ready, release);
        first.start();
        second.start();
        ready.await();
        release.countDown();
        first.join();
        second.join();
        assertEquals("dictionary-1", dictionaries.get("cluster-writer-1"));
        assertEquals("dictionary-2", dictionaries.get("cluster-writer-2"));
    }

    /** A restart dictionary queued by startup is consumed by the first Store thread. */
    @Test
    void queuedDictionaryCrossesTheStartupThreadBoundary() {
        final ClusterStorageBinaryDataDistributor delegate = new ClusterStorageBinaryDataDistributor() {
            public void messageIndex(final long value) {
            }

            public long messageIndex() {
                return -1;
            }

            public void ignoreDistribution(final boolean value) {
            }

            public boolean ignoreDistribution() {
                return false;
            }

            public void distributeTypeDictionary(final String value) {
            }

            public void distributeData(final org.eclipse.serializer.persistence.binary.types.Binary value) {
            }

            public void dispose() {
            }
        };
        final ClusterStorageBinaryDataDistributor caching = ClusterStorageBinaryDataDistributor.Caching(delegate);
        caching.queueTypeDictionaryForNextTransaction("full-dictionary");
        assertEquals("full-dictionary", caching.consumeTypeDictionary());
        assertEquals(null, caching.consumeTypeDictionary());
    }

    /** A restart snapshot replaces an incremental dictionary staged before startup completed. */
    @Test
    void queuedDictionarySupersedesStaleThreadLocalDictionary() {
        final ClusterStorageBinaryDataDistributor delegate = new ClusterStorageBinaryDataDistributor() {
            public void messageIndex(final long value) {
            }

            public long messageIndex() {
                return -1;
            }

            public void ignoreDistribution(final boolean value) {
            }

            public boolean ignoreDistribution() {
                return false;
            }

            public void distributeTypeDictionary(final String value) {
            }

            public void distributeData(final org.eclipse.serializer.persistence.binary.types.Binary value) {
            }

            public void dispose() {
            }
        };
        final ClusterStorageBinaryDataDistributor caching = ClusterStorageBinaryDataDistributor.Caching(delegate);
        caching.distributeTypeDictionary("stale-incremental");
        caching.queueTypeDictionaryForNextTransaction("full-restart-dictionary");
        assertEquals("full-restart-dictionary", caching.consumeTypeDictionary());
    }
}
