package peruncs.datagrid.cluster.node.replication;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Tests neutral transport behavior.
class NeutralTransportTest {

        /// Verifies no op transport keeps core usable without any provider dependency.
    @Test
    void noOpTransportKeepsCoreUsableWithoutAnyProviderDependency() {
        final ClusterReplicationTransport transport = ClusterReplicationTransport.noOp();
        final StorageBinaryDataDistributor distributor = transport.distributor("stream", false);
        distributor.messageIndex(12);
        distributor.ignoreDistribution(true);
        assertEquals(12, distributor.messageIndex());
        assertTrue(distributor.ignoreDistribution());
        assertEquals("none", transport.id());
        distributor.dispose();
        transport.close();
    }

        /// Verifies caching distributor keeps dictionaries associated with writing threads.
    @Test
    void cachingDistributorCarriesOneWriterDictionaryToTheNextTransaction() {
        final List<String> dictionaries = new ArrayList<>();
        final StorageBinaryDataDistributor delegate = new StorageBinaryDataDistributor() {
            private final AtomicReference<String> pending = new AtomicReference<>();

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
                dictionaries.add(this.pending.getAndSet(null));
            }

            public void dispose() {
            }
        };
        final StorageBinaryDataDistributor caching = StorageBinaryDataDistributor.Caching(delegate);
        caching.distributeTypeDictionary("dictionary-1");
        caching.distributeData(null);
        caching.distributeTypeDictionary("dictionary-2");
        caching.distributeData(null);
        assertEquals(List.of("dictionary-1", "dictionary-2"), dictionaries);
    }

        /// A restart dictionary queued by startup is consumed by the first Store thread.
    @Test
    void queuedDictionaryCrossesTheStartupThreadBoundary() {
        final StorageBinaryDataDistributor delegate = new StorageBinaryDataDistributor() {
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
        final StorageBinaryDataDistributor caching = StorageBinaryDataDistributor.Caching(delegate);
        caching.queueTypeDictionaryForNextTransaction("full-dictionary");
        assertEquals("full-dictionary", caching.consumeTypeDictionary());
        assertEquals(null, caching.consumeTypeDictionary());
    }

        /// A restart snapshot replaces an incremental dictionary staged before startup completed.
    @Test
    void queuedDictionarySupersedesStaleIncrementalDictionary() {
        final StorageBinaryDataDistributor delegate = new StorageBinaryDataDistributor() {
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
        final StorageBinaryDataDistributor caching = StorageBinaryDataDistributor.Caching(delegate);
        caching.distributeTypeDictionary("stale-incremental");
        caching.queueTypeDictionaryForNextTransaction("full-restart-dictionary");
        assertEquals("full-restart-dictionary", caching.consumeTypeDictionary());
    }
}
