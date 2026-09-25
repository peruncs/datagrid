package peruncs.cluster.node.replication;

import org.junit.jupiter.api.Test;
import peruncs.cluster.storage.binary.ReplicationPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Tests neutral transport behavior.
class NeutralTransportTest {

        /// Verifies no op transport keeps core usable without any provider dependency.
    @Test
    void noOpTransportKeepsCoreUsableWithoutAnyProviderDependency() {
        final ClusterReplicationTransport transport = ClusterReplicationTransport.noOp();
        final ReplicationPublisher distributor = transport.distributor("stream");
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
        final ReplicationPublisher delegate = new ReplicationPublisher() {
            private final AtomicReference<String> pending = new AtomicReference<>();

            public void distributeTypeDictionary(final String value) {
                this.pending.set(value);
            }

            public void distributeData(final org.eclipse.serializer.persistence.binary.types.Binary value) {
                dictionaries.add(this.pending.getAndSet(null));
            }

            public void dispose() {
            }
        };
        final ReplicationPublisher caching = ReplicationPublisher.Caching(delegate);
        caching.distributeTypeDictionary("dictionary-1");
        caching.distributeData(null);
        caching.distributeTypeDictionary("dictionary-2");
        caching.distributeData(null);
        assertEquals(List.of("dictionary-1", "dictionary-2"), dictionaries);
    }

        /// A restart dictionary queued by startup is consumed by the first Store thread.
    @Test
    void queuedDictionaryCrossesTheStartupThreadBoundary() {
        final ReplicationPublisher delegate = new ReplicationPublisher() {
            public void distributeTypeDictionary(final String value) {
            }

            public void distributeData(final org.eclipse.serializer.persistence.binary.types.Binary value) {
            }

            public void dispose() {
            }
        };
        final ReplicationPublisher caching = ReplicationPublisher.Caching(delegate);
        caching.queueTypeDictionaryForNextTransaction("full-dictionary");
        assertEquals("full-dictionary", caching.consumeTypeDictionary());
        assertNull(caching.consumeTypeDictionary());
    }

        /// A restart snapshot replaces an incremental dictionary staged before startup completed.
    @Test
    void queuedDictionarySupersedesStaleIncrementalDictionary() {
        final ReplicationPublisher delegate = new ReplicationPublisher() {
            public void distributeTypeDictionary(final String value) {
            }

            public void distributeData(final org.eclipse.serializer.persistence.binary.types.Binary value) {
            }

            public void dispose() {
            }
        };
        final ReplicationPublisher caching = ReplicationPublisher.Caching(delegate);
        caching.distributeTypeDictionary("stale-incremental");
        caching.queueTypeDictionaryForNextTransaction("full-restart-dictionary");
        assertEquals("full-restart-dictionary", caching.consumeTypeDictionary());
    }
}
