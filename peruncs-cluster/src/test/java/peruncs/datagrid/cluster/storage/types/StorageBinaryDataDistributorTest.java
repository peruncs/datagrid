package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Tests storage binary data distributor behavior.
class StorageBinaryDataDistributorTest {
        /// Verifies the one-writer dictionary handoff without thread-bound state.
    @Test
    void carriesDictionariesToTheNextTransaction() {
        final List<String> dictionaries = new ArrayList<>();
        final StorageBinaryDataDistributor delegate = new StorageBinaryDataDistributor() {
            private final AtomicReference<String> pending = new AtomicReference<>();

            @Override
            public void distributeData(final Binary ignored) {
                dictionaries.add(this.pending.getAndSet(null));
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
        caching.distributeTypeDictionary("dictionary-1");
        caching.distributeData(null);
        caching.distributeTypeDictionary("dictionary-2");
        caching.distributeData(null);
        assertEquals(List.of("dictionary-1", "dictionary-2"), dictionaries);
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

        /// A stale incremental arriving after a restart snapshot is dropped.
    @Test
    void incrementalAfterQueuedSnapshotIsDropped() {
        final StorageBinaryDataDistributor caching =
                StorageBinaryDataDistributor.Caching(StorageBinaryDataDistributor.NoOp());
        caching.queueTypeDictionaryForNextTransaction("full-restart-dictionary");
        caching.distributeTypeDictionary("stale-incremental");
        assertEquals("full-restart-dictionary", caching.consumeTypeDictionary());
        assertNull(caching.consumeTypeDictionary());
    }

        /// A queued snapshot travels with the next data transaction, not stranded.
    @Test
    void queuedSnapshotIsForwardedWithData() {
        final List<String> dictionaries = new ArrayList<>();
        final StorageBinaryDataDistributor delegate = new StorageBinaryDataDistributor() {
            @Override
            public void distributeData(final Binary ignored) {
            }

            @Override
            public void distributeTypeDictionary(final String value) {
                dictionaries.add(value);
            }

            @Override
            public void dispose() {
            }
        };
        final StorageBinaryDataDistributor caching = StorageBinaryDataDistributor.Caching(delegate);
        caching.queueTypeDictionaryForNextTransaction("full-restart-dictionary");
        caching.distributeData(null);
        assertEquals(List.of("full-restart-dictionary"), dictionaries);
        assertNull(caching.consumeTypeDictionary());
    }
}
