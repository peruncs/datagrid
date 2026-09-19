package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
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

        /// Verifies that a failed data delivery keeps the staged dictionary for
    /// the retry, so a later data message cannot ship unresolvable type ids.
    @Test
    void retainsDictionaryUntilDelegateDataDeliverySucceeds() {
        final List<String> dictionaries = new ArrayList<>();
        final AtomicBoolean failData = new AtomicBoolean(true);
        final StorageBinaryDataDistributor delegate = new StorageBinaryDataDistributor() {
            public void distributeData(final Binary ignored) {
                if (failData.get()) throw new IllegalStateException("data failure");
            }

            public void distributeTypeDictionary(final String value) {
                dictionaries.add(value);
            }

            public void dispose() {
            }
        };
        final StorageBinaryDataDistributor caching = StorageBinaryDataDistributor.Caching(delegate);
        caching.distributeTypeDictionary("survives");

        assertThrows(IllegalStateException.class, () -> caching.distributeData(null));
        assertEquals(List.of("survives"), dictionaries);

        failData.set(false);
        caching.distributeData(null);
        assertEquals(List.of("survives", "survives"), dictionaries,
                "a retried data delivery must re-send the still-pending dictionary");
        assertNull(caching.consumeTypeDictionary(),
                "the staged dictionary clears only after the delegate accepted the data");
    }

        /// Verifies that a failed dictionary delivery leaves the dictionary
    /// staged instead of losing it.
    @Test
    void retainsDictionaryWhenDictionaryDeliveryFails() {
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
        caching.distributeTypeDictionary("pending");
        assertThrows(IllegalStateException.class, () -> caching.distributeData(null));
        assertEquals(1, dictionaryCalls.get());
        assertEquals("pending", caching.consumeTypeDictionary(),
                "a failed dictionary delivery must not drop the staged dictionary");
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
