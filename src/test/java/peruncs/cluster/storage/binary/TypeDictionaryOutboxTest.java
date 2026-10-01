package peruncs.cluster.storage.binary;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/// Verifies the outbox keeps exactly one dictionary and protects a restart snapshot.
class TypeDictionaryOutboxTest {
    @Test
    void anIncrementalDictionaryIsConsumedOnce() {
        final TypeDictionaryOutbox outbox = new TypeDictionaryOutbox();
        outbox.stageIncremental("dictionary-1");
        outbox.stageIncremental("dictionary-2");
        assertEquals("dictionary-2", outbox.consume(), "the newest incremental export replaces the older one");
        assertNull(outbox.consume(), "consuming empties the slot");
    }

    @Test
    void aRestartSnapshotIsNotOverwrittenByLaterIncrementalExports() {
        final TypeDictionaryOutbox outbox = new TypeDictionaryOutbox();
        outbox.stageSnapshot("full-restart-dictionary");
        outbox.stageIncremental("stale-incremental");
        assertEquals("full-restart-dictionary", outbox.consume());
        assertNull(outbox.consume());
    }

    @Test
    void clearingAnIncrementalDictionaryKeepsAWaitingSnapshot() {
        final TypeDictionaryOutbox outbox = new TypeDictionaryOutbox();
        outbox.stageSnapshot("full-restart-dictionary");
        outbox.stageIncremental(null);
        assertEquals("full-restart-dictionary", outbox.consume());

        outbox.stageIncremental("incremental");
        outbox.stageIncremental(null);
        assertNull(outbox.consume(), "null clears a staged incremental dictionary");
    }

    @Test
    void aSnapshotReplacesAnIncrementalDictionaryStagedDuringStartup() {
        final TypeDictionaryOutbox outbox = new TypeDictionaryOutbox();
        outbox.stageIncremental("accumulated-while-starting");
        outbox.stageSnapshot("full-restart-dictionary");
        assertEquals("full-restart-dictionary", outbox.consume());
    }
}
