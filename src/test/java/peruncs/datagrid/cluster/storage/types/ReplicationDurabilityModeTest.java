package peruncs.datagrid.cluster.storage.types;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the persisted durability-mode codes and their corrupt-data
/// failure contract.
class ReplicationDurabilityModeTest {
        /// The persisted codes are stable and round-trip.
    @Test
    void persistedCodesRoundTrip() {
        assertEquals(1, ReplicationDurabilityMode.ARCHIVE_FIRST.code());
        assertEquals(2, ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE.code());
        assertSame(ReplicationDurabilityMode.ARCHIVE_FIRST,
                ReplicationDurabilityMode.fromCode(1));
        assertSame(ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE,
                ReplicationDurabilityMode.fromCode(2));
    }

        /// An unknown code marks corrupt persisted data, not a programming error,
    /// so it fails with the domain's invalid-message exception.
    @Test
    void unknownCodeFailsAsCorruptData() {
        final StorageBinaryDataException failure = assertThrows(StorageBinaryDataException.class,
                () -> ReplicationDurabilityMode.fromCode(99));
        assertTrue(failure.getMessage().contains("unknown replication durability mode"),
                "unexpected message: " + failure.getMessage());
    }
}
