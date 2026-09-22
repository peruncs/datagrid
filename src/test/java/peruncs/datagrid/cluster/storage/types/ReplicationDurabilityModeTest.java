package peruncs.datagrid.cluster.storage.types;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.errors.CorruptReplicationDataException;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the persisted durability-mode codes and their corrupt-data
/// failure contract.
class ReplicationDurabilityModeTest {
        /// The persisted codes are stable and round-trip.
    @Test
    void persistedCodesRoundTrip() {
        assertEquals(1, ReplicationDurabilityMode.ARCHIVE_FIRST.code());
        assertSame(ReplicationDurabilityMode.ARCHIVE_FIRST,
                ReplicationDurabilityMode.fromCode(1));
        assertThrows(CorruptReplicationDataException.class, () -> ReplicationDurabilityMode.fromCode(2));
    }

        /// An unknown code marks corrupt persisted data, not a programming error,
    /// so it fails with the domain's invalid-message exception.
    @Test
    void unknownCodeFailsAsCorruptData() {
        final CorruptReplicationDataException failure = assertThrows(CorruptReplicationDataException.class,
                () -> ReplicationDurabilityMode.fromCode(99));
        assertTrue(failure.getMessage().contains("unknown replication durability mode"),
                "unexpected message: " + failure.getMessage());
    }
}
