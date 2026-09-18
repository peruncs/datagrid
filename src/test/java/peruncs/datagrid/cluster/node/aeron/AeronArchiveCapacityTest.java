package peruncs.datagrid.cluster.node.aeron;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies Archive capacity caching and fail-closed write admission.
class AeronArchiveCapacityTest {
    /// Verifies admission reserves at least one segment and caches the filesystem probe so repeated checks share one measurement while an oversized requirement is rejected.
    @Test
    void reservesAtLeastOneSegmentAndCachesTheFilesystemProbe() {
        final AtomicInteger probes = new AtomicInteger();
        final AeronArchiveCapacity capacity = new AeronArchiveCapacity(
                false, 100, 1_000, () -> {
            probes.incrementAndGet();
            return 1_100;
        });
        assertTrue(capacity.available());
        assertTrue(capacity.available(1_000));
        assertEquals(1, probes.get());
        assertFalse(capacity.available(1_001));
    }

    /// Verifies unknown, negative, and overflowing capacity requirements fail closed instead of admitting a write.
    @Test
    void rejectsUnknownNegativeAndOverflowingCapacityRequirements() {
        assertFalse(new AeronArchiveCapacity(false, 1, 1, () -> -1).available());
        assertFalse(new AeronArchiveCapacity(false, 1, 1, () -> Long.MAX_VALUE).available(-1));
        assertFalse(new AeronArchiveCapacity(false, Long.MAX_VALUE, 1, () -> Long.MAX_VALUE).available(1));
    }

    /// Verifies an external Archive always admits writes and reports no local disk capacity instead of probing the filesystem.
    @Test
    void externalArchiveDoesNotPretendToReportLocalDiskCapacity() {
        final AeronArchiveCapacity capacity = new AeronArchiveCapacity(true, Long.MAX_VALUE, 1, () -> 0);
        assertTrue(capacity.available());
        assertTrue(capacity.available(Long.MAX_VALUE));
        assertEquals(-1, capacity.usableSpaceBytes());
    }
}
