package peruncs.datagrid.cluster.node.store;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies that the storage limit gate recovers without boundary flapping.
class StorageLimitGateTest {
        /// Usage must fall below the hysteresis release point before writes reopen.
    @Test
    void releasesOnlyAfterUsageLeavesHysteresisBand() {
        final StorageLimitGate gate = StorageLimitGate.New(10);

        gate.updateUsage(10_000_000_000L);
        assertTrue(gate.limitReached());

        gate.updateUsage(9_500_000_000L);
        assertTrue(gate.limitReached());

        gate.updateUsage(9_000_000_000L);
        assertFalse(gate.limitReached());
    }

        /// A fresh gate accepts writes.
    @Test
    void startsBelowLimit() {
        assertFalse(StorageLimitGate.New(10).limitReached());
    }

        /// Usage below the limit never trips the gate.
    @Test
    void ignoresUsageBelowLimit() {
        final StorageLimitGate gate = StorageLimitGate.New(10);

        gate.updateUsage(9_999_999_999L);

        assertFalse(gate.limitReached());
    }

        /// The gate exposes its configured limit for log messages.
    @Test
    void exposesConfiguredLimit() {
        final StorageLimitGate gate = StorageLimitGate.New(10);

        assertEquals(10, gate.limitGb());
        assertEquals(10_000_000_000L, gate.limitBytes());
    }

        /// A non-positive limit is rejected.
    @Test
    void rejectsNonPositiveLimit() {
        assertThrows(IllegalArgumentException.class, () -> StorageLimitGate.New(0));
        assertThrows(IllegalArgumentException.class, () -> StorageLimitGate.New(-5));
    }
}
