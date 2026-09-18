package peruncs.datagrid.cluster.storage.types;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/// Covers the replication retry deadline arithmetic: budget validation,
/// saturating deadline math, and expiry against both wall-clock and manual clocks.
class ReplicationRetryTest {
    /// Verifies zero and negative retry budgets are rejected with an illegal-argument failure.
    @Test
    void rejectsUnboundedOrEmptyBudgets() {
        assertThrows(IllegalArgumentException.class, () -> ReplicationRetry.deadlineNanos(0L));
        assertThrows(IllegalArgumentException.class, () -> ReplicationRetry.deadlineNanos(-1L));
    }

    /// Verifies deadline arithmetic saturates at the maximum value and remaining time never goes negative while past deadlines read expired.
    @Test
    void saturatesAndNeverReturnsNegativeRemainingTime() {
        assertEquals(Long.MAX_VALUE, ReplicationRetry.deadlineNanos(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, ReplicationRetry.remainingNanos(Long.MAX_VALUE));
        assertTrue(ReplicationRetry.remainingNanos(Long.MIN_VALUE) >= 0L);
        assertTrue(ReplicationRetry.expired(System.nanoTime() - 1_000_000L));
    }

        /// A manual clock makes deadline arithmetic deterministic.
    @Test
    void manualClockBoundsDeadlineAndExpiry() {
        final var now = new java.util.concurrent.atomic.AtomicLong(1_000_000L);

        final long deadline = ReplicationRetry.deadlineNanos(500L, now::get);
        assertEquals(1_000_500L, deadline);
        assertEquals(500L, ReplicationRetry.remainingNanos(deadline, now::get));
        assertFalse(ReplicationRetry.expired(deadline, now::get));

        now.set(1_000_500L);
        assertEquals(0L, ReplicationRetry.remainingNanos(deadline, now::get));
        assertTrue(ReplicationRetry.expired(deadline, now::get));

        assertThrows(IllegalArgumentException.class, () -> ReplicationRetry.deadlineNanos(0L, now::get));
    }
}
