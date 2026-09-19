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

        /// Full-jitter backoff stays inside the exponential cap and rejects
    /// non-positive inputs.
    @Test
    void fullJitterDelayStaysWithinTheExponentialCap() {
        for (long attempt = 1L; attempt <= 8L; attempt++) {
            final long delay = ReplicationRetry.fullJitterDelayNanos(attempt, 10L, 1_000L);
            final long exponential = Math.min(10L << Math.min(attempt - 1L, 62L), 1_000L);
            assertTrue(delay >= 0L, "a jitter delay must never be negative");
            assertTrue(delay <= exponential,
                    "attempt %s produced %s beyond its %s bound".formatted(attempt, delay, exponential));
        }
        assertThrows(IllegalArgumentException.class,
                () -> ReplicationRetry.fullJitterDelayNanos(0L, 10L, 100L));
        assertThrows(IllegalArgumentException.class,
                () -> ReplicationRetry.fullJitterDelayNanos(1L, 0L, 100L));
        assertThrows(IllegalArgumentException.class,
                () -> ReplicationRetry.fullJitterDelayNanos(1L, 10L, 0L));
    }
}
