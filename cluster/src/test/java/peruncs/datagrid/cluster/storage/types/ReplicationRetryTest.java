package peruncs.datagrid.cluster.storage.types;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.types.ReplicationRetry;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationRetryTest {
    @Test
    void rejectsUnboundedOrEmptyBudgets() {
        assertThrows(IllegalArgumentException.class, () -> ReplicationRetry.deadlineNanos(0L));
        assertThrows(IllegalArgumentException.class, () -> ReplicationRetry.deadlineNanos(-1L));
    }

    @Test
    void saturatesAndNeverReturnsNegativeRemainingTime() {
        assertEquals(Long.MAX_VALUE, ReplicationRetry.deadlineNanos(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, ReplicationRetry.remainingNanos(Long.MAX_VALUE));
        assertTrue(ReplicationRetry.remainingNanos(Long.MIN_VALUE) >= 0L);
        assertTrue(ReplicationRetry.expired(System.nanoTime() - 1_000_000L));
    }
}
