package peruncs.datagrid.cluster.storage.aeron.writer;

import io.aeron.Publication;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies that terminal publication retries stop as soon as writer ownership is lost.
class AeronOfferRetryerOwnershipTest {
    /// A lease flip after back pressure must prevent the next publication attempt.
    @Test
    void stopsBeforeRetryAfterOwnershipLoss() {
        final AtomicInteger attempts = new AtomicInteger();
        final AeronOfferRetryer retryer = new AeronOfferRetryer(
                (buffer, offset, length) -> {
                    attempts.incrementAndGet();
                    return Publication.BACK_PRESSURED;
                }, AeronReplicationConfiguration.defaults());
        final AtomicInteger checks = new AtomicInteger();

        final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> retryer.offer(
                new UnsafeBuffer(new byte[64]), 64, () -> checks.incrementAndGet() == 1));

        assertTrue(failure.getMessage().contains("lease lost"), failure::getMessage);
        assertEquals(1, attempts.get(), "ownership loss must prevent a second publication attempt");
        assertEquals(2, checks.get(), "ownership is checked before the initial and retry attempts");
    }
}
