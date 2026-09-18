package peruncs.datagrid.cluster.storage.aeron.writer;

import io.aeron.Publication;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.config.AeronRetryPolicy;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/// The offer retry loop honors its deadline on a manual clock and the policy
/// defaults preserve the historical pacing.
class AeronOfferRetryerTest {
    /// Verifies persistent back pressure fails with an offer timeout once the deadline expires.
    @Test
    void timesOutOnPersistentBackPressure() {
        final var now = new AtomicLong(1_000_000L);
        final AeronOfferRetryer retryer = new AeronOfferRetryer(
                (buffer, offset, length) -> Publication.BACK_PRESSURED,
                AeronReplicationConfiguration.defaults(),
                () -> now.getAndAdd(1_000_000_000L));

        final var failure = assertThrows(IllegalStateException.class,
                () -> retryer.offer(new UnsafeBuffer(new byte[64]), 64));
        assertTrue(failure.getMessage().startsWith("Aeron offer timed out"),
                () -> "unexpected failure: " + failure.getMessage());
    }

    /// Verifies an immediately accepted offer returns its position without parking.
    @Test
    void succeedsWithoutParkingWhenAccepted() {
        final AeronOfferRetryer retryer = new AeronOfferRetryer(
                (buffer, offset, length) -> 42L,
                AeronReplicationConfiguration.defaults());

        assertEquals(42L, retryer.offer(new UnsafeBuffer(new byte[64]), 64));
    }

    /// Verifies the default retry policy preserves the historical idle, jitter, and probe pacing.
    @Test
    void policyDefaultsPreserveHistoricalPacing() {
        final AeronRetryPolicy policy = AeronRetryPolicy.Default();

        assertEquals(1, policy.idleMaxSpins());
        assertEquals(10, policy.idleMaxYields());
        assertEquals(1L, policy.idleMinParkNanos());
        assertEquals(1_000_000L, policy.idleMaxParkNanos());
        assertEquals(1_000L, policy.jitterBaseNanos());
        assertEquals(1_000_000L, policy.jitterCapNanos());
        assertEquals(10_000_000L, policy.archiveProbeDelayNanos());
        assertEquals(1_000_000L, policy.catalogProbeInitialDelayNanos());
        assertEquals(100_000_000L, policy.catalogProbeMaxDelayNanos());
    }

    /// Verifies the retry policy rejects non-positive bounds and inverted park limits.
    @Test
    void policyRejectsNonPositiveBounds() {
        assertThrows(IllegalArgumentException.class, () -> new AeronRetryPolicy(
                1, 10, 1L, 1_000_000L, 0L, 1_000_000L, 10_000_000L, 1_000_000L, 100_000_000L));
        assertThrows(IllegalArgumentException.class, () -> new AeronRetryPolicy(
                1, 10, 1_000_000L, 1L, 1_000L, 1_000_000L, 10_000_000L, 1_000_000L, 100_000_000L));
    }
}
