package peruncs.cluster.storage.aeron.writer;

import io.aeron.Publication;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.config.AeronRetryPolicy;

import java.util.concurrent.atomic.AtomicInteger;
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

        final var failure = assertThrows(ReplicationUnavailableException.class,
                () -> retryer.offer(new UnsafeBuffer(new byte[64]), 64));
        assertTrue(failure.getMessage().startsWith("Aeron offer timed out"),
                () -> "unexpected failure: " + failure.getMessage());
    }

    /// A closed publication or an exhausted position space is final: no retry, no timeout wait.
    @Test
    void closedAndMaxPositionExceededFailImmediately() {
        for (final long result : new long[]{Publication.CLOSED, Publication.MAX_POSITION_EXCEEDED}) {
            final AtomicInteger offers = new AtomicInteger();
            final AeronOfferRetryer retryer = new AeronOfferRetryer((buffer, offset, length) -> {
                offers.incrementAndGet();
                return result;
            }, AeronReplicationConfiguration.defaults());
            final var failure = assertThrows(ReplicationUnavailableException.class,
                    () -> retryer.offer(new UnsafeBuffer(new byte[64]), 64));
            assertTrue(failure.getMessage().startsWith("Aeron publication failed"), failure.getMessage());
            assertEquals(1, offers.get(), "a final result must not be retried");
        }
    }

    /// A subscriber that is not connected yet and a pending admin action are retried until the deadline.
    @Test
    void notConnectedAndAdminActionAreRetriedUntilTheDeadline() {
        for (final long result : new long[]{Publication.NOT_CONNECTED, Publication.ADMIN_ACTION}) {
            final var now = new AtomicLong(1_000_000L);
            final AeronOfferRetryer retryer = new AeronOfferRetryer(
                    (buffer, offset, length) -> result, AeronReplicationConfiguration.defaults(),
                    () -> now.getAndAdd(1_000_000_000L));
            final var failure = assertThrows(ReplicationUnavailableException.class,
                    () -> retryer.offer(new UnsafeBuffer(new byte[64]), 64));
            assertTrue(failure.getMessage().startsWith("Aeron offer timed out"), failure.getMessage());
            assertTrue(failure.getMessage().contains(result == Publication.NOT_CONNECTED
                    ? "NOT_CONNECTED" : "ADMIN_ACTION"), failure.getMessage());
        }
    }

    /// A result code this code does not know is reported as such instead of hidden behind a timeout.
    @Test
    void anUnknownPublicationResultFailsFast() {
        final AeronOfferRetryer retryer = new AeronOfferRetryer(
                (buffer, offset, length) -> -99L, AeronReplicationConfiguration.defaults());
        final var failure = assertThrows(ReplicationUnavailableException.class,
                () -> retryer.offer(new UnsafeBuffer(new byte[64]), 64));
        assertTrue(failure.getMessage().contains("unknown Aeron publication result"), failure.getMessage());
    }

    /// Verifies an immediately accepted offer returns its position without parking.
    @Test
    void succeedsWithoutParkingWhenAccepted() {
        final AeronOfferRetryer retryer = new AeronOfferRetryer(
                (buffer, offset, length) -> 42L,
                AeronReplicationConfiguration.defaults());

        assertEquals(42L, retryer.offer(new UnsafeBuffer(new byte[64]), 64));
    }

    /// Full-jitter spacing must keep a persistent back-pressure loop from
    /// hammering the publication: with a millisecond-scale jitter cap, a short
    /// deadline allows only a handful of retries. Without jitter the same
    /// deadline admits tens of thousands of attempts.
    @Test
    void jitterSpacesPersistentBackPressureRetries() {
        final int attempts;
        {
            final var attemptsCounter = new AtomicInteger();
            final var policy = new AeronRetryPolicy(
                    1, 10, 1L, 1_000_000L,
                    2_000_000L, 4_000_000L,
                    10_000_000L, 1_000_000L, 100_000_000L);
            final var configuration = AeronReplicationConfiguration.builder()
                    .termLength(64 * 1024).chunkSize(256).maxTransactionBytes(1024)
                    .offerTimeoutNanos(20_000_000L)
                    .retryPolicy(policy)
                    .build();
            final AeronOfferRetryer retryer = new AeronOfferRetryer(
                    (buffer, offset, length) -> {
                        attemptsCounter.incrementAndGet();
                        return Publication.BACK_PRESSURED;
                    }, configuration);
            assertThrows(ReplicationUnavailableException.class,
                    () -> retryer.offer(new UnsafeBuffer(new byte[64]), 64));
            attempts = attemptsCounter.get();
        }
        /* Each retry parks uniform in [0, cap); reaching 30 attempts would
         * require the jitter sum to stay under 20 ms, which is effectively
         * impossible, while an un-jittered loop would produce far more. */
        assertTrue(attempts < 30, "jitter must space retries, but observed %s attempts".formatted(attempts));
        assertTrue(attempts > 1, "the deadline must allow retries before it expires");
    }


    /// Verifies the default retry policy preserves the historical idle, jitter, and probe pacing.
    @Test
    void policyDefaultsPreserveHistoricalPacing() {
        final AeronRetryPolicy policy = AeronRetryPolicy.defaults();

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
