package peruncs.datagrid.cluster.storage.aeron.writer;

import io.aeron.Publication;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.errors.WriterFencedException;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.config.AeronRetryPolicy;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

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
            assertThrows(IllegalStateException.class,
                    () -> retryer.offer(new UnsafeBuffer(new byte[64]), 64));
            attempts = attemptsCounter.get();
        }
        /* Each retry parks uniform in [0, cap); reaching 30 attempts would
         * require the jitter sum to stay under 20 ms, which is effectively
         * impossible, while an un-jittered loop would produce far more. */
        assertTrue(attempts < 30, "jitter must space retries, but observed %s attempts".formatted(attempts));
        assertTrue(attempts > 1, "the deadline must allow retries before it expires");
    }

    /// Verifies the ownership callback failure is reported as fencing, not timeout.
    @Test
    void ownershipLossIsReportedAsWriterFenced() {
        final AeronOfferRetryer retryer = new AeronOfferRetryer(
                (buffer, offset, length) -> Publication.BACK_PRESSURED,
                AeronReplicationConfiguration.defaults());

        final WriterFencedException failure = assertThrows(WriterFencedException.class,
                () -> retryer.offer(new UnsafeBuffer(new byte[64]), 64, () -> false));
        assertTrue(failure.getMessage().contains("lease lost"), failure::getMessage);
    }

    /// Back pressure releases the lease claim before the next publication attempt.
    @Test
    void claimsOwnershipSeparatelyForEachRetry() {
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger claims = new AtomicInteger();
        final AeronOfferRetryer retryer = new AeronOfferRetryer(
                (buffer, offset, length) -> attempts.incrementAndGet() < 3
                        ? Publication.BACK_PRESSURED : 42L,
                AeronReplicationConfiguration.defaults());
        final WriterLeaseGate gate = new WriterLeaseGate() {
            @Override
            public boolean isValid() { return true; }

            @Override
            public long offerUnderOwnership(final LongSupplier offer) {
                throw new AssertionError("the ungated overload must not be used");
            }

            @Override
            public long offerUnderOwnership(final OwnedOffer offer) {
                claims.incrementAndGet();
                return offer.offer(() -> true);
            }
        };
        assertEquals(42L, retryer.offerGated(new UnsafeBuffer(new byte[64]), 64, gate,
                Long.MAX_VALUE));
        assertEquals(3, attempts.get());
        assertEquals(attempts.get(), claims.get());
    }

    /// The lease's terminal budget can end retries before the general offer timeout.
    @Test
    void terminalBudgetOverridesGeneralOfferTimeout() {
        final AtomicLong now = new AtomicLong();
        final AtomicInteger attempts = new AtomicInteger();
        final AeronOfferRetryer retryer = new AeronOfferRetryer(
                (buffer, offset, length) -> {
                    attempts.incrementAndGet();
                    return Publication.BACK_PRESSURED;
                }, AeronReplicationConfiguration.defaults(), () -> now.getAndAdd(1_000_000L));
        assertThrows(IllegalStateException.class, () -> retryer.offerGated(
                new UnsafeBuffer(new byte[64]), 64, WriterLeaseGate.alwaysValid(), 1_000L));
        assertEquals(1, attempts.get());
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
