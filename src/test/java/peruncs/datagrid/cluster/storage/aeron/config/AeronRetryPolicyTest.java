package peruncs.datagrid.cluster.storage.aeron.config;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/// Pins the retry-policy bounds that keep Aeron idle and probe pacing sane.
class AeronRetryPolicyTest {
        /// Verifies that defaults are accepted and produce a backoff strategy.
    @Test
    void defaultsAreValidAndExposeAnIdleStrategy() {
        final AeronRetryPolicy policy = AeronRetryPolicy.defaults();

        assertEquals(1, policy.idleMaxSpins());
        assertEquals(10, policy.idleMaxYields());
        assertInstanceOf(BackoffIdleStrategy.class, policy.idleStrategy());
    }

        /// Verifies rejection of a jitter base above its cap.
    @Test
    void rejectsJitterBaseAboveCap() {
        assertThrows(IllegalArgumentException.class, () -> new AeronRetryPolicy(
                1, 10, 1L, 1_000_000L,
                2_000_000L, 1_000L,
                10_000_000L,
                1_000_000L, 100_000_000L));
    }

        /// Verifies rejection of negative spin or yield counts.
    @Test
    void rejectsNegativeIdleCounts() {
        assertThrows(IllegalArgumentException.class, () -> new AeronRetryPolicy(
                -1, 10, 1L, 1_000_000L,
                1_000L, 1_000_000L,
                10_000_000L,
                1_000_000L, 100_000_000L));
        assertThrows(IllegalArgumentException.class, () -> new AeronRetryPolicy(
                1, -1, 1L, 1_000_000L,
                1_000L, 1_000_000L,
                10_000_000L,
                1_000_000L, 100_000_000L));
    }

        /// Verifies rejection of inverted or non-positive park bounds.
    @Test
    void rejectsInvalidParkBounds() {
        assertThrows(IllegalArgumentException.class, () -> new AeronRetryPolicy(
                1, 10, 0L, 1_000_000L,
                1_000L, 1_000_000L,
                10_000_000L,
                1_000_000L, 100_000_000L));
        assertThrows(IllegalArgumentException.class, () -> new AeronRetryPolicy(
                1, 10, 2_000_000L, 1_000_000L,
                1_000L, 1_000_000L,
                10_000_000L,
                1_000_000L, 100_000_000L));
    }

        /// Verifies rejection of non-positive jitter bounds.
    @Test
    void rejectsNonPositiveJitterBounds() {
        assertThrows(IllegalArgumentException.class, () -> new AeronRetryPolicy(
                1, 10, 1L, 1_000_000L,
                0L, 1_000_000L,
                10_000_000L,
                1_000_000L, 100_000_000L));
    }

        /// Verifies rejection of inverted or non-positive probe delays.
    @Test
    void rejectsInvalidProbeDelays() {
        assertThrows(IllegalArgumentException.class, () -> new AeronRetryPolicy(
                1, 10, 1L, 1_000_000L,
                1_000L, 1_000_000L,
                0L,
                1_000_000L, 100_000_000L));
        assertThrows(IllegalArgumentException.class, () -> new AeronRetryPolicy(
                1, 10, 1L, 1_000_000L,
                1_000L, 1_000_000L,
                10_000_000L,
                200_000_000L, 100_000_000L));
    }
}
