package peruncs.cluster.storage.aeron.writer;

import io.aeron.DirectBufferVector;
import io.aeron.Publication;
import org.agrona.DirectBuffer;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.storage.ReplicationRetry;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.config.AeronRetryPolicy;

import java.util.Objects;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/// The bounded retry policy used by the writer's Aeron publications.
///
/// It belongs to one publisher and must be called by that publisher's
/// serialized write path. Each failed attempt is
/// spaced by a full-jitter delay derived from the configured jitter bounds, so
/// concurrent writers that share an outage do not retry in lockstep.
final class AeronOfferRetryer {
    private final Offerer offerer;
    private final AeronReplicationConfiguration configuration;
    private final LongSupplier clock;

    AeronOfferRetryer(final Offerer offerer, final AeronReplicationConfiguration configuration) {
        this(offerer, configuration, System::nanoTime);
    }

    /// Creates a retryer with an explicit monotonic clock for deterministic tests.
    AeronOfferRetryer(
            final Offerer offerer,
            final AeronReplicationConfiguration configuration,
            final LongSupplier clock
    ) {
        this.offerer = Objects.requireNonNull(offerer, "offerer");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    boolean supportsVectors() {
        return this.offerer.supportsVectors();
    }

    /// Offers until Aeron accepts the frame or its deadline expires.
    ///
    /// @throws ReplicationUnavailableException when the thread is interrupted, the
    ///                                       publication closes, or the frame is not
    ///                                       accepted before the deadline
    long offer(final DirectBuffer source, final int length) {
        if (source == null || length < 0 || length > source.capacity()) {
            throw new IllegalArgumentException("invalid Aeron offer length");
        }
        return this.offerLoop(source, null, length, this.configuration.offerTimeoutNanos());
    }

    /// Offers a reusable vector message until Aeron accepts it or its deadline expires.
    long offer(final DirectBufferVector[] vectors) {
        if (!this.offerer.supportsVectors()) throw new UnsupportedOperationException("vector offers are unsupported");
        final int length = DirectBufferVector.validateAndComputeLength(vectors);
        return this.offerLoop(null, vectors, length, this.configuration.offerTimeoutNanos());
    }

    private long offerLoop(final DirectBuffer source, final DirectBufferVector[] vectors,
                           final int length, final long timeoutNanos) {
        final AeronRetryPolicy policy = this.configuration.retryPolicy();
        final long deadline = ReplicationRetry.deadlineNanos(timeoutNanos, this.clock);
        long backPressured = 0;
        long notConnected = 0;
        long adminActions = 0;
        long attempt = 0L;
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                throw new ReplicationUnavailableException("interrupted while offering Aeron replication frame");
            }
            final long position = vectors == null
                    ? this.offerer.offer(source, 0, length)
                    : this.offerer.offer(vectors);
            if (position >= 0) return position;
            if (position == Publication.CLOSED || position == Publication.MAX_POSITION_EXCEEDED) {
                throw new ReplicationUnavailableException(
                        "Aeron publication failed: %s".formatted(position));
            }
            if (position == Publication.BACK_PRESSURED) backPressured++;
            else if (position == Publication.NOT_CONNECTED) notConnected++;
            else if (position == Publication.ADMIN_ACTION) adminActions++;
            else {
                /* Aeron adds result codes rarely; retrying an unknown value would
                 * hide a protocol/API change behind a misleading timeout. */
                throw new ReplicationUnavailableException(
                        "unknown Aeron publication result: %s".formatted(position));
            }
            if (ReplicationRetry.expired(deadline, this.clock)) {
                final String reason;
                if (position == Publication.BACK_PRESSURED) {
                    reason = "BACK_PRESSURED retries=%s".formatted(backPressured);
                } else if (position == Publication.NOT_CONNECTED) {
                    reason = "NOT_CONNECTED retries=%s".formatted(notConnected);
                } else {
                    reason = "ADMIN_ACTION retries=%s".formatted(adminActions);
                }
                throw new ReplicationUnavailableException(
                        "Aeron offer timed out: %s, connected=%s".formatted(reason, this.offerer.isConnected()));
            }
            /* Full jitter is the single pacing mechanism: it spreads retries of
             * writers that share an outage instead of letting them collide on
             * every back-pressure interval. The park never outlives the
             * operation deadline, so the timeout stays bounded. */
            attempt++;
            final long delay = ReplicationRetry.fullJitterDelayNanos(
                    attempt, policy.jitterBaseNanos(), policy.jitterCapNanos());
            LockSupport.parkNanos(Math.min(delay, ReplicationRetry.remainingNanos(deadline, this.clock)));
        }
    }

    /// Supplies one publication attempt to the retry loop.
    @FunctionalInterface
    interface Offerer {
        long offer(DirectBuffer buffer, int offset, int length);

        /// Returns whether this offerer supports Aeron's gathering offer.
        default boolean supportsVectors() {
            return false;
        }

        /// Offers one gathered message; implementations opt in with `supportsVectors()`.
        default long offer(final DirectBufferVector[] vectors) {
            throw new UnsupportedOperationException("vector offers are unsupported");
        }

        /// Returns the publication connectivity observed by the offerer. Test
        /// offerers may keep the default because they do not model a subscription.
        default boolean isConnected() {
            return true;
        }
    }
}
