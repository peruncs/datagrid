package peruncs.cluster.storage.aeron.writer;

import io.aeron.Publication;
import org.agrona.DirectBuffer;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.errors.WriterFencedException;
import peruncs.cluster.storage.ReplicationRetry;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.config.AeronRetryPolicy;

import java.util.Objects;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

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

    /// Offers until Aeron accepts the frame, the deadline expires, or ownership is lost.
    ///
    /// The ownership callback is evaluated before every publication attempt. It is
    /// intentionally part of this loop rather than a one-time caller check: a
    /// terminal marker must not be retried after its writer lease has been fenced.
    ///
    /// @throws WriterFencedException         when `stillOwner` reports that the lease was lost
    /// @throws ReplicationUnavailableException when the thread is interrupted, the
    ///                                       publication closes, or the frame is not
    ///                                       accepted before the deadline
    long offer(final DirectBuffer source, final int length, final BooleanSupplier stillOwner) {
        if (source == null || length < 0 || length > source.capacity()) {
            throw new IllegalArgumentException("invalid Aeron offer length");
        }
        Objects.requireNonNull(stillOwner, "stillOwner");
        return this.offerLoop(() -> null, stillOwner,
                () -> this.offerer.offer(source, 0, length),
                this.configuration.offerTimeoutNanos());
    }

    /// Claims lease ownership for one non-blocking Aeron attempt at a time.
    long offerGated(final DirectBuffer source, final int length, final WriterLeaseGate gate,
                    final long budgetNanos) {
        if (source == null || length < 0 || length > source.capacity()) {
            throw new IllegalArgumentException("invalid Aeron offer length");
        }
        Objects.requireNonNull(gate, "gate");
        if (budgetNanos <= 0) throw new IllegalArgumentException("offer budget must be positive");
        return this.offerLoop(gate::terminalFailure, () -> true,
                () -> gate.offerUnderOwnership(owner -> {
                    if (!owner.getAsBoolean()) {
                        throw new WriterFencedException("writer fencing lease lost during Aeron offer retry");
                    }
                    return this.offerer.offer(source, 0, length);
                }), Math.min(budgetNanos, this.configuration.offerTimeoutNanos()));
    }

    private long offerLoop(final Supplier<RuntimeException> terminalFailure,
                           final BooleanSupplier stillOwner,
                           final LongSupplier attemptOffer, final long timeoutNanos) {
        final AeronRetryPolicy policy = this.configuration.retryPolicy();
        final long deadline = ReplicationRetry.deadlineNanos(timeoutNanos, this.clock);
        long backPressured = 0;
        long notConnected = 0;
        long adminActions = 0;
        long attempt = 0L;
        while (true) {
            if (!stillOwner.getAsBoolean()) {
                throw new WriterFencedException("writer fencing lease lost during Aeron offer retry");
            }
            /* A terminal driver failure must not be retried to the full offer
             * deadline: Aeron only surfaces it through NOT_CONNECTED, so the
             * wedged offer would otherwise park until the deadline expires. */
            final RuntimeException terminal = terminalFailure.get();
            if (terminal != null) {
                throw terminal;
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new ReplicationUnavailableException("interrupted while offering Aeron replication frame");
            }
            final long position = attemptOffer.getAsLong();
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

        /// Returns the publication connectivity observed by the offerer. Test
        /// offerers may keep the default because they do not model a subscription.
        default boolean isConnected() {
            return true;
        }
    }
}
