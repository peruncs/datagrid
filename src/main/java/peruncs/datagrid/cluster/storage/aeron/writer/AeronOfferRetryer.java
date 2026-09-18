package peruncs.datagrid.cluster.storage.aeron.writer;

import io.aeron.Publication;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.types.ReplicationRetry;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/// The bounded retry policy used by the writer's Aeron publications.
///
/// The helper reuses its buffer and idle strategy, so it belongs to one
/// publisher and must be called by that publisher's serialized write path.
final class AeronOfferRetryer {
    private final Offerer offerer;
    private final AeronReplicationConfiguration configuration;
    private final LongSupplier clock;
    private final BackoffIdleStrategy idle;

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
        final var policy = this.configuration.retryPolicy();
        this.idle = new BackoffIdleStrategy(
                policy.idleMaxSpins(), policy.idleMaxYields(),
                policy.idleMinParkNanos(), policy.idleMaxParkNanos());
    }

        /// Offers the first `length` bytes of a buffer until Aeron accepts them
    /// or the configured deadline expires.
    ///
    /// The buffer is read synchronously and is not retained after this method
    /// returns. The caller must keep it valid and unchanged for the duration of
    /// the call. No heap copy is made. The helper is not thread-safe.
    ///
    /// @param source buffer containing the frame to offer
    /// @param length number of bytes to offer, starting at offset zero
    /// @return the Aeron publication position
    /// @throws IllegalArgumentException if `source` is null or the length
    ///                                  is outside the buffer capacity
    /// @throws IllegalStateException    if the publication closes, exceeds its
    ///                                  maximum position, or does not accept the frame before timeout
    long offer(final DirectBuffer source, final int length) {
        return this.offer(source, length, () -> true);
    }

    /// Offers until Aeron accepts the frame, the deadline expires, or ownership is lost.
    ///
    /// The ownership callback is evaluated before every publication attempt. It is
    /// intentionally part of this loop rather than a one-time caller check: a
    /// terminal marker must not be retried after its writer lease has been fenced.
    long offer(final DirectBuffer source, final int length, final BooleanSupplier stillOwner) {
        if (source == null || length < 0 || length > source.capacity()) {
            throw new IllegalArgumentException("invalid Aeron offer length");
        }
        return this.offerLoop(source, length, Objects.requireNonNull(stillOwner, "stillOwner"));
    }

    private long offerLoop(final DirectBuffer source, final int length, final BooleanSupplier stillOwner) {
        this.idle.reset();
        final long deadline = ReplicationRetry.deadlineNanos(this.configuration.offerTimeoutNanos(), this.clock);
        long backPressured = 0;
        long notConnected = 0;
        long adminActions = 0;
        long attempt = 0L;
        while (true) {
            if (!stillOwner.getAsBoolean()) {
                throw new IllegalStateException("writer fencing lease lost during Aeron offer retry");
            }
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while offering Aeron replication frame");
            }
            final long position = this.offerer.offer(source, 0, length);
            if (position >= 0) return position;
            if (position == Publication.CLOSED || position == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("Aeron publication failed: %s".formatted(position));
            }
            if (position == Publication.BACK_PRESSURED) backPressured++;
            else if (position == Publication.NOT_CONNECTED) notConnected++;
            else if (position == Publication.ADMIN_ACTION) adminActions++;
            else {
                /* Aeron adds result codes rarely; retrying an unknown value would
                 * hide a protocol/API change behind a misleading timeout. */
                throw new IllegalStateException("unknown Aeron publication result: %s".formatted(position));
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
                throw new IllegalStateException("Aeron offer timed out: %s, connected=%s".formatted(reason, this.offerer.isConnected()));
            }
            /* BackoffIdleStrategy is the single pacing mechanism. A second
             * explicit park compounded latency on every retry and made the
             * configured offer deadline much less predictable. */
            attempt++;
            this.idle.idle();
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
