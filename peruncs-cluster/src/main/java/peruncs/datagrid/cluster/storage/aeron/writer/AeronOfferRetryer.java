package peruncs.datagrid.cluster.storage.aeron.writer;

import io.aeron.Publication;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.types.ReplicationRetry;

import java.util.concurrent.locks.LockSupport;

/// The bounded retry policy used by the writer's Aeron publications.
///
/// The helper reuses its buffer and idle strategy, so it belongs to one
/// publisher and must be called by that publisher's serialized write path.
final class AeronOfferRetryer {
    private final Offerer offerer;
    private final AeronReplicationConfiguration configuration;
    private final BackoffIdleStrategy idle = new BackoffIdleStrategy(1, 10, 1, 1_000_000);
    AeronOfferRetryer(final Offerer offerer, final AeronReplicationConfiguration configuration) {
        this.offerer = java.util.Objects.requireNonNull(offerer, "offerer");
        this.configuration = java.util.Objects.requireNonNull(configuration, "configuration");
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
        if (source == null || length < 0 || length > source.capacity()) {
            throw new IllegalArgumentException("invalid Aeron offer length");
        }
        return this.offerLoop(source, length);
    }

    private long offerLoop(final DirectBuffer source, final int length) {
        this.idle.reset();
        final long deadline = ReplicationRetry.deadlineNanos(this.configuration.offerTimeoutNanos());
        long backPressured = 0;
        long notConnected = 0;
        long adminActions = 0;
        long attempt = 0L;
        while (true) {
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
            if (ReplicationRetry.expired(deadline)) {
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
            /* Full-jitter spacing between attempts keeps concurrent writers from
             * retrying in lockstep after a shared back-pressure wave. The idle
             * strategy still governs the tight spin; this park only desynchronizes
             * successive attempts within the per-operation deadline above. */
            attempt++;
            LockSupport.parkNanos(
                    ReplicationRetry.fullJitterDelayNanos(attempt, 1_000L, 1_000_000L));
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
