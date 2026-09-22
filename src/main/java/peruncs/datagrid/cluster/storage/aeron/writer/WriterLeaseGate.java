package peruncs.datagrid.cluster.storage.aeron.writer;

import peruncs.datagrid.cluster.errors.WriterFencedException;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/// Gates each replication-frame offer on continued writer-lease ownership.
///
/// Admission checks use [#isValid()], but a deposed writer can lose the lease
/// between that check and the Aeron offer when the offer retries under back
/// pressure. Every frame therefore runs each publication attempt
/// through [#offerUnderOwnership(OwnedOffer)] once per non-blocking attempt:
/// ownership is verified while holding the same interprocess lock used for acquisition. The
/// heartbeat is refreshed before the lock is released. The slow Archive
/// acknowledgement wait always runs outside that lock.
///
/// Only genuine fencing loss is reported as [WriterFencedException]. Gate
/// implementations must not translate back-pressure timeouts, closed
/// publications, interrupts, or IO failures into that type: recovery
/// diagnostics depend on the real failure category.
public interface WriterLeaseGate {
    /// Reports whether the writer lease is still current.
    ///
    /// @return `true` while this writer still owns a fresh lease
    boolean isValid();

    /// Maximum time a terminal marker may retry, below lease staleness in production.
    default long terminalOfferBudgetNanos() { return Long.MAX_VALUE; }

    /// Offers one terminal marker after a single ownership check.
    ///
    /// This overload cannot re-check ownership between publication retries and
    /// is therefore reserved for gates whose offer cannot pause long enough
    /// for a takeover. Paths that can retry must use
    /// [#offerUnderOwnership(OwnedOffer)].
    ///
    /// @param offer bounded Aeron offer returning the publication position
    /// @return Aeron position returned by the offer
    /// @throws WriterFencedException when the lease was lost before the offer
    long offerUnderOwnership(LongSupplier offer);

    /// Offers a terminal marker while passing the current ownership check into
    /// each bounded publication attempt.
    ///
    /// Production gates override this overload so a retry cannot continue after
    /// fencing. The default fails instead of silently degrading to the
    /// single-check [#offerUnderOwnership(LongSupplier)] overload, which would
    /// let a deposed writer offer a stale marker after its lease was stolen.
    ///
    /// @param offer offer operation receiving the per-attempt ownership check
    /// @return Aeron position returned by the offer
    /// @throws WriterFencedException         when the lease was lost
    /// @throws UnsupportedOperationException when the gate only supports
    ///                                       single-check offers
    default long offerUnderOwnership(final OwnedOffer offer) {
        Objects.requireNonNull(offer, "offer");
        throw new UnsupportedOperationException(
                "writer lease gate does not support per-attempt ownership checks");
    }

    /// Supplies a publication operation and its per-attempt ownership check.
    @FunctionalInterface
    interface OwnedOffer {
        long offer(BooleanSupplier stillOwner);
    }

    /// Creates a gate that always reports valid and offers directly.
    ///
    /// @return ungated marker gate for tests and lease-free topologies
    static WriterLeaseGate alwaysValid() {
        return new WriterLeaseGate() {
            @Override
            public boolean isValid() {
                return true;
            }

            @Override
            public long offerUnderOwnership(final LongSupplier offer) {
                return offer.getAsLong();
            }

            @Override
            public long offerUnderOwnership(final OwnedOffer offer) {
                return offer.offer(() -> true);
            }
        };
    }

    /// Creates a gate from a validity supplier with direct offers.
    ///
    /// @param valid validity supplier
    /// @return gate that checks validity but does not serialize offers
    static WriterLeaseGate of(final BooleanSupplier valid) {
        Objects.requireNonNull(valid, "valid");
        return new WriterLeaseGate() {
            @Override
            public boolean isValid() {
                return valid.getAsBoolean();
            }

            @Override
            public long offerUnderOwnership(final LongSupplier offer) {
                if (!valid.getAsBoolean()) {
                    throw new WriterFencedException("writer fencing lease lost before commit; restart required");
                }
                return offer.getAsLong();
            }

            @Override
            public long offerUnderOwnership(final OwnedOffer offer) {
                if (!valid.getAsBoolean()) {
                    throw new WriterFencedException("writer fencing lease lost before commit; restart required");
                }
                return offer.offer(valid);
            }
        };
    }
}
