package peruncs.datagrid.cluster.storage.aeron.writer;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/// Gates terminal-marker offers on continued writer-lease ownership.
///
/// Admission checks use [#isValid()], but a deposed writer can lose the lease
/// between that check and the Aeron offer when the offer retries under back
/// pressure. Commit and abort markers must therefore run their bounded offer
/// through [#offerUnderOwnership(OwnedOffer)]: ownership is verified while
/// holding the same interprocess lock used for acquisition, and the retry loop
/// receives a callback that is evaluated before every publication attempt. The
/// heartbeat is refreshed before the lock is released. The slow Archive
/// acknowledgement wait always runs outside that lock.
public interface WriterLeaseGate {
    /// Reports whether the writer lease is still current.
    ///
    /// @return `true` while this writer still owns a fresh lease
    boolean isValid();

    /// Offers one terminal marker under continued ownership.
    ///
    /// @param offer bounded Aeron offer returning the publication position
    /// @return Aeron position returned by the offer
    /// @throws IllegalStateException when the lease was lost before the offer
    long offerUnderOwnership(LongSupplier offer);

    /// Offers a terminal marker while passing the current ownership check into
    /// each bounded publication attempt.
    ///
    /// The legacy zero-argument overload remains the compatibility seam for
    /// simple test gates. Production gates override this overload so retries
    /// cannot continue after fencing.
    ///
    /// @param offer offer operation receiving the per-attempt ownership check
    /// @return Aeron position returned by the offer
    default long offerUnderOwnership(final OwnedOffer offer) {
        Objects.requireNonNull(offer, "offer");
        return this.offerUnderOwnership(() -> offer.offer(() -> true));
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
        return new WriterLeaseGate() {
            @Override
            public boolean isValid() {
                return valid.getAsBoolean();
            }

            @Override
            public long offerUnderOwnership(final LongSupplier offer) {
                if (!valid.getAsBoolean()) {
                    throw new IllegalStateException("writer fencing lease lost before commit; restart required");
                }
                return offer.getAsLong();
            }

            @Override
            public long offerUnderOwnership(final OwnedOffer offer) {
                if (!valid.getAsBoolean()) {
                    throw new IllegalStateException("writer fencing lease lost before commit; restart required");
                }
                return offer.offer(valid);
            }
        };
    }
}
