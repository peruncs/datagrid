package peruncs.cluster.storage.aeron.writer;

import peruncs.cluster.errors.WriterFencedException;

import java.util.Objects;
import java.util.function.BooleanSupplier;

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

    /// Returns the terminal transport failure that must stop offer retries, or `null`.
    ///
    /// Production gates surface the Aeron MediaDriver failure here so a wedged
    /// offer fails fast with its recorded cause instead of retrying
    /// NOT_CONNECTED until the deadline expires.
    ///
    /// @return already-typed terminal failure, or `null` while the transport is healthy
    default RuntimeException terminalFailure() { return null; }

    /// Offers a terminal marker while passing the current ownership check into
    /// each bounded publication attempt.
    ///
    /// Production gates implement this method so a retry cannot continue after
    /// fencing; each attempt claims the interprocess lease lock separately.
    ///
    /// @param offer offer operation receiving the per-attempt ownership check
    /// @return Aeron position returned by the offer
    /// @throws WriterFencedException         when the lease was lost
    /// @throws UnsupportedOperationException when the gate cannot serialize offers
    ///                                       with the interprocess lock
    long offerUnderOwnership(final OwnedOffer offer);

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
            public long offerUnderOwnership(final OwnedOffer offer) {
                if (!valid.getAsBoolean()) {
                    throw new WriterFencedException("writer fencing lease lost before commit; restart required");
                }
                return offer.offer(valid);
            }
        };
    }
}
