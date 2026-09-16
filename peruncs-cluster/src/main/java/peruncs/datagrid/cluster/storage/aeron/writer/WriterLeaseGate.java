package peruncs.datagrid.cluster.storage.aeron.writer;

import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/// Gates terminal-marker offers on continued writer-lease ownership.
///
/// Admission checks use [#isValid()], but a deposed writer can lose the lease
/// between that check and the Aeron offer when the offer retries under back
/// pressure. Commit and abort markers must therefore run their bounded offer
/// through [#offerUnderOwnership(LongSupplier)]: ownership is verified while
/// holding the same interprocess lock used for acquisition, the offer runs,
/// and the heartbeat is refreshed before the lock is released. The slow
/// Archive acknowledgement wait always runs outside that lock.
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
        };
    }
}
