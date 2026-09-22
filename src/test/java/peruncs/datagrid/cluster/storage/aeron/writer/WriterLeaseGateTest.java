package peruncs.datagrid.cluster.storage.aeron.writer;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.errors.WriterFencedException;

import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies the writer lease gate contract that guards terminal-marker offers.
class WriterLeaseGateTest {
    /// A gate that only implements the single-check overload must fail loudly
    /// instead of silently treating every retry as still owned.
    @Test
    void ownedOfferWithoutOverrideIsRejected() {
        final WriterLeaseGate singleCheckOnly = new WriterLeaseGate() {
            @Override
            public boolean isValid() {
                return true;
            }

            @Override
            public long offerUnderOwnership(final LongSupplier offer) {
                return offer.getAsLong();
            }
        };

        assertThrows(UnsupportedOperationException.class,
                () -> singleCheckOnly.offerUnderOwnership(stillOwner -> 1L),
                "a single-check gate must not accept per-attempt ownership offers");
    }

    /// A lost lease is fencing loss for both overloads.
    @Test
    void invalidValiditySupplierThrowsWriterFencedException() {
        final WriterLeaseGate gate = WriterLeaseGate.of(() -> false);

        assertThrows(WriterFencedException.class, () -> gate.offerUnderOwnership(() -> 1L));
        assertThrows(WriterFencedException.class, () -> gate.offerUnderOwnership(stillOwner -> 1L));
    }

    /// The always-valid gate must pass the ownership check through unchanged.
    @Test
    void alwaysValidGateOffersBothOverloads() {
        final WriterLeaseGate gate = WriterLeaseGate.alwaysValid();

        assertEquals(7L, gate.offerUnderOwnership(() -> 7L));
        assertEquals(9L, gate.offerUnderOwnership(stillOwner -> stillOwner.getAsBoolean() ? 9L : -1L));
    }
}
