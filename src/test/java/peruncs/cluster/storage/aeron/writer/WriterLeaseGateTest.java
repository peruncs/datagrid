package peruncs.cluster.storage.aeron.writer;

import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.WriterFencedException;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies the writer lease gate contract that guards terminal-marker offers.
class WriterLeaseGateTest {
    /// A gate that only implements the single-check overload must fail loudly

    /// A lost lease is fencing loss for both overloads.
    @Test
    void invalidValiditySupplierThrowsWriterFencedException() {
        final WriterLeaseGate gate = WriterLeaseGate.of(() -> false);

        assertThrows(WriterFencedException.class, () -> gate.offerUnderOwnership(stillOwner -> 1L));
    }

    /// The always-valid gate must pass the ownership check through unchanged.
    @Test
    void alwaysValidGatePassesOwnershipThrough() {
        final WriterLeaseGate gate = WriterLeaseGate.alwaysValid();

        assertEquals(9L, gate.offerUnderOwnership(stillOwner -> stillOwner.getAsBoolean() ? 9L : -1L));
    }
}
