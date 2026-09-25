package peruncs.cluster.node.aeron;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies writer-only admission and sequence synchronization at the Aeron distribution gate.
class AeronDistributionGateTest {
    @Test
    void nonWriterCannotAdvanceTheMessageIndex() {
        final AeronDistributionGate gate = new AeronDistributionGate(() -> false, ignored -> {
        });

        assertThrows(IllegalStateException.class, () -> gate.messageIndex(1L));
    }

    @Test
    void writerIndexAdvancesTheNextSequence() {
        final AtomicLong next = new AtomicLong();
        final AeronDistributionGate gate = new AeronDistributionGate(() -> true, next::set);

        gate.messageIndex(41L);

        assertEquals(41L, gate.messageIndex());
        assertEquals(42L, next.get());
    }
}
