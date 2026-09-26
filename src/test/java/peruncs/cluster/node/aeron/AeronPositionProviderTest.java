package peruncs.cluster.node.aeron;

import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.ReplicationPositionUnavailableException;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies that the Aeron provider exposes only an initialized writer boundary.
class AeronPositionProviderTest {
    private static final UUID ID = UUID.randomUUID();

    @Test
    void readersCannotInventAWriterBoundary() {
        final AeronPositionProvider provider = provider(false, true, () -> 1L);

        assertThrows(ReplicationPositionUnavailableException.class, provider::latest);
    }

    @Test
    void fencingFailureIsPreservedAsTheCause() {
        final RuntimeException cause = new IllegalStateException("lease lost");
        final AeronPositionProvider provider = provider(true, true, () -> {
            throw cause;
        });

        final ReplicationPositionUnavailableException failure =
                assertThrows(ReplicationPositionUnavailableException.class, provider::latest);
        assertSame(cause, failure.getCause());
    }

    private static AeronPositionProvider provider(final boolean writer, final boolean initialized,
                                                  final java.util.function.LongSupplier fencingToken) {
        return new AeronPositionProvider(() -> writer, () -> initialized, () -> {
        }, () -> new AeronWriterRecoveryBoundary(7L, 8L, 9L), () -> ID, () -> ID, () -> ID,
                () -> 1L, fencingToken);
    }
}
