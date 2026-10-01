package peruncs.cluster.api;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies that inconsistent retry pacing fails at configuration time, not on the first retry.
class RetryPacingTest {
    private static NodeConfig.RetryPacing pacing(final Duration jitterBase, final Duration jitterCap,
                                                 final Duration catalogInitial, final Duration catalogMax) {
        return new NodeConfig.RetryPacing(Duration.ofMillis(1), jitterBase, jitterCap, Duration.ofMillis(10),
                catalogInitial, catalogMax);
    }

    @Test
    void theDefaultsAreConsistent() {
        assertDoesNotThrow(() -> NodeConfig.RetryPacing.DEFAULT.toString());
    }

    @Test
    void aJitterBaseAboveItsCapIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> pacing(
                Duration.ofMillis(2), Duration.ofMillis(1), Duration.ofMillis(1), Duration.ofMillis(100)));
    }

    @Test
    void aCatalogProbeStartAboveItsMaximumIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> pacing(
                Duration.ofNanos(1_000), Duration.ofMillis(1), Duration.ofMillis(200), Duration.ofMillis(100)));
    }

    @Test
    void equalBoundsAreAccepted() {
        assertDoesNotThrow(() -> pacing(
                Duration.ofMillis(1), Duration.ofMillis(1), Duration.ofMillis(5), Duration.ofMillis(5)));
    }
}
