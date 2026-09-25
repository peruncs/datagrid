package peruncs.cluster.node.aeron;


import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

/// Verifies rejection-log level selection: shutdown interrupts stay quiet,
/// genuine watermark rejections warn.
class WatermarkCollectorTest {
    /// A rejection whose cause chain ends in an interrupt logs at DEBUG,
    /// matching the close-path contract (worker interrupted mid-wait).
    @Test
    void interruptedShutdownIsDebug() {
        final RuntimeException closed = new IllegalStateException(
                "Interrupted while waiting for Aeron retention",
                new InterruptedException("shutdown"));
        assertEquals(System.Logger.Level.DEBUG, WatermarkCollector.levelFor(closed));
    }

    /// A rejection with no interrupt in the chain keeps the WARNING level.
    @Test
    void genuineRejectionWarns() {
        final RuntimeException rejected = new IllegalStateException(
                "watermark outside the durable boundary");
        assertEquals(System.Logger.Level.WARNING, WatermarkCollector.levelFor(rejected));
    }

    /// An interrupt nested under arbitrary wrappers is still recognized.
    @Test
    void nestedInterruptIsDebug() {
        final RuntimeException wrapped = new IllegalStateException(
                "outer",
                new IllegalStateException("middle", new InterruptedException()));
        assertEquals(System.Logger.Level.DEBUG, WatermarkCollector.levelFor(wrapped));
    }
}
