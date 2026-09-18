package peruncs.datagrid.cluster.storage.aeron.wire;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Smoke checks the benchmark's accounting without imposing machine-specific performance limits.
class AeronEnvelopeBenchmarkTest {
    /// Verifies the benchmark copies the expected chunked bytes and reports positive throughput timing.
    @Test
    void measuresChunkedCopyAndThroughput() {
        final AeronEnvelopeBenchmark.BenchmarkResult result = AeronEnvelopeBenchmark.measure(
                UUID.randomUUID(), 1, 10_000, 1_024, 2, 5);

        assertEquals(10, result.chunkCount());
        assertEquals(10_000, result.copiedBytesPerTransaction());
        assertTrue(result.nanosecondsPerTransaction() > 0);
        assertTrue(result.mebibytesPerSecond() > 0);
    }

    /// Verifies measuring rejects invalid payload and iteration parameters.
    @Test
    void rejectsInvalidMeasurementParameters() {
        assertThrows(IllegalArgumentException.class, () -> AeronEnvelopeBenchmark.measure(
                UUID.randomUUID(), 1, 0, 1_024, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> AeronEnvelopeBenchmark.measure(
                UUID.randomUUID(), 1, 1, 1_024, 0, 0));
    }
}
