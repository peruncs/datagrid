package peruncs.datagrid.cluster.storage.aeron.writer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/// Smoke tests the complete publisher staging benchmark without machine-specific limits.
class AeronPublisherBenchmarkTest {
    @Test
    void measuresCopiesOffersAndAllocation() {
        final AeronPublisherBenchmark.Result result = AeronPublisherBenchmark.measure(
                10_000, 1_024, 4, 2, 5);

        assertEquals(10, result.chunkCount());
        assertEquals(10_000, result.copiedBytesPerTransaction());
        assertTrue(result.offeredBytesPerTransaction() > result.copiedBytesPerTransaction());
        assertTrue(result.nanosecondsPerTransaction() > 0);
        assertTrue(result.mebibytesPerSecond() > 0);
        /* -1 is the documented value when the VM does not expose allocation counters. */
        assertTrue(result.allocatedBytesPerTransaction() >= -1);
    }

    @Test
    void rejectsInvalidMeasurementParameters() {
        assertThrows(IllegalArgumentException.class, () -> AeronPublisherBenchmark.measure(0, 1_024, 4, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> AeronPublisherBenchmark.measure(1_024, 0, 4, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> AeronPublisherBenchmark.measure(1_024, 1_024, 0, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> AeronPublisherBenchmark.measure(1_024, 1_024, 4, 0, 0));
    }
}
