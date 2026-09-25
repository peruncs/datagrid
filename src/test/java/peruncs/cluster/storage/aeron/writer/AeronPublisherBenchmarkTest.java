package peruncs.cluster.storage.aeron.writer;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.*;

/// Smoke tests the complete publisher staging benchmark without machine-specific limits.
class AeronPublisherBenchmarkTest {
    /// Verifies the benchmark stages the expected copies and offers while reporting throughput and allocation.
    @Test
    void measuresCopiesOffersAndAllocation() {
        final AeronPublisherBenchmark.Result result = AeronPublisherBenchmark.measure(
                10_000, 1_024, 4, 2, 5);

        assertEquals(10, result.chunkCount());
        assertEquals(10_000, result.copiedBytesPerTransaction());
        assertTrue(result.offeredBytesPerTransaction() > result.copiedBytesPerTransaction());
        assertTrue(result.nanosecondsPerTransaction() > 0);
        assertTrue(result.mebibytesPerSecond() > 0);
        /* -1 is only valid when the VM hides allocation counters; on a
         * supporting VM a negative value would mean the counter regressed, so
         * assert the branch the runtime actually takes. */
        if (threadAllocationSupported()) {
            assertTrue(result.allocatedBytesPerTransaction() >= 0,
                    "allocation counters are supported, so usage must be measured");
        } else {
            assertEquals(-1, result.allocatedBytesPerTransaction());
        }
    }

    private static boolean threadAllocationSupported() {
        return ManagementFactory.getThreadMXBean() instanceof com.sun.management.ThreadMXBean bean
               && bean.isThreadAllocatedMemorySupported();
    }

    /// Verifies measuring rejects invalid payload, chunk, and iteration parameters.
    @Test
    void rejectsInvalidMeasurementParameters() {
        assertThrows(IllegalArgumentException.class, () -> AeronPublisherBenchmark.measure(0, 1_024, 4, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> AeronPublisherBenchmark.measure(1_024, 0, 4, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> AeronPublisherBenchmark.measure(1_024, 1_024, 0, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> AeronPublisherBenchmark.measure(1_024, 1_024, 4, 0, 0));
    }
}
