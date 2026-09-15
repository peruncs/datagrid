package peruncs.datagrid.storage.distributed.aeron.reader;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Smoke-tests the reproducible end-to-end allocation benchmark without machine-dependent limits. */
class AeronEndToEndBenchmarkTest {
    @Test
    void assemblesEveryByteAndReportsThroughput() {
        final AeronEndToEndBenchmark.Result result = AeronEndToEndBenchmark.measure(256 * 1024, 8 * 1024, 10, 40);
        assertEquals(result.payloadLength(), result.assembledBytesPerTransaction());
        assertEquals(32, result.chunkCount());
        assertTrue(result.nanosecondsPerTransaction() > 0.0);
        assertTrue(result.mebibytesPerSecond() > 0.0);
    }
}
