package peruncs.cluster.node.aeron;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Smoke-checks that the full-path benchmark records writer, reader, and graph-boundary metrics.
///
/// It starts real nodes and measures for seconds, so it runs with the integration profile, not the
/// unit gate. The integration JVM keeps the pool's checked mode on, which changes buffer retention:
/// read real throughput and latency numbers only from `-Pbench` runs.
class AeronFullPathBenchmarkIT {
    /// Verifies the benchmark measures the store, archive, reader-import, and cursor path and reports sane percentiles.
    @Test
    void measuresConcurrentStoreArchiveAndReaderPath() throws Exception {
        final int payload = Integer.getInteger("aeron.benchmark.payload.bytes", 64 * 1024);
        final int warmupSeconds = Integer.getInteger("aeron.benchmark.warmup.seconds", 1);
        final int windowSeconds = Integer.getInteger("aeron.benchmark.window.seconds", 2);
        final AeronFullPathBenchmark.Measurement result =
                AeronFullPathBenchmark.measure(payload, warmupSeconds, windowSeconds, 1, 2, 1);
        assertEquals(1, result.windows().size());
        assertTrue(result.writerCommits() > 0L);
        assertTrue(result.writerCommitsPerSecond() > 0.0);
        assertTrue(result.writerP50Nanos() > 0L);
        assertTrue(result.writerP99Nanos() >= result.writerP50Nanos());
        assertTrue(result.readerApplyP99Nanos() > 0L);
        assertTrue(result.loadedReadP99Nanos() > 0L);
        assertEquals(1, result.readerMetrics().size());
        assertEquals(result.writerCommits(), result.readerMetrics().getFirst().samples());
        assertEquals(result.writerCommits(), result.windows().getFirst().commits());
    }
}
