package peruncs.datagrid.cluster.nodelibrary.aeron;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Smoke-checks that the full-path benchmark measures every production stage. */
class AeronFullPathBenchmarkTest
{
	@Test
	void measuresStoreArchiveReaderImportAndCursorPath() throws Exception
	{
		final int payload = 64 * 1024;
		final AeronFullPathBenchmark.Result result = AeronFullPathBenchmark.measure(payload, 4, 8);
		System.out.printf("Aeron full-path regression: tx/s=%.1f MiB/s=%.2f p99-us=%.1f heap-bytes/tx=%d%n",
			result.transactionsPerSecond(), result.mebibytesPerSecond(), result.p99Nanos() / 1_000.0,
			result.heapBytesPerTransaction());
		assertEquals(8, result.iterations());
		assertTrue(result.p50Nanos() > 0L);
		assertTrue(result.p99Nanos() >= result.p50Nanos());
		/* Throughput and allocation numbers are deliberately reported, not hard-coded
		 * as a wall-clock gate: shared CI, GC scheduling, and Archive I/O make those
		 * thresholds machine-dependent.  A benchmark run may opt into local floors
		 * with -Daeron.benchmark.minimum.mib.per.second=... and
		 * -Daeron.benchmark.maximum.heap.bytes.per.transaction=... . */
		final String minimumThroughput = System.getProperty("aeron.benchmark.minimum.mib.per.second");
		if (minimumThroughput != null)
		{
			assertTrue(result.mebibytesPerSecond() >= Double.parseDouble(minimumThroughput),
				"full Store/Archive/import path fell below the configured throughput floor");
		}
		final String maximumHeap = System.getProperty("aeron.benchmark.maximum.heap.bytes.per.transaction");
		if (maximumHeap != null && result.heapBytesPerTransaction() >= 0L)
		{
			assertTrue(result.heapBytesPerTransaction() <= Long.parseLong(maximumHeap),
				"full Store/Archive/import path exceeded the configured heap-allocation ceiling");
		}
	}
}
