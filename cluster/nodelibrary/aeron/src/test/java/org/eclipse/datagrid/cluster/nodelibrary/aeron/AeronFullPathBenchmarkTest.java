package org.eclipse.datagrid.cluster.nodelibrary.aeron;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Aeron Provider
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

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
		assertTrue(result.mebibytesPerSecond() >= Double.parseDouble(
			System.getProperty("aeron.benchmark.minimum.mib.per.second", "0.10")),
			"full Store/Archive/import path fell below its conservative throughput floor");
		assertTrue(result.p50Nanos() > 0L);
		assertTrue(result.p99Nanos() >= result.p50Nanos());
		assertTrue(result.heapBytesPerTransaction() >= payload,
			"allocation accounting did not include the application payload");
		assertTrue(result.heapBytesPerTransaction() <= 8L * 1024 * 1024 + payload * 16L,
			"full path allocated more than its per-transaction regression budget");
		assertTrue(result.directMemoryDelta() < 128L * 1024 * 1024,
			"full path retained excessive direct memory across measured transactions");
		assertTrue(result.mappedMemoryDelta() < 128L * 1024 * 1024,
			"full path retained excessive mapped memory across measured transactions");
	}
}
