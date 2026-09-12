package org.eclipse.datagrid.storage.distributed.aeron.wire;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Smoke checks the benchmark's accounting without imposing machine-specific performance limits. */
class AeronEnvelopeBenchmarkTest
{
	@Test
	void measuresChunkedCopyAndThroughput()
	{
		final AeronEnvelopeBenchmark.BenchmarkResult result = AeronEnvelopeBenchmark.measure(
			UUID.randomUUID(), 1, 10_000, 1_024, 2, 5);

		assertEquals(10, result.chunkCount());
		assertEquals(10_000, result.copiedBytesPerTransaction());
		assertTrue(result.nanosecondsPerTransaction() > 0);
		assertTrue(result.mebibytesPerSecond() > 0);
	}

	@Test
	void rejectsInvalidMeasurementParameters()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronEnvelopeBenchmark.measure(
			UUID.randomUUID(), 1, 0, 1_024, 0, 1));
		assertThrows(IllegalArgumentException.class, () -> AeronEnvelopeBenchmark.measure(
			UUID.randomUUID(), 1, 1, 1_024, 0, 0));
	}
}
