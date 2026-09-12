package org.eclipse.datagrid.storage.distributed.aeron.writer;

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

import static org.junit.jupiter.api.Assertions.*;

/** Smoke tests the complete publisher staging benchmark without machine-specific limits. */
class AeronPublisherBenchmarkTest
{
	@Test
	void measuresCopiesOffersAndAllocation()
	{
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
	void rejectsInvalidMeasurementParameters()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronPublisherBenchmark.measure(0, 1_024, 4, 0, 1));
		assertThrows(IllegalArgumentException.class, () -> AeronPublisherBenchmark.measure(1_024, 0, 4, 0, 1));
		assertThrows(IllegalArgumentException.class, () -> AeronPublisherBenchmark.measure(1_024, 1_024, 0, 0, 1));
		assertThrows(IllegalArgumentException.class, () -> AeronPublisherBenchmark.measure(1_024, 1_024, 4, 0, 0));
	}
}
