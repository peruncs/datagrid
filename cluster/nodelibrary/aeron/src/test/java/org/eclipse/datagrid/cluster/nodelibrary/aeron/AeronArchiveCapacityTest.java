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

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies Archive capacity caching and fail-closed write admission. */
class AeronArchiveCapacityTest
{
	@Test
	void reservesAtLeastOneSegmentAndCachesTheFilesystemProbe()
	{
		final AtomicInteger probes = new AtomicInteger();
		final AeronArchiveCapacity capacity = new AeronArchiveCapacity(
			false, 100, 1_000, () -> { probes.incrementAndGet(); return 1_100; });
		assertTrue(capacity.available());
		assertTrue(capacity.available(1_000));
		assertEquals(1, probes.get());
		assertFalse(capacity.available(1_001));
	}

	@Test
	void rejectsUnknownNegativeAndOverflowingCapacityRequirements()
	{
		assertFalse(new AeronArchiveCapacity(false, 1, 1, () -> -1).available());
		assertFalse(new AeronArchiveCapacity(false, 1, 1, () -> Long.MAX_VALUE).available(-1));
		assertFalse(new AeronArchiveCapacity(false, Long.MAX_VALUE, 1, () -> Long.MAX_VALUE).available(1));
	}

	@Test
	void externalArchiveDoesNotPretendToReportLocalDiskCapacity()
	{
		final AeronArchiveCapacity capacity = new AeronArchiveCapacity(true, Long.MAX_VALUE, 1, () -> 0);
		assertTrue(capacity.available());
		assertTrue(capacity.available(Long.MAX_VALUE));
		assertEquals(-1, capacity.usableSpaceBytes());
	}
}
