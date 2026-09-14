package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
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

class ReplicationRetryTest
{
	@Test
	void rejectsUnboundedOrEmptyBudgets()
	{
		assertThrows(IllegalArgumentException.class, () -> ReplicationRetry.deadlineNanos(0L));
		assertThrows(IllegalArgumentException.class, () -> ReplicationRetry.deadlineNanos(-1L));
	}

	@Test
	void saturatesAndNeverReturnsNegativeRemainingTime()
	{
		assertEquals(Long.MAX_VALUE, ReplicationRetry.deadlineNanos(Long.MAX_VALUE));
		assertEquals(Long.MAX_VALUE, ReplicationRetry.remainingNanos(Long.MAX_VALUE));
		assertTrue(ReplicationRetry.remainingNanos(Long.MIN_VALUE) >= 0L);
		assertTrue(ReplicationRetry.expired(System.nanoTime() - 1_000_000L));
	}
}
